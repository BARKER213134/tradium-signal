# -*- coding: utf-8 -*-
"""MongoDB backend + тонкий wrapper, имитирующий API который использует остальной код.

Публичный API (не меняется для пользователей):
- utcnow()                                    — tz-naive UTC
- init_db()                                   — создаёт индексы
- SessionLocal() → Session                    — контекст для запросов
- Signal                                      — класс-данные с атрибутами (dict wrapper)
- get_db()                                    — FastAPI dependency

Особенности:
- id: int, автоинкремент через коллекцию counters
- поля хранятся как обычные ключи документа
- все булевы/даты сохраняются как есть
"""
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
import gridfs

from config import MONGO_URL, MONGO_DB


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ─── Connection (lazy) ─────────────────────────────────────────────────
_client: Optional[MongoClient] = None
_db = None


def _get_db():
    global _client, _db
    if _db is not None:
        return _db
    if not MONGO_URL:
        raise RuntimeError("MONGO_URL не задан в .env")
    _client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=10000)
    _db = _client[MONGO_DB]
    return _db


def _signals() -> Collection:
    return _get_db().signals


def _events() -> Collection:
    return _get_db().events


def _anomalies() -> Collection:
    return _get_db().anomalies


def _confluence() -> Collection:
    return _get_db().confluence


def _clusters() -> Collection:
    return _get_db().clusters


def _cluster_config() -> Collection:
    return _get_db().system


def _fvg_signals() -> Collection:
    """Forex FVG сигналы (все статусы: FORMED/WAITING/ENTERED/TP/SL/EXPIRED)."""
    return _get_db().fvg_signals


def _fvg_config() -> Collection:
    """Настройки Hybrid v2 Forex FVG (хранится в system._id='fvg_config')."""
    return _get_db().system


def _conflicts() -> Collection:
    """Conflicts collection — противоречия между сигналами (Anti-cluster)."""
    return _get_db().conflicts


def _td_quota() -> Collection:
    """TwelveData usage tracking — log of credits spent."""
    return _get_db().td_quota


def _market_events() -> Collection:
    """Market state changes — Keltner ETH и Reversal Meter zone flips.
    Используется для маркеров на графиках."""
    return _get_db().market_events


def _key_levels() -> Collection:
    """Key Levels от Tradium (Support/Resistance/Ranges boots в topics 3086/3088/3091).
    Используются для обогащения сигналов emoji-маркерами и зонами на графиках."""
    return _get_db().key_levels


def _supertrend_signals() -> Collection:
    """SuperTrend сигналы — VIP / Triple MTF / Daily Filter.
    Генерируются tracker'ом каждые 5 мин при флипе ST на 1h + условия по tier.
    Отправляются в BOT10. Используются в отдельной вкладке UI + маркеры на графиках +
    бейджи в журнале для совпадающих сигналов."""
    return _get_db().supertrend_signals


def _cv_flip_signals() -> Collection:
    """CV + SuperTrend 30m Flip observation signals.

    Lifecycle: приход CV (source=cryptovizor) → WAITING →
    (FLIPPED | TIMEOUT | INVALIDATED). Только наблюдение, paper_trader
    не открывает сделок на этот источник. Watcher: cv_flip_watcher."""
    return _get_db().cv_flip_signals


def _live_trades() -> Collection:
    """Реальные сделки (Binance Futures через ccxt). Аналог paper_trades
    но с exchange_order_id, exchange_fills, real PnL. Режим testnet|real
    в поле `env`."""
    return _get_db().live_trades


def _live_state() -> Collection:
    """Глобальное состояние реальной торговли:
    {_id: 'state', mode: 'paper|testnet|real', kill_switch: bool,
     safety_preset: 'conservative|moderate|aggressive',
     balance_testnet: 1000, balance_real: 200, equity_peak: ...,
     daily_pnl_start_balance: ..., daily_reset_at: datetime,
     enabled: false, last_sync_at: ...}"""
    return _get_db().live_state


def _live_pending_confirmations() -> Collection:
    """Ожидающие подтверждения сигналы (AI решил открыть — ждём ✅/❌ в Telegram
    перед исполнением). TTL 15 мин — если не подтверждён за это время, протух."""
    return _get_db().live_pending_confirmations




def _counters() -> Collection:
    return _get_db().counters


# ─── GridFS для графиков ──────────────────────────────────────────────
_fs = None


def _get_fs() -> gridfs.GridFS:
    global _fs
    if _fs is None:
        _fs = gridfs.GridFS(_get_db(), collection="charts")
    return _fs


def save_chart(signal_id: int, data: bytes, filename: str = "", content_type: str = "image/jpeg"):
    """Сохраняет график в GridFS. Перезаписывает если уже есть."""
    fs = _get_fs()
    # Удаляем старый если есть
    for old in fs.find({"signal_id": signal_id}):
        fs.delete(old._id)
    fs.put(data, signal_id=signal_id, filename=filename, content_type=content_type)


def get_chart(signal_id: int) -> bytes | None:
    """Возвращает байты графика из GridFS."""
    fs = _get_fs()
    f = fs.find_one({"signal_id": signal_id}, sort=[("uploadDate", DESCENDING)])
    if f:
        return f.read()
    return None


# ─── Event log ─────────────────────────────────────────────────────────
def log_event(
    signal_id: int,
    event_type: str,
    price: float | None = None,
    data: dict | None = None,
    message: str | None = None,
) -> None:
    """Записывает действие в коллекцию events."""
    try:
        doc = {
            "signal_id": signal_id,
            "type": event_type,
            "price": price,
            "data": data or {},
            "message": message,
            "at": utcnow(),
        }
        _events().insert_one(doc)
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"log_event failed: {e}")


def get_events(signal_id: int, limit: int = 50) -> list[dict]:
    """События для одного сигнала, новые сверху."""
    cur = (
        _events()
        .find({"signal_id": signal_id})
        .sort("at", DESCENDING)
        .limit(limit)
    )
    out = []
    for d in cur:
        d.pop("_id", None)
        if d.get("at"):
            d["at"] = d["at"].isoformat() if hasattr(d["at"], "isoformat") else str(d["at"])
        out.append(d)
    return out


def _next_id() -> int:
    doc = _counters().find_one_and_update(
        {"_id": "signals"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=True,
    )
    return doc["seq"]


# ─── Signal: data class + dict bridge ─────────────────────────────────
SIGNAL_FIELDS = [
    "id", "message_id", "chart_message_id", "source_group_id",
    "raw_text", "chart_path", "chart_ai_raw",
    "text_pair", "text_direction", "text_entry", "text_sl", "text_tp1", "text_tp2", "text_tp3",
    "chart_pair", "chart_direction", "chart_entry", "chart_sl",
    "chart_tp1", "chart_tp2", "chart_tp3", "chart_notes",
    "pair", "direction", "entry", "sl", "tp1", "tp2", "tp3",
    "timeframe", "risk_reward", "risk_percent", "amount",
    "tp_percent", "sl_percent", "trend", "comment", "setup_number", "status",
    "dca1", "dca2", "dca3", "dca4", "dca4_triggered", "dca4_triggered_at",
    "pattern_triggered", "pattern_name", "pattern_triggered_at", "pattern_price",
    "closed_at", "exit_price", "pnl_percent", "result",
    "has_chart", "chart_analyzed", "is_forwarded", "is_filtered", "filter_reason",
    "received_at", "chart_received_at", "forwarded_at",
    # AI quality assessment
    "ai_score", "ai_confidence", "ai_reasoning", "ai_risks", "ai_verdict",
    # Источник сигнала (какой бот его поймал)
    "source",
    # Keltner Channel ETH фильтр
    "st_passed", "st_direction", "st_streak",
    # Pump potential
    "pump_score", "pump_factors",
]

_DEFAULTS = {
    "has_chart": False, "chart_analyzed": False, "is_forwarded": False,
    "is_filtered": False, "dca4_triggered": False, "pattern_triggered": False,
    "status": "СЛЕЖУ",
    "source": "tradium",
}


class _Field:
    """Descriptor: класс-уровень → _FieldRef (для фильтров), инстанс → значение."""
    def __init__(self, name: str):
        self.name = name

    def __get__(self, obj, objtype=None):
        if obj is None:
            return _FieldRef(self.name)
        return obj._data.get(self.name)

    def __set__(self, obj, value):
        obj._data[self.name] = value


class Signal:
    """Dict-backed data class с фильтр-DSL на class-level."""

    def __init__(self, **kwargs):
        object.__setattr__(self, "_data", {})
        for f in SIGNAL_FIELDS:
            if f in _DEFAULTS:
                self._data[f] = _DEFAULTS[f]
        for k, v in kwargs.items():
            self._data[k] = v

    def to_dict(self) -> dict:
        return {k: v for k, v in self._data.items() if v is not None}

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> Optional["Signal"]:
        if not d:
            return None
        s = cls()
        _DT_FIELDS = {"received_at", "pattern_triggered_at", "chart_received_at", "forwarded_at"}
        for k, v in d.items():
            if k == "_id":
                continue
            # Автоконверсия строковых дат в datetime
            if k in _DT_FIELDS and isinstance(v, str):
                try:
                    v = datetime.fromisoformat(v)
                except (ValueError, TypeError):
                    pass
            s._data[k] = v
        return s


# ─── Filter DSL (Signal.status == "X") ─────────────────────────────────
class _FilterExpr:
    def __init__(self, field: str | None, op: str, value: Any):
        self.field = field
        self.op = op
        self.value = value

    def to_mongo(self) -> dict:
        if self.op == "eq":
            return {self.field: self.value}
        if self.op == "ne":
            return {self.field: {"$ne": self.value}}
        if self.op == "in":
            return {self.field: {"$in": list(self.value)}}
        if self.op == "nin":
            return {self.field: {"$nin": list(self.value)}}
        if self.op == "gt":
            return {self.field: {"$gt": self.value}}
        if self.op == "gte":
            return {self.field: {"$gte": self.value}}
        if self.op == "lt":
            return {self.field: {"$lt": self.value}}
        if self.op == "lte":
            return {self.field: {"$lte": self.value}}
        if self.op == "or":
            return {"$or": [e.to_mongo() for e in self.value]}
        if self.op == "and":
            return {"$and": [e.to_mongo() for e in self.value]}
        raise ValueError(self.op)

    def __or__(self, other):
        return _FilterExpr(None, "or", [self, other])

    def __and__(self, other):
        return _FilterExpr(None, "and", [self, other])


class _FieldRef:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other):
        return _FilterExpr(self.name, "eq", other)

    def __ne__(self, other):
        return _FilterExpr(self.name, "ne", other)

    def __lt__(self, other):
        return _FilterExpr(self.name, "lt", other)

    def __le__(self, other):
        return _FilterExpr(self.name, "lte", other)

    def __gt__(self, other):
        return _FilterExpr(self.name, "gt", other)

    def __ge__(self, other):
        return _FilterExpr(self.name, "gte", other)

    def in_(self, values):
        return _FilterExpr(self.name, "in", values)

    def __hash__(self):
        return hash(self.name)


def _attach_fields():
    for f in SIGNAL_FIELDS:
        setattr(Signal, f, _Field(f))


_attach_fields()


# ─── Query ─────────────────────────────────────────────────────────────
class _OrderBy:
    def __init__(self, field: str, direction: int):
        self.field = field
        self.direction = direction


def desc(field: _FieldRef) -> _OrderBy:
    return _OrderBy(field.name, DESCENDING)


def asc(field: _FieldRef) -> _OrderBy:
    return _OrderBy(field.name, ASCENDING)


class Query:
    """Минималистичный query-builder, имитирующий часть API SQLAlchemy."""

    def __init__(self, model, session: "Session", *projections):
        self._model = model
        self._session = session
        self._filters: list[dict] = []
        self._order: list[tuple] = []
        self._limit: Optional[int] = None
        self._offset: int = 0
        self._projections = projections  # для query(Signal.status, func.count(...))
        self._distinct = False

    def filter(self, *conds) -> "Query":
        for c in conds:
            if isinstance(c, _FilterExpr):
                self._filters.append(c.to_mongo())
            elif isinstance(c, dict):
                self._filters.append(c)
            else:
                raise ValueError(f"Unsupported filter: {c}")
        return self

    def order_by(self, order) -> "Query":
        if isinstance(order, _OrderBy):
            self._order.append((order.field, order.direction))
        elif isinstance(order, _FieldRef):
            self._order.append((order.name, ASCENDING))
        elif isinstance(order, str):
            # db.query(...).order_by(desc("cnt")) для агрегатов
            self._order.append((order, DESCENDING))
        return self

    def limit(self, n: int) -> "Query":
        self._limit = n
        return self

    def offset(self, n: int) -> "Query":
        self._offset = n
        return self

    def _mongo_filter(self) -> dict:
        if not self._filters:
            return {}
        if len(self._filters) == 1:
            return self._filters[0]
        return {"$and": self._filters}

    # --- exec ---
    def count(self) -> int:
        return _signals().count_documents(self._mongo_filter())

    def first(self) -> Optional[Signal]:
        cur = _signals().find(self._mongo_filter())
        if self._order:
            cur = cur.sort(self._order)
        doc = next(iter(cur.limit(1)), None)
        s = Signal.from_dict(doc)
        if s is not None:
            self._session._tracked.append(s)
        return s

    def all(self) -> list:
        # Специальный случай: group-by агрегат (func.count + group_by)
        if self._projections and any(isinstance(p, _GroupAgg) for p in self._projections):
            return self._aggregate()

        # distinct по одному полю
        if self._distinct and self._projections:
            field = None
            for p in self._projections:
                if isinstance(p, _FieldRef):
                    field = p.name
            if field:
                values = _signals().distinct(field, self._mongo_filter())
                return [(v,) for v in values if v is not None]

        cur = _signals().find(self._mongo_filter())
        if self._order:
            cur = cur.sort(self._order)
        if self._offset:
            cur = cur.skip(self._offset)
        if self._limit is not None:
            cur = cur.limit(self._limit)
        result = [Signal.from_dict(d) for d in cur]
        self._session._tracked.extend(result)
        return result

    def _aggregate(self):
        # поддерживаем только db.query(Signal.status, func.count(Signal.id)).group_by(Signal.status)
        group_field = None
        for p in self._projections:
            if isinstance(p, _FieldRef):
                group_field = p.name
        if group_field is None:
            return []
        pipeline = []
        if self._filters:
            pipeline.append({"$match": self._mongo_filter()})
        pipeline.append({"$group": {"_id": f"${group_field}", "cnt": {"$sum": 1}}})
        out = []
        for doc in _signals().aggregate(pipeline):
            out.append((doc["_id"], doc["cnt"]))
        return out

    def group_by(self, field) -> "Query":
        return self  # флаг уже установлен через _projections

    def delete(self) -> int:
        res = _signals().delete_many(self._mongo_filter())
        return res.deleted_count

    def distinct(self) -> "Query":
        self._distinct = True
        return self


class _GroupAgg:
    """Placeholder для func.count(Signal.id)."""
    pass


class _Func:
    def count(self, field):
        return _GroupAgg()


func = _Func()


# ─── Session ───────────────────────────────────────────────────────────
class Session:
    def __init__(self):
        self._pending_inserts: list[Signal] = []
        self._tracked: list[Signal] = []  # для commit() после изменений

    def query(self, *args) -> Query:
        if not args:
            raise ValueError("query() requires args")
        first = args[0]
        projections = args[1:] if len(args) > 1 else args
        # Если первый аргумент Signal → обычный query
        if first is Signal:
            return Query(Signal, self)
        # Иначе это проекция (Signal.status, func.count(...))
        q = Query(Signal, self, *args)
        return q

    def add(self, obj: Signal):
        self._pending_inserts.append(obj)
        self._tracked.append(obj)

    def add_all(self, objs: Iterable[Signal]):
        for o in objs:
            self.add(o)

    def refresh(self, obj: Signal):
        # Перечитываем по id из базы
        if getattr(obj, "id", None) is None:
            return
        doc = _signals().find_one({"id": obj.id})
        if doc:
            for k, v in doc.items():
                if k != "_id":
                    obj._data[k] = v

    def commit(self):
        # 1) Новые вставки
        inserted = set()
        for obj in self._pending_inserts:
            if getattr(obj, "id", None) is None:
                obj._data["id"] = _next_id()
            if obj._data.get("received_at") is None:
                obj._data["received_at"] = utcnow()
            _signals().insert_one(obj.to_dict())
            inserted.add(id(obj))
        self._pending_inserts.clear()

        # 2) Апдейты отслеживаемых (пропускаем только что вставленные)
        for obj in self._tracked:
            if id(obj) in inserted:
                continue
            if getattr(obj, "id", None) is None:
                continue
            _signals().update_one({"id": obj.id}, {"$set": obj.to_dict()})

    def rollback(self):
        self._pending_inserts.clear()
        self._tracked.clear()

    def close(self):
        self._pending_inserts.clear()
        self._tracked.clear()


def SessionLocal() -> Session:
    return Session()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    col = _signals()
    col.create_index("id", unique=True)
    col.create_index("message_id", unique=True, sparse=True)
    col.create_index("status")
    col.create_index("pair")
    col.create_index("received_at")
    col.create_index("source")
    # Миграция: проставить source='tradium' для старых документов
    col.update_many({"source": {"$exists": False}}, {"$set": {"source": "tradium"}})

    ev = _events()
    ev.create_index("signal_id")
    ev.create_index("at")
    ev.create_index("type")

    an = _anomalies()
    an.create_index("symbol")
    an.create_index("score")
    an.create_index("detected_at")

    cl = _clusters()
    cl.create_index("symbol")
    cl.create_index("direction")
    cl.create_index("trigger_at")
    cl.create_index("status")
    cl.create_index([("is_top_pick", ASCENDING), ("trigger_at", DESCENDING)])
    cl.create_index([("symbol", ASCENDING), ("direction", ASCENDING), ("trigger_at", DESCENDING)])

    fv = _fvg_signals()
    fv.create_index("instrument")
    fv.create_index("status")
    fv.create_index("formed_at")
    fv.create_index("entered_at")
    fv.create_index([("status", ASCENDING), ("formed_at", DESCENDING)])

    # Новые коллекции и indexes (добавлены при аудите платформы)
    cf = _confluence()
    cf.create_index("detected_at")
    cf.create_index([("direction", ASCENDING), ("detected_at", DESCENDING)])
    cf.create_index([("symbol", ASCENDING), ("direction", ASCENDING), ("detected_at", DESCENDING)])
    cf.create_index([("pair", ASCENDING), ("direction", ASCENDING), ("detected_at", DESCENDING)])
    cf.create_index([("score", ASCENDING), ("detected_at", DESCENDING)])
    cf.create_index([("is_top_pick", ASCENDING), ("detected_at", DESCENDING)])

    conf_col = _conflicts()
    conf_col.create_index("detected_at")
    conf_col.create_index([("pair", ASCENDING), ("detected_at", DESCENDING)])

    pt = _get_db().paper_trades
    pt.create_index("opened_at")
    pt.create_index([("status", ASCENDING), ("opened_at", DESCENDING)])
    # paper_trader делает find_one({"trade_id": N}) / update_one в hot-loop — без индекса это full scan
    pt.create_index("trade_id", unique=True, sparse=True, name="trade_id_unique")

    tdq = _get_db().td_quota
    tdq.create_index("at")

    fcc = _get_db().fvg_candle_cache
    fcc.create_index([("instrument", ASCENDING), ("tf", ASCENDING)])

    # Signals — composite для top_pick queries
    col.create_index([("source", ASCENDING), ("pattern_triggered", ASCENDING), ("pattern_triggered_at", DESCENDING)])
    col.create_index([("is_top_pick", ASCENDING), ("source", ASCENDING)])
    col.create_index([("pair", ASCENDING), ("direction", ASCENDING), ("source", ASCENDING)])
    # Chart-overlay queries: выборка по паре + сортировка по времени
    col.create_index([("pair", ASCENDING), ("received_at", DESCENDING)])

    # Anomalies — direction queries
    an.create_index([("pair", ASCENDING), ("direction", ASCENDING), ("detected_at", DESCENDING)])
    an.create_index([("direction", ASCENDING), ("detected_at", DESCENDING)])

    # TTL cleanup — автоматическое удаление старых записей через 90 дней
    # Atlas free 512MB не должен переполниться
    try:
        # Anomalies: 90 дней (шум, не критичны после закрытия)
        an.create_index("detected_at", expireAfterSeconds=90*86400, name="ttl_90d")
        # Confluence: 90 дней
        cf.create_index("detected_at", expireAfterSeconds=90*86400, name="ttl_90d")
        # FVG candle cache: 7 дней (refresh'ится сканером)
        fcc.create_index("cached_at", expireAfterSeconds=7*86400, name="ttl_7d")
        # TD quota: 14 дней
        tdq.create_index("at", expireAfterSeconds=14*86400, name="ttl_14d")
        # Market events (KC/Reversal changes): 30 дней
        me = _market_events()
        me.create_index("at", expireAfterSeconds=30*86400, name="ttl_30d")
        me.create_index([("at", DESCENDING)])
        me.create_index([("type", ASCENDING), ("at", DESCENDING)])
        # Key Levels: 14 дней (уровни теряют актуальность)
        kl = _key_levels()
        kl.create_index("detected_at", expireAfterSeconds=14*86400, name="ttl_14d")
        kl.create_index([("pair_norm", ASCENDING), ("detected_at", DESCENDING)])
        kl.create_index([("pair_norm", ASCENDING), ("event", ASCENDING), ("tf", ASCENDING), ("zone_low", ASCENDING)], unique=False)
        # KL backfill делает find_one({"message_id": m.id}) в цикле — без индекса full scan на каждой итерации
        kl.create_index("message_id", sparse=True, name="message_id_sparse")

        # Market phases — TTL 60 дней, индекс по времени
        mp = _get_db().market_phases
        mp.create_index("at", expireAfterSeconds=60*86400, name="ttl_60d")
        mp.create_index([("at", DESCENDING)])

        # Verified Entries (Entry Checker авто-проверка сигналов) — TTL 30д
        vs = _get_db().verified_signals
        vs.create_index("created_at", expireAfterSeconds=30*86400, name="ttl_30d")
        vs.create_index([("pair_norm", ASCENDING), ("direction", ASCENDING), ("created_at", DESCENDING)])
        vs.create_index([("created_at", DESCENDING)])

        # Paper AI rejections — лог отказов от сделок (TTL 7 дней)
        try:
            rej = _get_db().paper_rejections
            rej.create_index("at", expireAfterSeconds=7*86400, name="ttl_7d")
            rej.create_index([("at", DESCENDING)])
            rej.create_index([("symbol", ASCENDING), ("at", DESCENDING)])
        except Exception:
            pass

        # SuperTrend signals
        sts = _supertrend_signals()
        sts.create_index("created_at", expireAfterSeconds=30*86400, name="ttl_30d")
        sts.create_index([("tier", ASCENDING), ("flip_at", DESCENDING)])
        sts.create_index([("pair_norm", ASCENDING), ("flip_at", DESCENDING)])
        sts.create_index([("pair_norm", ASCENDING), ("tier", ASCENDING), ("flip_at", ASCENDING)], unique=True, name="uniq_flip")
        sts.create_index("id", unique=True, sparse=True)

        # CV+ST Flip observation
        cvf = _cv_flip_signals()
        cvf.create_index("created_at", expireAfterSeconds=60*86400, name="ttl_60d")
        cvf.create_index([("state", ASCENDING), ("cv_triggered_at", DESCENDING)])
        cvf.create_index([("pair", ASCENDING), ("state", ASCENDING)])
        cvf.create_index([("cv_triggered_at", DESCENDING)])
        cvf.create_index("cv_signal_id", unique=True, sparse=True)
        # Быстрый поиск по паре для /api/cv-flips?pair=X (маркеры на графиках)
        cvf.create_index([("pair", ASCENDING), ("cv_triggered_at", DESCENDING)])

        # Live trades (реальные сделки Binance)
        lt = _live_trades()
        lt.create_index([("status", ASCENDING), ("opened_at", DESCENDING)])
        lt.create_index([("env", ASCENDING), ("opened_at", DESCENDING)])
        lt.create_index("exchange_order_id", unique=True, sparse=True)
        lt.create_index("trade_id", unique=True, sparse=True)

        # Live pending confirmations (15 мин TTL)
        lpc = _live_pending_confirmations()
        lpc.create_index("created_at", expireAfterSeconds=15*60, name="ttl_15min")
        lpc.create_index([("status", ASCENDING), ("created_at", DESCENDING)])
        lpc.create_index("confirmation_token", unique=True, sparse=True)
    except Exception:
        pass  # idempotent — если TTL индексы уже есть, ok

