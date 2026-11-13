"""📐 Структура рынка: регрессионный канал (13.11.26).

Юзер: «по графикам вроде аптренд, а на самом деле ни хера — можем ли мы
определять тренд/диапазон автоматически». SuperTrend 2h для лонгов школы
не различал ничего (▲ −0.05 / ▼ +0.02), а при ST ▲ настоящий восходящий
канал был только в 20% случаев.

Канал = линейная регрессия закрытий за N закрытых баров. Дрейф линии
меряем в ширинах канала (±2σ остатков): > +0.75 → аптренд (U),
< −0.75 → даунтренд (D), иначе диапазон (R). Плюс место цены в коридоре
max/min окна (низ/середина/верх).

Проверка на 16 037 сделках школы с 20.09 (параметры взяты априори, не
подбирались): LONG U +1.18 / R −0.05 / D −2.85 (WR 44/36/17), порядок
сохраняется в обеих половинах; SHORT лучший — D × середина (WR 61 +2.63).

Ось школы — 2h, N=120 (10 дней). Без заглядывания: на вход подаются
только ЗАКРЫТЫЕ бары.
"""
import numpy as np

N = 120            # баров в окне (2h → 10 дней)
MOVE_THR = 0.75    # порог дрейфа в ширинах канала
KCH_LABEL = {"U": "📐 канал ▲", "R": "📐 диапазон", "D": "📐 канал ▼"}
KPOS_LABEL = {"lo": "низ", "mid": "середина", "hi": "верх"}

_SX = {}


def _sx(n):
    v = _SX.get(n)
    if v is None:
        x = np.arange(n, dtype=float) - (n - 1) / 2.0
        v = _SX[n] = (x, float(x @ x))
    return v


def channel(c, h, l, n=N):
    """c/h/l — ряды ЗАКРЫТЫХ баров по возрастанию времени (последний —
    самый свежий закрытый). → {ch, mv, pos, kp, top, bot} или None."""
    try:
        if c is None or len(c) < n:
            return None
        cc = np.asarray(c[-n:], dtype=float)
        x, xx = _sx(n)
        k = float(x @ cc) / xx
        res = cc - (cc.mean() + k * x)
        sd = float(res.std())
        if not sd > 0:
            return None
        mv = k * (n - 1) / (4.0 * sd)
        hi = float(np.max(np.asarray(h[-n:], dtype=float)))
        lo = float(np.min(np.asarray(l[-n:], dtype=float)))
        pos = (float(cc[-1]) - lo) / (hi - lo) if hi > lo else 0.5
        return {"ch": "U" if mv > MOVE_THR else "D" if mv < -MOVE_THR else "R",
                "mv": round(float(mv), 2), "pos": round(float(pos), 3),
                "kp": "lo" if pos < 1 / 3 else "hi" if pos > 2 / 3 else "mid",
                "top": hi, "bot": lo}
    except Exception:
        return None


def channel_from_1h(kd, n=N):
    """kd — 1h dict-бары (t мс, h, l, c) по возрастанию, последний может
    быть формирующимся. Ресемпл в 2h как у школы (learn_engine._resample:
    t // 2ч); последнюю 2h-группу отбрасываем — она не закрыта."""
    try:
        t2, h2, l2, c2 = [], [], [], []
        cur = None
        for b in kd:
            g = int(b["t"]) // 7_200_000
            if g != cur:
                cur = g
                t2.append(g)
                h2.append(b["h"])
                l2.append(b["l"])
                c2.append(b["c"])
            else:
                h2[-1] = max(h2[-1], b["h"])
                l2[-1] = min(l2[-1], b["l"])
                c2[-1] = b["c"]
        return channel(c2[:-1], h2[:-1], l2[:-1], n)
    except Exception:
        return None
