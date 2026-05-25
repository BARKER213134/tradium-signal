"""🔔 BIG BUY signal parser — Cryptovizor bot (@cvizor_bot).

Пример сообщения:
  🌟 #ONDOUSDT.P #BINANCE_FUTURES
  💬 BIG BUY
  1d Объем в USDT больше чем заданное значение 20M
  1d Объем в USDT меньше чем заданное значение 500M
  1h Объем положительной дельты в USDT больше чем заданное значение 1M
  30m Объем положительной дельты в USDT вырос на 2547%
  🏷️ Цена 0.4156

Парсинг:
  - pair: ONDO/USDT (из #ONDOUSDT.P)
  - type: 'BIG BUY' (LONG bias — high positive delta vol)
  - price: 0.4156
  - rules: список conditions (по строкам)
  - delta_pct: 2547% (из "вырос на N%")
  - vol_24h_usdt: 20M (из "Объем больше N")
"""
from __future__ import annotations
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Regex patterns
_PAIR_RE = re.compile(r'#([A-Z0-9_]+)USDT\.P', re.IGNORECASE)
_PRICE_RE = re.compile(r'Цена\s+([\d.]+)', re.IGNORECASE)
_DELTA_PCT_RE = re.compile(r'дельты.*?вырос на\s+([\d.]+)\s*%', re.IGNORECASE | re.DOTALL)
_VOL_MIN_RE = re.compile(r'Объем.*?больше.*?(\d+(?:\.\d+)?)\s*([MK])', re.IGNORECASE | re.DOTALL)


def is_bigbuy_message(text: str) -> bool:
    """True если message выглядит как BIG BUY signal."""
    if not text:
        return False
    return 'BIG BUY' in text and '#' in text and 'Цена' in text


def _parse_volume(s: str) -> Optional[float]:
    """'20M' → 20_000_000, '500K' → 500_000."""
    m = re.search(r'(\d+(?:\.\d+)?)\s*([MK])', s, re.IGNORECASE)
    if not m: return None
    n = float(m.group(1))
    unit = m.group(2).upper()
    return n * (1_000_000 if unit == 'M' else 1_000)


def parse_bigbuy_message(text: str) -> Optional[dict]:
    """Parse BIG BUY message → dict. Returns None если не соответствует формату.

    Returns:
      {
        'pair': 'ONDO/USDT',
        'symbol': 'ONDOUSDT',
        'price': 0.4156,
        'delta_pct_30m': 2547.0,
        'vol_min_usdt': 20_000_000,
        'rules': ['1d Объем в USDT больше...', ...],
        'raw_text': '<original message>',
      }
    """
    if not is_bigbuy_message(text):
        return None
    try:
        # 1. Pair
        pair_m = _PAIR_RE.search(text)
        if not pair_m:
            return None
        symbol_base = pair_m.group(1).upper().replace('_', '')
        if symbol_base.endswith('USDT'):
            symbol_base = symbol_base[:-4]
        pair = f"{symbol_base}/USDT"
        symbol = f"{symbol_base}USDT"

        # 2. Price
        price_m = _PRICE_RE.search(text)
        if not price_m:
            return None
        price = float(price_m.group(1))

        # 3. Delta % (последняя цифра в "вырос на N%")
        delta_pct = None
        delta_m = _DELTA_PCT_RE.search(text)
        if delta_m:
            try: delta_pct = float(delta_m.group(1))
            except Exception: pass

        # 4. Min volume threshold (первая "Объем больше N")
        vol_min = None
        vol_m = _VOL_MIN_RE.search(text)
        if vol_m:
            n = float(vol_m.group(1))
            unit = vol_m.group(2).upper()
            vol_min = n * (1_000_000 if unit == 'M' else 1_000)

        # 5. Rules (все строки кроме headers)
        rules = []
        for line in text.split('\n'):
            ln = line.strip()
            if not ln: continue
            if ln.startswith('🌟') or ln.startswith('💬') or ln.startswith('🏷️'):
                continue
            if 'BIG BUY' in ln or 'Цена' in ln:
                continue
            rules.append(ln)

        return {
            'pair': pair,
            'symbol': symbol,
            'price': price,
            'delta_pct_30m': delta_pct,
            'vol_min_usdt': vol_min,
            'rules': rules,
            'raw_text': text,
            'source': 'bigbuy',
            'direction': 'LONG',  # BIG BUY = LONG bias (positive delta)
        }
    except Exception:
        logger.exception(f'[bigbuy-parser] parse fail: {text[:100]}')
        return None


def store_bigbuy_signal(data: dict, msg_id: int | None = None) -> bool:
    """Сохраняет в Mongo collection bigbuy_signals."""
    try:
        from database import _get_db, utcnow
        db = _get_db()
        doc = {
            **data,
            'msg_id': msg_id,
            'received_at': utcnow(),
            'entry': data.get('price'),
            'state': 'NEW',
        }
        # Дедуп: same pair + msg_id или same pair within 5 min
        if msg_id:
            existing = db.bigbuy_signals.find_one({'msg_id': msg_id})
            if existing:
                return False
        from datetime import timedelta
        cutoff = utcnow() - timedelta(minutes=5)
        recent_dup = db.bigbuy_signals.find_one({
            'pair': data['pair'], 'received_at': {'$gte': cutoff},
        })
        if recent_dup:
            return False
        db.bigbuy_signals.insert_one(doc)
        logger.info(f"[bigbuy] 🔔 stored {data['pair']} price={data['price']} "
                     f"delta={data.get('delta_pct_30m')}% rules={len(data.get('rules',[]))}")
        return True
    except Exception:
        logger.exception('[bigbuy] store fail')
        return False


if __name__ == '__main__':
    # Test
    sample = """🌟 #ONDOUSDT.P #BINANCE_FUTURES
💬 BIG BUY
1d Объем в USDT больше чем заданное значение 20M
1d Объем в USDT меньше чем заданное значение 500M
1h Объем положительной дельты в USDT больше чем заданное значение 1M
30m Объем положительной дельты в USDT вырос на 2547%
🏷️ Цена 0.4156"""
    r = parse_bigbuy_message(sample)
    print('Parsed:', r)
