"""Рисует свечи + маркер entry + горизонтальные линии S1/R1.

Возвращает bytes PNG для отправки в Telegram.
"""
import io
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def render_chart(
    candles: list[dict],
    pair: str,
    direction: str,
    entry: float | None,
    s1: float | None = None,
    r1: float | None = None,
    pattern: str | None = None,
) -> bytes | None:
    """Генерирует PNG с candlestick chart + overlays."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import mplfinance as mpf
        import pandas as pd
    except Exception as e:
        logger.error(f"mplfinance not available: {e}")
        return None

    if not candles:
        return None

    # Готовим DataFrame для mplfinance
    rows = []
    for c in candles:
        ts = datetime.fromtimestamp(c["t"] / 1000, tz=timezone.utc)
        rows.append({
            "Date": ts,
            "Open": c["o"],
            "High": c["h"],
            "Low": c["l"],
            "Close": c["c"],
            "Volume": c.get("v", 0),
        })
    df = pd.DataFrame(rows)
    df.set_index("Date", inplace=True)

    # Стиль (тёмная тема, совпадает с админкой)
    mc = mpf.make_marketcolors(
        up="#00e5a0", down="#ff4d6d",
        edge={"up": "#00e5a0", "down": "#ff4d6d"},
        wick={"up": "#00e5a0", "down": "#ff4d6d"},
        volume={"up": "#00e5a0", "down": "#ff4d6d"},
    )
    style = mpf.make_mpf_style(
        base_mpf_style="nightclouds",
        marketcolors=mc,
        facecolor="#0b0d12",
        edgecolor="#252a38",
        gridcolor="#171b24",
        figcolor="#0b0d12",
        rc={"axes.labelcolor": "#c6cad5", "xtick.color": "#6b7180",
            "ytick.color": "#6b7180", "axes.edgecolor": "#252a38"},
    )

    # Горизонтальные линии для S1/R1
    hlines_values = []
    hlines_colors = []
    if s1 is not None:
        hlines_values.append(s1)
        hlines_colors.append("#00e5a0")
    if r1 is not None:
        hlines_values.append(r1)
        hlines_colors.append("#ff4d6d")
    if entry is not None:
        hlines_values.append(entry)
        hlines_colors.append("#00ffff")

    hlines_cfg = None
    if hlines_values:
        hlines_cfg = dict(
            hlines=hlines_values,
            colors=hlines_colors,
            linewidths=1.0,
            linestyle="--",
        )

    dir_emoji = "🟢 LONG" if direction in ("LONG", "BUY") else "🔴 SHORT"
    title_parts = [f"{pair} · 1h · {dir_emoji}"]
    if pattern:
        title_parts.append(pattern)
    title = "  |  ".join(title_parts)

    buf = io.BytesIO()
    try:
        mpf.plot(
            df,
            type="candle",
            style=style,
            hlines=hlines_cfg,
            volume=True,
            figsize=(10, 6),
            title=title,
            savefig=dict(fname=buf, dpi=100, bbox_inches="tight",
                         facecolor="#0b0d12"),
        )
    except Exception as e:
        logger.error(f"render_chart fail: {e}")
        plt.close("all")
        return None

    plt.close("all")
    buf.seek(0)
    return buf.getvalue()
