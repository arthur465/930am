"""
notifications/telegram_bot.py
Sends BOS entry alerts and trade outcome results via Telegram.
"""
import logging
from datetime import datetime
from typing import Optional

import aiohttp
import pytz

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TARGET_R

logger = logging.getLogger("telegram")
ET = pytz.timezone("America/New_York")

DIR_EMOJI = {"long": "🟢", "short": "🔴"}
DIR_LABEL = {"long": "LONG  📈", "short": "SHORT 📉"}


async def _send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured — skipping")
        return
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.error(f"Telegram {r.status}: {await r.text()}")
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")


def _fmt(p: float) -> str:
    if p > 1000:
        return f"${p:,.2f}"
    elif p > 10:
        return f"${p:.2f}"
    return f"${p:.4f}"


async def send_startup() -> None:
    now = datetime.now(ET).strftime("%I:%M %p ET")
    symbols = "AAPL · GOOGL · AMZN · TSLA · NVDA · SPY · QQQ · BTC · ETH"
    msg = (
        "🤖 <b>Nitro BOS Scanner Online</b>\n"
        f"⏰ Started: {now}\n"
        f"📡 Watching: {symbols}\n"
        "⏳ Building opening range 9:30–9:40...\n"
        "\n<i>Alerts fire on BOS confirmation. No FVG retest required.</i>"
    )
    await _send(msg)


async def send_setup_alert(
    symbol:      str,
    direction:   str,
    entry:       float,
    sl:          float,
    tp:          float,
    rr:          float,
    or_high:     float,
    or_low:      float,
    swing_level: Optional[float] = None,
) -> None:
    now_str = datetime.now(ET).strftime("%I:%M %p ET")
    emoji   = DIR_EMOJI.get(direction, "⚪")
    label   = DIR_LABEL.get(direction, direction.upper())
    risk    = abs(entry - sl)
    reward  = abs(tp - entry)

    # OR level that was broken
    broken_level = or_high if direction == "long" else or_low
    broken_label = "OR High" if direction == "long" else "OR Low"

    # SL note
    sl_note = f"just inside {broken_label} (breakout failed)"

    # Swing level line
    swing_line = ""
    if swing_level:
        beyond = "above" if direction == "long" else "below"
        swing_line = f"📌 <b>1H Swing:</b>  {_fmt(swing_level)}  <i>({beyond} — discretionary ext)</i>\n"

    msg = (
        f"🔥 <b>NITRO SETUP — {symbol}</b>\n"
        f"{emoji} <b>{label}</b>  |  {now_str}\n"
        "─────────────────────\n"
        f"📐 <b>Opening Range:</b>  {_fmt(or_low)} – {_fmt(or_high)}\n"
        f"💥 <b>BOS:</b>  Close {'above' if direction == 'long' else 'below'} "
        f"{broken_label} ({_fmt(broken_level)}) → <b>structure broken</b>\n\n"
        f"🎯 <b>Entry:</b>   {_fmt(entry)}  <i>(BOS candle close)</i>\n"
        f"🛑 <b>Stop:</b>    {_fmt(sl)}  <i>({sl_note})</i>\n"
        f"✅ <b>TP {TARGET_R}R:</b>  {_fmt(tp)}\n"
        f"{swing_line}"
        "─────────────────────\n"
        f"📊 Risk: {_fmt(risk)}  |  Reward: {_fmt(reward)}\n"
        f"⚡ <b>R:R = {rr:.1f}R</b>\n"
        "\n<i>✅ OR  ✅ Vol  ✅ BOS  ✅ Entry</i>\n"
        "<i>👀 Watching for outcome...</i>"
    )
    await _send(msg)


async def send_outcome_alert(
    symbol:    str,
    direction: str,
    entry:     float,
    exit:      float,
    sl:        float,
    tp:        float,
    outcome:   str,
    r_mult:    float,
    minutes:   int,
    **kwargs,
) -> None:
    emoji  = {"win": "🎯", "loss": "🛑", "expired": "⏰"}[outcome]
    result = {
        "win":     f"<b>+{r_mult:.2f}R WIN</b>",
        "loss":    f"<b>{r_mult:.2f}R LOSS</b>",
        "expired": f"<b>{r_mult:+.2f}R (session end)</b>",
    }[outcome]

    dir_label = DIR_LABEL.get(direction, direction.upper())
    dur = f"{minutes // 60}h {minutes % 60}m" if minutes >= 60 else f"{minutes}m"

    msg = (
        f"{emoji} <b>{symbol} — {outcome.upper()}</b>\n"
        f"{dir_label}\n"
        "─────────────────────\n"
        f"📥 <b>Entry:</b>  {_fmt(entry)}\n"
        f"📤 <b>Exit:</b>   {_fmt(exit)}\n"
        f"🛑 <b>SL:</b>     {_fmt(sl)}\n"
        f"🎯 <b>TP:</b>     {_fmt(tp)}\n"
        "─────────────────────\n"
        f"⚡ {result}\n"
        f"⏱ Duration: {dur}"
    )
    await _send(msg)


async def send_stats(summary: str) -> None:
    await _send(summary)
