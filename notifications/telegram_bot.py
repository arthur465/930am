"""
notifications/telegram_bot.py
Sends setup alerts and trade outcome results via Telegram.
"""
import logging
from datetime import datetime

import aiohttp
import pytz

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger("telegram")
ET = pytz.timezone("America/New_York")

DIRECTION_EMOJI = {"long": "🟢", "short": "🔴"}
DIRECTION_LABEL = {"long": "LONG  📈", "short": "SHORT 📉"}


async def _send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured — skipping send")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"Telegram error {resp.status}: {body}")
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
    msg = (
        "🤖 <b>Nitro BOS+FVG Scanner Online</b>\n"
        f"⏰ Started at {now}\n"
        "📡 Watching: Stocks · Futures · Crypto\n"
        "⏳ Waiting for 9:30 opening range...\n"
        "\n<i>Alerts fire only when ALL conditions are met.</i>"
    )
    await _send(msg)


async def send_setup_alert(
    symbol: str,
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    fvg,
    or_high: float,
    or_low: float,
    rr: float,
) -> None:
    now_str = datetime.now(ET).strftime("%I:%M %p ET")
    emoji   = DIRECTION_EMOJI.get(direction, "⚪")
    label   = DIRECTION_LABEL.get(direction, direction.upper())
    risk    = abs(entry - sl)
    reward  = abs(tp - entry)

    msg = (
        f"🔥 <b>NITRO SETUP — {symbol}</b>\n"
        f"{emoji} <b>{label}</b>  |  {now_str}\n"
        "─────────────────────\n"
        f"📐 <b>BOS:</b> Clean break of OR {'high' if direction == 'long' else 'low'}\n"
        f"    OR Range: {_fmt(or_low)} – {_fmt(or_high)}\n\n"
        f"📦 <b>FVG ({fvg.timeframe}):</b> {_fmt(fvg.bottom)} – {_fmt(fvg.top)}\n"
        f"    Size: {fvg.size_pct:.3f}%\n\n"
        f"🎯 <b>Entry:</b>  {_fmt(entry)}  <i>(retest confirmed)</i>\n"
        f"🛑 <b>Stop:</b>   {_fmt(sl)}  <i>(beyond FVG)</i>\n"
        f"✅ <b>Target:</b> {_fmt(tp)}  <i>(next 1H {'high' if direction == 'long' else 'low'})</i>\n"
        "─────────────────────\n"
        f"📊 Risk: {_fmt(risk)}  |  Reward: {_fmt(reward)}\n"
        f"⚡ <b>R:R = {rr:.1f}R</b>\n"
        "\n<i>✅ OR  ✅ Vol  ✅ BOS  ✅ FVG  ✅ Retest  ✅ R:R</i>\n"
        "<i>👀 Watching for outcome...</i>"
    )
    await _send(msg)


async def send_outcome_alert(
    symbol: str,
    direction: str,
    entry: float,
    exit_price: float,
    sl: float,
    tp: float,
    outcome: str,        # 'win' | 'loss' | 'expired'
    r_multiple: float,
    duration_minutes: int,
) -> None:
    if outcome == "win":
        emoji  = "🎯"
        header = f"<b>TARGET HIT — {symbol}</b>"
        result = f"+{r_multiple:.2f}R WIN"
    elif outcome == "loss":
        emoji  = "🛑"
        header = f"<b>STOP HIT — {symbol}</b>"
        result = f"{r_multiple:.2f}R LOSS"
    else:
        emoji  = "⏰"
        header = f"<b>TRADE EXPIRED — {symbol}</b>"
        result = f"{r_multiple:+.2f}R (2hr max hold)"

    direction_label = DIRECTION_LABEL.get(direction, direction.upper())
    hours   = duration_minutes // 60
    minutes = duration_minutes % 60
    dur_str = f"{hours}h {minutes}m" if hours else f"{minutes}m"

    msg = (
        f"{emoji} {header}\n"
        f"{DIRECTION_EMOJI.get(direction, '')} {direction_label}\n"
        "─────────────────────\n"
        f"📥 <b>Entry:</b>   {_fmt(entry)}\n"
        f"📤 <b>Exit:</b>    {_fmt(exit_price)}\n"
        f"🎯 <b>TP was:</b>  {_fmt(tp)}\n"
        f"🛑 <b>SL was:</b>  {_fmt(sl)}\n"
        "─────────────────────\n"
        f"⚡ <b>Result: {result}</b>\n"
        f"⏱ Duration: {dur_str}"
    )
    await _send(msg)


async def send_outcome_alert(
    symbol: str,
    direction: str,
    entry: float,
    exit: float,
    sl: float,
    tp: float,
    outcome: str,
    r_mult: float,
    minutes: int,
    fvg_tf: str = "",
    **kwargs,
) -> None:
    emoji  = {"win": "🎯", "loss": "🛑", "expired": "⏰"}[outcome]
    result = {
        "win":     f"<b>+{r_mult:.2f}R WIN</b>",
        "loss":    f"<b>{r_mult:.2f}R LOSS</b>",
        "expired": f"<b>{r_mult:+.2f}R (session end)</b>",
    }[outcome]

    direction_label = "LONG 📈" if direction == "long" else "SHORT 📉"
    dur = f"{minutes // 60}h {minutes % 60}m" if minutes >= 60 else f"{minutes}m"

    msg = (
        f"{emoji} <b>{symbol} — {outcome.upper()}</b>\n"
        f"{direction_label}  |  {fvg_tf} FVG entry\n"
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
