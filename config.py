"""
Nitro BOS+FVG Scanner — Config
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Coinalyze API (for enhanced execution data) ──────────────────────────────
COINALYZE_API_KEY = os.getenv("COINALYZE_API_KEY", "")
COINALYZE_EXCHANGE = os.getenv("COINALYZE_EXCHANGE", "binance")  # binance, okx, bybit, coinbase

# ── Watchlist ─────────────────────────────────────────────────────────────────
# Focus on top 2 most liquid pairs for best execution and minimal rate limit issues
# BTC and ETH have deepest liquidity and respond best to US market open volatility
ALL_SYMBOLS = [
    "BTC/USDT",   # Bitcoin — king, highest volume, tightest spreads
    "ETH/USDT",   # Ethereum — follows BTC, deep liquidity
]

# ── Strategy timing (Eastern Time) ───────────────────────────────────────────
OR_START_HOUR, OR_START_MIN = 9, 30
OR_END_HOUR,   OR_END_MIN   = 9, 40
SCAN_END_HOUR, SCAN_END_MIN = 11, 0

# ── BOS / Candle quality ──────────────────────────────────────────────────────
STRONG_BODY_PCT = 0.55
MAX_WICK_PCT    = 0.35
CHOP_LOOKBACK   = 5
MAX_CHOP_FLIPS  = 3

# ── FVG ───────────────────────────────────────────────────────────────────────
FVG_MIN_SIZE_PCT = 0.03

# ── Volatility filters ───────────────────────────────────────────────────────
OR_MIN_RANGE_PCT = 0.15
ATR_MIN_RATIO    = 0.75
ATR_LOOKBACK     = 14

# ── Risk / Reward ─────────────────────────────────────────────────────────────
MIN_RR = 1.5

# ── Paper Trading ─────────────────────────────────────────────────────────────
# OKX API keys required for actual order placement.
# If not set, trades are logged only (no real orders placed).
# Enable "Simulated Trading" on your OKX account before using.
PAPER_TRADE_SIZE_USDT = float(os.getenv("PAPER_TRADE_SIZE_USDT", "100"))

# ── Loop ──────────────────────────────────────────────────────────────────────
SCAN_INTERVAL_SECONDS = 90   # scan every 90s (was 60s - reduced to ease API pressure)
