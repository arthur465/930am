"""
Nitro BOS+FVG Scanner — Config
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Watchlist ─────────────────────────────────────────────────────────────────
# Top liquid crypto pairs on OKX
# These have tight spreads and respond well to US market open volatility
ALL_SYMBOLS = [
    "BTC/USDT",   # Bitcoin — king, highest volume
    "ETH/USDT",   # Ethereum — follows BTC, deep liquidity
    "SOL/USDT",   # Solana — high beta, strong intraday moves
    "AVAX/USDT",  # Avalanche — good volatility
    "LINK/USDT",  # Chainlink — solid volume
    "ARB/USDT",   # Arbitrum — L2 leader, good volume
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
SCAN_INTERVAL_SECONDS = 60   # scan every 60s during session
