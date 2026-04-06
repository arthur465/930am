"""
Nitro BOS Scanner — Config
Strategy: OR (9:30–9:40) → Volatility → BOS → Enter immediately
Entry on BOS candle close. No FVG retest.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ───────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Coinalyze API (crypto OHLCV + buy/sell volume confirmation) ────────────────
COINALYZE_API_KEY  = os.getenv("COINALYZE_API_KEY", "")
COINALYZE_EXCHANGE = os.getenv("COINALYZE_EXCHANGE", "binance")  # binance | okx | bybit

# ── Watchlist ──────────────────────────────────────────────────────────────────
# Stocks/ETFs — fetched via yfinance (market hours only, Mon–Fri)
STOCK_SYMBOLS = [
    "AAPL",   # Apple
    "GOOGL",  # Alphabet
    "AMZN",   # Amazon
    "TSLA",   # Tesla
    "NVDA",   # Nvidia
    "SPY",    # S&P 500 ETF
    "QQQ",    # Nasdaq 100 ETF
]

# Crypto — fetched via Coinalyze (primary) / OKX fallback, runs 24/7
CRYPTO_SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
]

ALL_SYMBOLS = STOCK_SYMBOLS + CRYPTO_SYMBOLS

# ── Strategy timing (Eastern Time) ────────────────────────────────────────────
OR_START_HOUR, OR_START_MIN = 9, 30
OR_END_HOUR,   OR_END_MIN   = 9, 40
SCAN_END_HOUR, SCAN_END_MIN = 11, 0

# ── BOS candle quality ─────────────────────────────────────────────────────────
STRONG_BODY_PCT = 0.55   # body must be >= 55% of candle range
MAX_WICK_PCT    = 0.35   # wick in trade direction <= 35%
CHOP_LOOKBACK   = 5
MAX_CHOP_FLIPS  = 3

# ── Volatility filters ─────────────────────────────────────────────────────────
OR_MIN_RANGE_PCT = 0.15  # OR range >= 0.15% of price
ATR_MIN_RATIO    = 0.75  # current ATR / avg ATR >= 0.75
ATR_LOOKBACK     = 14

# ── Entry / SL / TP ───────────────────────────────────────────────────────────
# Entry:  BOS candle close
# SL:     Just inside the OR boundary that was broken
#           Long  → SL = OR_HIGH * (1 - SL_BUFFER_PCT)
#           Short → SL = OR_LOW  * (1 + SL_BUFFER_PCT)
SL_BUFFER_PCT = 0.001   # 0.1% inside the OR level

# TP fixed at TARGET_R * risk from entry. 1H swing level shown in alert as reference.
TARGET_R = 1.5

# ── Coinalyze CVD confirmation (crypto only) ───────────────────────────────────
# Long BOS: buy/sell ratio must be > CVD_MIN_RATIO  (more buyers)
# Short BOS: buy/sell ratio must be < (1 - CVD_MIN_RATIO)
# Set to 0.0 to disable (always pass through)
CVD_MIN_RATIO = 0.50    # 0.50 = effectively disabled; raise to 0.55–0.60 to tighten

# ── Paper Trading ──────────────────────────────────────────────────────────────
PAPER_TRADE_SIZE_USDT = float(os.getenv("PAPER_TRADE_SIZE_USDT", "100"))

# ── Loop ──────────────────────────────────────────────────────────────────────
SCAN_INTERVAL_SECONDS = 60
