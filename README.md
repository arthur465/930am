# Nitro BOS + FVG Scanner (OKX Edition)

9:30 AM ET opening range scanner → Break of Structure → Fair Value Gap retest → Paper trade

Now powered by **OKX** for unlimited free crypto market data.

## Strategy

- **Session**: 9:30 AM – 11:00 AM ET (Mon–Fri)
- **Watchlist**: Top liquid crypto pairs (BTC/USDT, ETH/USDT, SOL/USDT, AVAX/USDT, LINK/USDT, ARB/USDT)
- **Entry**: BOS on OR break → FVG retest on 1m or 3m chart
- **Exit**: TP at nearest 1H high/low, SL just beyond FVG edge

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Telegram bot token (optional but recommended)
```bash
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"
```

### 3. Run
```bash
python main.py
```

## Key Features

- **OKX data source**: No API key needed for market data, generous rate limits
- **Smart caching**: Candles cached for 50s (1m), 185s (3m), 300s (1h) → minimal API calls
- **Paper trading**: Logs all trades with entry/exit/R-multiple/win rate stats
- **Telegram alerts**: Real-time BOS/FVG/trade notifications

## Files

- `main.py` — Entry point, session scheduler
- `config.py` — Watchlist, timing, filters
- `data/fetcher.py` — OKX/ccxt OHLCV fetcher
- `data/cache.py` — TTL cache + rate limiter
- `analysis/scanner.py` — Main scan loop
- `analysis/bos_detector.py` — Break of structure detection
- `analysis/fvg_detector.py` — Fair value gap detection
- `analysis/opening_range.py` — OR high/low calculation
- `execution/paper_trader.py` — Trade tracking + exit logic
- `execution/stats_tracker.py` — Win rate, R stats, session summary
- `notifications/telegram_bot.py` — Alert sender

## Railway Deployment

1. Push to GitHub
2. Connect repo to Railway
3. Add env vars: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
4. Deploy

Bot will sleep outside market hours and wake at 9:30 AM ET automatically.

## Rate Limits

OKX allows **20 requests per 2 seconds** for public endpoints (600/min). The cache layer keeps actual API calls well under this limit — expect ~6-8 real calls per minute during scanning.

No rate limit issues. Ever.
