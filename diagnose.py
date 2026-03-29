"""
diagnose.py — Nitro Scanner diagnostic (Polygon.io version)
Run locally to verify data is flowing before deploying.

Usage:
    POLYGON_API_KEY=your_key python diagnose.py
"""
import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
import pytz
from data.fetcher import get_candles

ET = pytz.timezone("America/New_York")

SYMBOLS = ["AAPL", "TSLA", "SPY", "QQQ", "NVDA", "BTC-USD"]

OR_MIN_RANGE_PCT = 0.15
ATR_MIN_RATIO    = 0.75
ATR_LOOKBACK     = 14
STRONG_BODY_PCT  = 0.55


def fmt(p):
    if p > 1000: return f"${p:,.2f}"
    elif p > 10: return f"${p:.2f}"
    return f"${p:.4f}"


async def diagnose_symbol(symbol):
    print(f"\n{'='*50}")
    print(f"  {symbol}")
    print(f"{'='*50}")

    # 1m data
    print("\n[1] Fetching 1m candles via Polygon...")
    candles_1m = await get_candles(symbol, "1m", "1d")

    if candles_1m.empty:
        print("    ❌ NO DATA — check POLYGON_API_KEY and that market is open")
        return

    print(f"    ✅ {len(candles_1m)} candles")
    print(f"    First: {candles_1m.index[0].strftime('%I:%M %p ET')}")
    print(f"    Last:  {candles_1m.index[-1].strftime('%I:%M %p ET')}")
    print(f"    Price: {fmt(float(candles_1m['Close'].iloc[-1]))}")

    # OR
    print("\n[2] Opening range 9:30–9:39...")
    or_candles = candles_1m.between_time("09:30", "09:39")
    if or_candles.empty:
        print("    ❌ No OR candles — run during/after 9:30 AM ET")
        return

    or_high = float(or_candles["High"].max())
    or_low  = float(or_candles["Low"].min())
    or_pct  = (or_high - or_low) / or_low * 100
    print(f"    {len(or_candles)} candles  H={fmt(or_high)}  L={fmt(or_low)}  Range={or_pct:.3f}%")
    if or_pct < OR_MIN_RANGE_PCT:
        print(f"    ❌ OR too tight ({or_pct:.3f}% < {OR_MIN_RANGE_PCT}%) — would skip symbol")
    else:
        print(f"    ✅ OR range OK")

    # ATR
    print("\n[3] ATR check (1H)...")
    candles_1h = await get_candles(symbol, "1h", "5d")
    if candles_1h.empty:
        print("    ⚠️  No 1H data")
    else:
        import pandas as pd
        high  = candles_1h["High"]
        low   = candles_1h["Low"]
        close = candles_1h["Close"].shift(1)
        tr    = pd.concat([(high-low), (high-close).abs(), (low-close).abs()], axis=1).max(axis=1)
        atr   = tr.rolling(ATR_LOOKBACK).mean().dropna()
        if len(atr) >= 2:
            cur = float(atr.iloc[-1])
            avg = float(atr.iloc[-ATR_LOOKBACK:].mean())
            ratio = cur / avg if avg > 0 else 1.0
            print(f"    ATR={cur:.4f}  Avg={avg:.4f}  Ratio={ratio:.2f}")
            if ratio < ATR_MIN_RATIO:
                print(f"    ❌ ATR compressed — would skip symbol")
            else:
                print(f"    ✅ ATR OK")

    # Post-OR / BOS
    print("\n[4] Post-OR candles + BOS check...")
    post_or = candles_1m.between_time("09:40", "23:59")
    if post_or.empty:
        print("    ⚠️  No post-OR candles yet")
        return

    print(f"    {len(post_or)} post-OR candles")
    bos_found = False
    for idx, row in post_or.iterrows():
        r = row["High"] - row["Low"]
        if r == 0: continue
        body = abs(row["Close"] - row["Open"]) / r
        bull = row["Close"] > or_high and row["Close"] > row["Open"]
        bear = row["Close"] < or_low  and row["Close"] < row["Open"]
        if (bull or bear) and body >= STRONG_BODY_PCT:
            d = "LONG" if bull else "SHORT"
            print(f"    ✅ BOS {d} @ {fmt(float(row['Close']))} ({idx.strftime('%I:%M %p')}) body={body:.0%}")
            bos_found = True
            break
    if not bos_found:
        print(f"    ℹ️  No clean BOS yet (OR H={fmt(or_high)} L={fmt(or_low)})")


async def main():
    now = datetime.now(ET)
    print(f"\nNitro Diagnostic — {now.strftime('%A %I:%M %p ET')}")

    if not os.getenv("POLYGON_API_KEY"):
        print("\n❌ POLYGON_API_KEY not set!")
        print("   Run: POLYGON_API_KEY=your_key python diagnose.py\n")
        return

    if now.weekday() >= 5:
        print("⚠️  Weekend — market closed, intraday data likely empty\n")

    for sym in SYMBOLS:
        await diagnose_symbol(sym)

    print(f"\n{'='*50}")
    print("DONE")
    print("If data is flowing → deploy to Railway and add POLYGON_API_KEY env var")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    asyncio.run(main())
