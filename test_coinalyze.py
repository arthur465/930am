"""
test_coinalyze.py
─────────────────
Test script for Coinalyze API integration.
Run this to verify your API key and see enhanced data in action.

Usage:
    python test_coinalyze.py
"""

import asyncio
import logging
import sys
from datetime import datetime

import pytz

from data.coinalyze_fetcher import (
    get_execution_candles,
    get_trade_context,
    cleanup,
)
from config import COINALYZE_API_KEY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("test")
ET = pytz.timezone("America/New_York")


async def test_basic_fetch():
    """Test basic OHLCV fetch."""
    logger.info("\n" + "="*60)
    logger.info("TEST 1: Basic OHLCV Fetch")
    logger.info("="*60)
    
    symbol = "BTC/USDT"
    interval = "1m"
    lookback = 10
    
    logger.info(f"Fetching {interval} candles for {symbol} (last {lookback} min)")
    
    df = await get_execution_candles(
        symbol=symbol,
        interval=interval,
        lookback_minutes=lookback,
    )
    
    if df.empty:
        logger.error("❌ No data returned — check API key or network")
        return False
    
    logger.info(f"✅ Retrieved {len(df)} candles")
    logger.info(f"\nLatest candle:")
    logger.info(f"{df.tail(1).to_string()}")
    
    # Show buy/sell volume if available
    if "BuyVolume" in df.columns:
        latest = df.iloc[-1]
        total_vol = latest["Volume"]
        buy_vol = latest["BuyVolume"]
        sell_vol = total_vol - buy_vol
        buy_pct = (buy_vol / total_vol * 100) if total_vol > 0 else 0
        
        logger.info(f"\n📊 Order Flow:")
        logger.info(f"   Total Vol:  {total_vol:,.0f}")
        logger.info(f"   Buy Vol:    {buy_vol:,.0f} ({buy_pct:.1f}%)")
        logger.info(f"   Sell Vol:   {sell_vol:,.0f} ({100-buy_pct:.1f}%)")
        
        if buy_pct > 60:
            logger.info("   🟢 Strong buying pressure")
        elif buy_pct < 40:
            logger.info("   🔴 Strong selling pressure")
        else:
            logger.info("   🟡 Balanced")
    
    return True


async def test_trade_context():
    """Test trade entry context capture."""
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Trade Entry Context")
    logger.info("="*60)
    
    symbol = "BTC/USDT"
    entry_time = datetime.now(ET)
    
    logger.info(f"Capturing market context for {symbol} at {entry_time.strftime('%I:%M:%S %p')}")
    
    context = await get_trade_context(
        symbol=symbol,
        entry_time=entry_time,
        lookback_minutes=5,
    )
    
    if not context:
        logger.error("❌ Failed to get trade context")
        return False
    
    logger.info(f"✅ Context captured")
    logger.info(f"\n📈 Entry Candle:")
    logger.info(f"   Open:   {context['entry_candle']['Open']:.2f}")
    logger.info(f"   High:   {context['entry_candle']['High']:.2f}")
    logger.info(f"   Low:    {context['entry_candle']['Low']:.2f}")
    logger.info(f"   Close:  {context['entry_candle']['Close']:.2f}")
    
    logger.info(f"\n📊 Volume Breakdown:")
    logger.info(f"   Total:       {context['total_volume']:,.0f}")
    logger.info(f"   Buy:         {context['buy_volume']:,.0f}")
    logger.info(f"   Sell:        {context['sell_volume']:,.0f}")
    logger.info(f"   Buy/Sell:    {context['buy_sell_ratio']:.1%}")
    logger.info(f"   Avg Trade:   {context['avg_trade_size']:,.2f}")
    
    return True


async def test_multiple_timeframes():
    """Test fetching different timeframes."""
    logger.info("\n" + "="*60)
    logger.info("TEST 3: Multiple Timeframes")
    logger.info("="*60)
    
    symbol = "ETH/USDT"
    intervals = ["1m", "5m", "15m", "1h"]
    
    for interval in intervals:
        df = await get_execution_candles(
            symbol=symbol,
            interval=interval,
            lookback_minutes=60,
        )
        
        if not df.empty:
            logger.info(f"✅ {interval:>4s}: {len(df):>3d} candles")
        else:
            logger.warning(f"⚠️  {interval:>4s}: No data")
    
    return True


async def main():
    """Run all tests."""
    logger.info("\n" + "="*60)
    logger.info("Coinalyze API Integration Test")
    logger.info("="*60)
    
    if not COINALYZE_API_KEY:
        logger.error("\n❌ COINALYZE_API_KEY not set in .env file")
        logger.info("\nTo fix:")
        logger.info("1. Sign up at https://coinalyze.net")
        logger.info("2. Get API key from https://coinalyze.net/account/api-key/")
        logger.info("3. Add to .env: COINALYZE_API_KEY=your_key_here")
        return
    
    logger.info(f"\n🔑 API Key: {COINALYZE_API_KEY[:8]}...")
    
    try:
        # Run tests
        success1 = await test_basic_fetch()
        await asyncio.sleep(2)
        
        success2 = await test_trade_context()
        await asyncio.sleep(2)
        
        success3 = await test_multiple_timeframes()
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("Test Summary")
        logger.info("="*60)
        logger.info(f"Basic Fetch:         {'✅ PASS' if success1 else '❌ FAIL'}")
        logger.info(f"Trade Context:       {'✅ PASS' if success2 else '❌ FAIL'}")
        logger.info(f"Multiple Timeframes: {'✅ PASS' if success3 else '❌ FAIL'}")
        
        if all([success1, success2, success3]):
            logger.info("\n🎉 All tests passed! Coinalyze integration ready.")
        else:
            logger.warning("\n⚠️  Some tests failed. Check logs above.")
    
    finally:
        # Clean up
        await cleanup()
        logger.info("\n✅ Cleanup complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nTest interrupted.")
    except Exception as e:
        logger.error(f"\nTest failed: {e}", exc_info=True)
