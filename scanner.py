"""
scanner.py
──────────
Main scanner: Structure-first (swing + internal aligned), volatility confirms.
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional
import ccxt
import pandas as pd

from config import (
    SYMBOLS, TRADE_TIMEFRAME, HIGHER_TIMEFRAME,
    SWING_LENGTH, INTERNAL_LENGTH,
    STOP_LOSS_SWING, FIXED_STOP_PCT, TP1_R, TP2_R,
    RISK_PER_TRADE_PCT
)
from core.structure import detect_structure, get_htf_bias, is_aligned_with_htf
from core.volatility import analyze_volatility
from core.coinglass import get_cvd_delta
from core.database import SignalDB
from core.telegram_alert import send_alert
from core.structure_snapshot import scan_structure_snapshot, format_snapshot_alert


logger = logging.getLogger("scanner")


class IBOSScanner:
    """Main scanner orchestrating structure + volatility analysis."""
    
    def __init__(self):
        self.exchange = None
        self.db = SignalDB("data/ibos_signals.db")
        self.last_signals = {}  # Prevent duplicates
        self.cache = {}  # Cache for higher timeframes
        self.cache_duration = {'4h': 3600, '1h': 600}  # Cache 4h for 1hr, 1h for 10min
    
    async def initialize(self):
        """Initialize exchange connection."""
        self.exchange = ccxt.okx({
            'enableRateLimit': True,
            'timeout': 120000,  # Increased to 120 seconds
            'rateLimit': 100,   # 100ms between requests
            'options': {
                'defaultType': 'spot',
                'recvWindow': 120000,
            },
        })
        logger.info("Scanner initialized with extended timeout (120s)")
    
    async def cleanup(self):
        """Cleanup resources."""
        if self.exchange:
            await self.exchange.close()
    
    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 200) -> pd.DataFrame:
        """Fetch OHLCV data with caching for higher timeframes and improved retry logic."""
        cache_key = f"{symbol}_{timeframe}"
        now = datetime.now()
        
        # Check cache for higher timeframes
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            cache_age = (now - cached_time).total_seconds()
            
            if timeframe in self.cache_duration and cache_age < self.cache_duration[timeframe]:
                logger.info(f"Using cached {timeframe} data for {symbol} ({cache_age:.0f}s old)")
                return cached_data
        
        # Fetch fresh data with retry
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                df = pd.DataFrame(
                    ohlcv,
                    columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
                )
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
                
                # Cache higher timeframes
                if timeframe in self.cache_duration:
                    self.cache[cache_key] = (df, now)
                    logger.info(f"Cached {timeframe} data for {symbol}")
                
                return df
                
            except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
                if attempt < max_retries - 1:
                    wait_time = 3 * (2 ** attempt)  # 3s, 6s, 12s exponential backoff
                    logger.warning(f"Network error fetching {symbol} {timeframe} on attempt {attempt + 1}/{max_retries}, retrying in {wait_time}s: {e}")
                    import time
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {symbol} {timeframe} after {max_retries} attempts")
                    raise
    
    async def scan_symbol(self, symbol: str) -> int:
        """
        Scan single symbol for structure breaks.
        
        Flow:
        1. Fetch all timeframes ONCE (with caching)
        2. Check HTF bias (1h/4h)
        3. Structure snapshot check
        4. Detect swing structure (50-bar on 15m)
        5. Check internal structure (5-bar on 15m) confirms
        6. Verify alignment with HTF
        7. If aligned → score with volatility metrics
        8. Alert if score passes thresholds
        
        Returns:
            Number of signals generated
        """
        logger.info(f"Scanning {symbol}...")
        
        # ── FETCH ALL DATA ONCE (will use cache when available) ──
        df_4h = self.fetch_ohlcv(symbol, "4h", limit=200)
        df_1h = self.fetch_ohlcv(symbol, "1h", limit=200)
        df_15m = self.fetch_ohlcv(symbol, TRADE_TIMEFRAME, limit=250)
        
        # ── STRUCTURE SNAPSHOT CHECK ──
        snapshot = scan_structure_snapshot(df_4h, df_1h, df_15m)
        if snapshot:
            # Structure shifted - send snapshot alert
            snapshot_alert = format_snapshot_alert(snapshot, symbol)
            await send_alert(snapshot_alert)
            logger.info(f"Structure snapshot sent for {symbol}")
        
        # ── IBOS SIGNAL DETECTION ──
        # Reuse 1h data for HTF bias (no additional fetch)
        htf_bias = get_htf_bias(df_1h)
        logger.info(f"{symbol} HTF ({HIGHER_TIMEFRAME}) bias: {htf_bias}")
        
        # Reuse 15m data for structure detection
        df = df_15m
        
        # ── STEP 1: Structure Detection ──
        structure = detect_structure(df, symbol)
        
        if not structure:
            # No aligned structure break
            return 0
        
        # ── STEP 2: Check HTF Alignment ──
        if not is_aligned_with_htf(structure['direction'], htf_bias):
            logger.info(
                f"{symbol} {structure['direction']} signal REJECTED: "
                f"Against HTF bias ({htf_bias})"
            )
            return 0
        
        logger.info(f"{symbol} {structure['direction']} ALIGNED with HTF {htf_bias} ✅")
        logger.info(f"{symbol} {structure['direction']} ALIGNED with HTF {htf_bias} ✅")
        
        # ── STEP 3: Fetch CVD (optional - graceful if fails) ──
        cvd_delta = None
        try:
            cvd_delta = await get_cvd_delta(symbol)
        except Exception as e:
            logger.warning(f"CVD fetch failed (continuing without it): {e}")
        
        # ── STEP 4: Volatility Scoring ──
        volatility = analyze_volatility(df, cvd_delta)
        
        # ── STEP 5: Calculate Levels ──
        entry_price = structure['entry_price']
        direction = structure['direction']
        
        # Use swing break price as stop reference
        swing_break = structure['swing_break']['break_price']
        
        if direction == 'LONG':
            stop_loss = swing_break * 0.998  # Slightly below break
        else:
            stop_loss = swing_break * 1.002  # Slightly above break
        
        risk = abs(entry_price - stop_loss)
        tp1 = entry_price + (risk * TP1_R) if direction == 'LONG' else entry_price - (risk * TP1_R)
        tp2 = entry_price + (risk * TP2_R) if direction == 'LONG' else entry_price - (risk * TP2_R)
        risk_pct = (risk / entry_price) * 100
        
        # ── STEP 6: Format Alert ──
        swing_type = structure['swing_break']['type']
        internal_type = structure['internal_break']['type']
        
        emoji = '🟢' if direction == 'LONG' else '🔴'
        fire = '🔥' * min(3, volatility['score'] // 30)  # More fire for higher scores
        
        alert_lines = [
            f"{emoji} <b>{swing_type}</b> {fire}\n",
            f"<b>{symbol}</b>",
            f"HTF ({HIGHER_TIMEFRAME}): {htf_bias}",
            f"Swing: {swing_type}",
            f"Internal: {internal_type} ✅\n",
            f"Entry: ${entry_price:,.2f}",
            f"Stop: ${stop_loss:,.2f}",
            f"TP1: ${tp1:,.2f} ({TP1_R}R)",
            f"TP2: ${tp2:,.2f} ({TP2_R}R)",
            f"Risk: {risk_pct:.2f}%\n",
            f"📊 <b>VOLATILITY SCORE: {volatility['score']}/100</b>",
            f"{volatility['decision']}",
            f"Confidence: {volatility['confidence']}\n",
            f"<b>Metrics:</b>",
            f"  ATR Ratio: {volatility['atr_ratio']:.2f}x",
            f"  Volume: {volatility['volume_ratio']:.2f}x",
        ]
        
        if cvd_delta:
            alert_lines.append(f"  CVD Delta: ${cvd_delta:,.0f}")
        else:
            alert_lines.append(f"  CVD: Unavailable")
        
        alert_lines.append(f"  Body%: {volatility['body_pct']:.1f}%")
        
        # Action recommendation
        if volatility['score'] >= 70:
            alert_lines.append(f"\n🎯 <b>ACTION: ENTER FULL SIZE</b>")
        elif volatility['score'] >= 50:
            alert_lines.append(f"\n⚠️ <b>ACTION: ENTER 50% SIZE</b>")
        else:
            alert_lines.append(f"\n❌ <b>ACTION: SKIP (Wait for retest)</b>")
        
        alert_text = '\n'.join(alert_lines)
        
        # ── STEP 7: Save to DB ──
        signal_id = self.db.save_signal(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            stop_loss=stop_loss,
            tp1=tp1,
            tp2=tp2,
            volatility_score=volatility['score'],
            decision=volatility['decision'],
            atr_ratio=volatility['atr_ratio'],
            volume_ratio=volatility['volume_ratio'],
            cvd_delta=cvd_delta,
            body_pct=volatility['body_pct']
        )
        
        # ── STEP 8: Send Alert ──
        await send_alert(alert_text)
        
        logger.info(
            f"Signal #{signal_id} processed: {symbol} {direction} "
            f"(score: {volatility['score']}, decision: {volatility['decision']})"
        )
        
        return 1
    
    async def scan_all(self) -> int:
        """Scan all configured symbols."""
        total_signals = 0
        
        for symbol in SYMBOLS:
            try:
                signals = await self.scan_symbol(symbol)
                total_signals += signals
                
                # Small delay between symbols
                await asyncio.sleep(2)
            
            except Exception as e:
                logger.error(f"Error scanning {symbol}: {e}", exc_info=True)
        
        return total_signals
