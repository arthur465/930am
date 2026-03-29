"""
analysis/fvg_detector.py
Detects Fair Value Gaps (FVGs) on 1m and 3m timeframes AFTER the BOS.

FVG Rules:
  - 3-candle pattern: gap between candle[i].high and candle[i+2].low (bullish)
    or candle[i].low and candle[i+2].high (bearish)
  - Must form STRICTLY after the BOS candle
  - Gap must be >= FVG_MIN_SIZE_PCT of price
  - Priority: 3m FVG > 1m FVG | If multiple same TF: most recent wins
"""
import logging
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from config import FVG_MIN_SIZE_PCT

logger = logging.getLogger("fvg")


@dataclass
class FVG:
    top: float
    bottom: float
    mid: float
    timeframe: str       # '1m' or '3m'
    formed_at: object    # pd.Timestamp of 3rd candle
    direction: str       # 'bullish' or 'bearish'

    @property
    def size_pct(self) -> float:
        return (self.top - self.bottom) / self.bottom * 100


# ── Core detection ────────────────────────────────────────────────────────────

def find_fvgs(
    candles: pd.DataFrame,
    bos_time: object,
    direction: str,
    timeframe: str,
) -> List[FVG]:
    """Find all valid FVGs that formed after the BOS candle."""
    if candles.empty:
        return []

    post_bos = candles[candles.index > bos_time]
    if len(post_bos) < 3:
        return []

    rows = list(post_bos.itertuples())
    fvgs: List[FVG] = []

    for i in range(len(rows) - 2):
        c1, c2, c3 = rows[i], rows[i + 1], rows[i + 2]

        if direction == "long":
            # Bullish FVG: gap between c1.High and c3.Low
            if c1.High < c3.Low:
                size_pct = (c3.Low - c1.High) / c1.High * 100
                if size_pct >= FVG_MIN_SIZE_PCT:
                    fvgs.append(FVG(
                        top=float(c3.Low),
                        bottom=float(c1.High),
                        mid=float((c3.Low + c1.High) / 2),
                        timeframe=timeframe,
                        formed_at=c3.Index,
                        direction="bullish",
                    ))
        else:
            # Bearish FVG: gap between c3.High and c1.Low
            if c1.Low > c3.High:
                size_pct = (c1.Low - c3.High) / c1.Low * 100
                if size_pct >= FVG_MIN_SIZE_PCT:
                    fvgs.append(FVG(
                        top=float(c1.Low),
                        bottom=float(c3.High),
                        mid=float((c1.Low + c3.High) / 2),
                        timeframe=timeframe,
                        formed_at=c3.Index,
                        direction="bearish",
                    ))

    return fvgs


def get_best_fvg(fvgs_1m: List[FVG], fvgs_3m: List[FVG]) -> Optional[FVG]:
    """
    Priority:
      1. Most recent 3m FVG
      2. Most recent 1m FVG
    """
    if fvgs_3m:
        best = max(fvgs_3m, key=lambda f: f.formed_at)
        logger.info(f"Best FVG: 3m  top={best.top:.4f}  bot={best.bottom:.4f}  size={best.size_pct:.3f}%")
        return best
    if fvgs_1m:
        best = max(fvgs_1m, key=lambda f: f.formed_at)
        logger.info(f"Best FVG: 1m  top={best.top:.4f}  bot={best.bottom:.4f}  size={best.size_pct:.3f}%")
        return best
    return None


# ── Retest & RR ───────────────────────────────────────────────────────────────

def is_retesting(current_price: float, fvg: FVG) -> bool:
    """Price has entered the FVG zone."""
    return fvg.bottom <= current_price <= fvg.top


def get_1h_tp(candles_1h: pd.DataFrame, current_price: float, direction: str) -> Optional[float]:
    """
    Find the nearest 1H high (longs) or 1H low (shorts) beyond current price.
    Uses today's 1H candles as reference levels.
    """
    if candles_1h.empty:
        return None

    if direction == "long":
        candidates = [float(row["High"]) for _, row in candles_1h.iterrows()
                      if float(row["High"]) > current_price * 1.001]
        return min(candidates) if candidates else None
    else:
        candidates = [float(row["Low"]) for _, row in candles_1h.iterrows()
                      if float(row["Low"]) < current_price * 0.999]
        return max(candidates) if candidates else None


def calculate_rr(entry: float, fvg: FVG, tp: float, direction: str) -> float:
    """Calculate reward-to-risk ratio."""
    if direction == "long":
        sl     = fvg.bottom
        risk   = entry - sl
        reward = tp - entry
    else:
        sl     = fvg.top
        risk   = sl - entry
        reward = entry - tp

    if risk <= 0 or reward <= 0:
        return 0.0

    return round(reward / risk, 2)
