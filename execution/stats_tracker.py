"""
execution/stats_tracker.py
Collects trade results, calculates win rate + total R.
Persists to stats.json so data survives restarts.
"""
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger("stats")
STATS_FILE = os.path.join(os.path.dirname(__file__), "..", "stats.json")


def _empty_stats() -> dict:
    return {
        "trades": [], "total": 0, "wins": 0, "losses": 0, "expired": 0,
        "total_r": 0.0, "win_rate": 0.0, "avg_r": 0.0, "last_updated": "",
    }


def _load() -> dict:
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE) as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load stats.json: {e}")
    return _empty_stats()


def _save(stats: dict):
    try:
        stats["last_updated"] = datetime.now(timezone.utc).isoformat()
        with open(STATS_FILE, "w") as f:
            json.dump(stats, f, indent=2)
    except Exception as e:
        logger.error(f"Could not save stats.json: {e}")


def record_trade(close_result: dict):
    stats = _load()
    stats["trades"].append({**close_result, "closed_at": datetime.now(timezone.utc).isoformat()})
    stats["total"]   += 1
    stats["total_r"]  = round(stats["total_r"] + close_result["r_mult"], 2)

    outcome = close_result["outcome"]
    if outcome == "win":
        stats["wins"] += 1
    elif outcome == "loss":
        stats["losses"] += 1
    else:
        stats["expired"] += 1

    decided         = stats["wins"] + stats["losses"]
    stats["win_rate"] = round((stats["wins"] / decided * 100) if decided > 0 else 0.0, 1)
    stats["avg_r"]    = round((stats["total_r"] / stats["total"]) if stats["total"] > 0 else 0.0, 2)

    _save(stats)
    logger.info(
        f"[stats] {stats['total']} trades | WR={stats['win_rate']}% | "
        f"Total R={stats['total_r']:+.2f} | Avg={stats['avg_r']:+.2f}R"
    )


def get_summary() -> dict:
    return _load()


def format_summary() -> str:
    s = _load()
    if s["total"] == 0:
        return "📊 No trades recorded yet."

    lines = [
        "📊 <b>Session Stats</b>",
        f"Trades:   {s['total']}  ({s['wins']}W / {s['losses']}L / {s['expired']} exp)",
        f"Win Rate: {s['win_rate']}%",
        f"Total R:  {s['total_r']:+.2f}R",
        f"Avg R:    {s['avg_r']:+.2f}R per trade",
    ]

    recent = s["trades"][-5:]
    if recent:
        lines.append("\n<b>Last 5 trades:</b>")
        for t in reversed(recent):
            emoji = {"win": "🎯", "loss": "🛑", "expired": "⏰"}[t["outcome"]]
            lines.append(
                f"{emoji} {t['symbol']} {t['direction'].upper()} {t['r_mult']:+.2f}R"
            )

    return "\n".join(lines)
