"""
Time-based analysis: daily and hourly trade distributions.
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def _parse_date(timestamp_str):
    """Extract date from ISO timestamp string."""
    if not timestamp_str:
        return None
    try:
        return timestamp_str[:10]  # "YYYY-MM-DD"
    except (ValueError, TypeError):
        return None


def _parse_hour(timestamp_str):
    """Extract hour from ISO timestamp string."""
    if not timestamp_str:
        return None
    try:
        return int(timestamp_str[11:13])
    except (ValueError, TypeError, IndexError):
        return None


def compute_daily_stats(trades):
    """Compute daily aggregated statistics."""
    daily = defaultdict(lambda: {
        "trade_count": 0,
        "buy_count": 0,
        "sell_count": 0,
        "buy_amount": 0.0,
        "sell_amount": 0.0,
        "pnl": 0.0,
        "pnl_count": 0,
        "markets_traded": set(),
    })

    for t in trades:
        ts = t.get("timestamp", "")
        date = _parse_date(ts)
        if not date:
            continue

        side = t.get("side", "")
        notional = float(t.get("notional", 0) or 0)
        pnl = t.get("realized_pnl", "")
        market = t.get("market_slug", "") or t.get("market_question", "")

        d = daily[date]
        d["trade_count"] += 1

        if side == "BUY":
            d["buy_count"] += 1
            d["buy_amount"] += notional
        elif side in ("SELL", "SETTLE"):
            d["sell_count"] += 1
            d["sell_amount"] += notional

        if market:
            d["markets_traded"].add(market)

        if pnl != "" and pnl is not None:
            try:
                d["pnl"] += float(pnl)
                d["pnl_count"] += 1
            except (ValueError, TypeError):
                pass

    results = []
    for date in sorted(daily.keys()):
        d = daily[date]
        results.append({
            "date": date,
            "trade_count": d["trade_count"],
            "buy_count": d["buy_count"],
            "sell_count": d["sell_count"],
            "buy_amount": round(d["buy_amount"], 2),
            "sell_amount": round(d["sell_amount"], 2),
            "realized_pnl": round(d["pnl"], 2),
            "unique_markets": len(d["markets_traded"]),
        })

    return results


def compute_hourly_distribution(trades):
    """Compute hourly distribution of trades."""
    hourly = defaultdict(int)
    for t in trades:
        ts = t.get("timestamp", "")
        hour = _parse_hour(ts)
        if hour is not None:
            hourly[hour] += 1

    dist = []
    for h in range(24):
        dist.append({
            "hour_utc": h,
            "trade_count": hourly.get(h, 0),
        })

    # Find most active hour
    if hourly:
        peak_hour = max(hourly, key=hourly.get)
    else:
        peak_hour = None

    return {
        "distribution": dist,
        "peak_hour_utc": peak_hour,
        "peak_hour_count": hourly.get(peak_hour, 0) if peak_hour is not None else 0,
    }


def find_active_periods(daily_stats):
    """Identify most and least active periods."""
    if not daily_stats:
        return {}

    sorted_by_count = sorted(daily_stats, key=lambda d: -d["trade_count"])
    sorted_by_volume = sorted(daily_stats, key=lambda d: -(d["buy_amount"] + d["sell_amount"]))

    return {
        "most_active_day": sorted_by_count[0] if sorted_by_count else None,
        "highest_volume_day": sorted_by_volume[0] if sorted_by_volume else None,
        "total_active_days": len(daily_stats),
        "avg_trades_per_day": round(
            sum(d["trade_count"] for d in daily_stats) / len(daily_stats), 2
        ) if daily_stats else 0,
    }


def compute_holding_period_estimate(trades):
    """
    Estimate average holding period.
    For each market, measure time between first BUY and last SELL/SETTLE.
    """
    market_times = defaultdict(lambda: {"first_buy": None, "last_close": None, "buys": []})

    for t in trades:
        token_id = t.get("_token_id", "")
        if not token_id:
            continue
        side = t.get("side", "")
        ts = t.get("timestamp", "")
        if not ts:
            continue

        mt = market_times[token_id]

        if side == "BUY":
            if mt["first_buy"] is None:
                mt["first_buy"] = ts
            mt["buys"].append(ts)
        elif side in ("SELL", "SETTLE"):
            mt["last_close"] = ts

    holding_periods = []
    for token_id, mt in market_times.items():
        if mt["first_buy"] and mt["last_close"]:
            try:
                import datetime
                first = datetime.datetime.strptime(mt["first_buy"][:19], "%Y-%m-%d %H:%M:%S")
                last = datetime.datetime.strptime(mt["last_close"][:19], "%Y-%m-%d %H:%M:%S")
                delta = (last - first).total_seconds() / 3600  # hours
                holding_periods.append(delta)
            except (ValueError, TypeError):
                pass

    if holding_periods:
        return {
            "avg_holding_hours": round(sum(holding_periods) / len(holding_periods), 2),
            "min_holding_hours": round(min(holding_periods), 2),
            "max_holding_hours": round(max(holding_periods), 2),
            "median_holding_hours": round(sorted(holding_periods)[len(holding_periods) // 2], 2),
            "is_estimated": True,
        }
    return {
        "avg_holding_hours": None,
        "is_estimated": True,
        "note": "Insufficient data to estimate holding periods",
    }
