"""
CSV output writers for normalized trades, positions, and daily summaries.
"""

import csv
import logging
import os

from normalize.normalizer import CSV_FIELDS

log = logging.getLogger(__name__)


def write_trades_csv(trades, filepath):
    """Write normalized trades to CSV."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    count = 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for trade in trades:
            # Clean up internal fields
            row = {k: trade.get(k, "") for k in CSV_FIELDS}
            writer.writerow(row)
            count += 1
    log.info("Wrote %d trades to %s", count, filepath)
    return count


def write_positions_csv(market_stats, filepath):
    """Write per-market position summary to CSV."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fields = [
        "market", "trade_count", "buy_count", "sell_count",
        "total_invested", "total_returned", "realized_pnl",
        "win_rate", "avg_entry_price", "outcomes_traded", "has_dual_side",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for m in market_stats:
            row = dict(m)
            row["outcomes_traded"] = ", ".join(str(o) for o in row.get("outcomes_traded", []))
            writer.writerow(row)
    log.info("Wrote %d market positions to %s", len(market_stats), filepath)


def write_daily_csv(daily_stats, filepath):
    """Write daily summary to CSV."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fields = [
        "date", "trade_count", "buy_count", "sell_count",
        "buy_amount", "sell_amount", "realized_pnl", "unique_markets",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for d in daily_stats:
            writer.writerow(d)
    log.info("Wrote %d daily records to %s", len(daily_stats), filepath)


def write_raw_json(raw_data, filepath):
    """Write raw fetch results to JSON."""
    import json
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Convert any non-serializable types
    def default_serializer(obj):
        if isinstance(obj, (set, frozenset)):
            return list(obj)
        return str(obj)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2, default=default_serializer, ensure_ascii=False)
    log.info("Wrote raw data to %s", filepath)
