"""
Behavioral analysis: micro-trading patterns.
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def analyze_price_buckets(trades, bucket_size=0.1):
    """
    Analyze which price ranges the wallet enters most frequently.
    """
    buckets = defaultdict(lambda: {"count": 0, "total_size": 0.0, "total_notional": 0.0})

    for t in trades:
        price = t.get("price", "")
        if price == "" or price is None:
            continue
        try:
            price = float(price)
        except (ValueError, TypeError):
            continue
        if price <= 0:
            continue

        # Determine bucket
        bucket_idx = int(price / bucket_size)
        bucket_label = "{:.1f}-{:.1f}".format(bucket_idx * bucket_size, (bucket_idx + 1) * bucket_size)

        size = float(t.get("size", 0) or 0)
        notional = float(t.get("notional", 0) or 0)

        b = buckets[bucket_label]
        b["count"] += 1
        b["total_size"] += size
        b["total_notional"] += notional

    results = []
    for label in sorted(buckets.keys()):
        b = buckets[label]
        results.append({
            "price_range": label,
            "trade_count": b["count"],
            "total_size": round(b["total_size"], 2),
            "total_notional": round(b["total_notional"], 2),
        })

    # Find most common entry price range
    if results:
        top = max(results, key=lambda r: r["trade_count"])
    else:
        top = None

    return {
        "buckets": results,
        "most_common_range": top,
    }


def detect_averaging_behavior(trades):
    """
    Detect averaging down / scaling in / scaling out behavior.
    Look for multiple buys at decreasing/increasing prices in the same market.
    """
    market_trades = defaultdict(list)

    for t in trades:
        token_id = t.get("_token_id", "")
        if not token_id:
            continue
        side = t.get("side", "")
        price = t.get("price", "")
        ts = t.get("timestamp", "")

        if price == "" or price is None:
            continue
        try:
            price = float(price)
        except (ValueError, TypeError):
            continue

        market_trades[token_id].append({
            "side": side,
            "price": price,
            "timestamp": ts,
            "size": float(t.get("size", 0) or 0),
        })

    averaging_down = 0  # buys at lower prices
    averaging_up = 0  # buys at higher prices
    scale_out = 0  # multiple sells at different prices
    stop_loss = 0  # sell after price drop

    for token_id, trades_list in market_trades.items():
        # Sort by timestamp
        trades_list.sort(key=lambda x: x.get("timestamp", ""))

        buys = [t for t in trades_list if t["side"] == "BUY"]
        sells = [t for t in trades_list if t["side"] in ("SELL", "SETTLE")]

        # Check for averaging down (consecutive buys at lower prices)
        for i in range(1, len(buys)):
            if buys[i]["price"] < buys[i - 1]["price"]:
                averaging_down += 1
            elif buys[i]["price"] > buys[i - 1]["price"]:
                averaging_up += 1

        # Check for scale-out (multiple sells)
        if len(sells) > 1:
            scale_out += len(sells) - 1

        # Check for stop-loss pattern (buy then sell at lower price)
        if buys and sells:
            last_buy_price = buys[-1]["price"]
            first_sell_price = sells[0]["price"]
            if first_sell_price < last_buy_price:
                stop_loss += 1

    return {
        "averaging_down_count": averaging_down,
        "averaging_up_count": averaging_up,
        "scale_out_count": scale_out,
        "stop_loss_pattern_count": stop_loss,
        "notes": {
            "averaging_down": "Multiple buys at decreasing prices (pyramiding into losing position)",
            "averaging_up": "Multiple buys at increasing prices (adding to winning position)",
            "scale_out": "Multiple sells in same market (partial profit-taking)",
            "stop_loss": "Sold at lower price than last buy (possible stop-loss)",
        },
    }


def detect_dual_side_asymmetry(trades, market_stats):
    """
    Check for asymmetric dual-side bets in the same market.
    E.g., buying YES at low price AND buying NO at low price.
    """
    token_trades = defaultdict(list)

    for t in trades:
        market = t.get("market_slug", "") or t.get("market_question", "")
        if not market:
            continue
        side = t.get("side", "")
        outcome = t.get("outcome", "")
        price = t.get("price", "")
        if price == "" or price is None:
            continue
        try:
            price = float(price)
        except (ValueError, TypeError):
            continue

        token_trades[market].append({
            "side": side,
            "outcome": outcome,
            "price": price,
            "size": float(t.get("size", 0) or 0),
        })

    asymmetric = []
    for market, trades_list in token_trades.items():
        outcomes = defaultdict(list)
        for t in trades_list:
            if t["outcome"]:
                outcomes[t["outcome"]].append(t)

        if len(outcomes) > 1:
            # Has dual-side trades
            outcome_summary = {}
            for outcome, o_trades in outcomes.items():
                buy_trades = [t for t in o_trades if t["side"] == "BUY"]
                if buy_trades:
                    outcome_summary[outcome] = {
                        "count": len(buy_trades),
                        "avg_price": round(sum(t["price"] for t in buy_trades) / len(buy_trades), 4),
                        "total_size": round(sum(t["size"] for t in buy_trades), 2),
                    }

            if len(outcome_summary) > 1:
                asymmetric.append({
                    "market": market,
                    "outcomes": outcome_summary,
                })

    return asymmetric


def analyze_market_preference(trades):
    """Analyze what types of markets the wallet prefers."""
    short_cycle = 0  # BTC/ETH up/down type
    event_type = 0  # event-driven
    other = 0

    for t in trades:
        question = (t.get("market_question", "") + " " + t.get("market_slug", "")).lower()
        if any(k in question for k in ["btc", "bitcoin", "eth", "ethereum", "up/down", "above", "below", "price"]):
            short_cycle += 1
        elif any(k in question for k in ["election", "win", "will ", "president", "senate"]):
            event_type += 1
        else:
            other += 1

    total = short_cycle + event_type + other
    return {
        "crypto_price_markets": short_cycle,
        "event_markets": event_type,
        "other_markets": other,
        "total": total,
        "crypto_pct": round(short_cycle / total * 100, 1) if total > 0 else 0,
        "event_pct": round(event_type / total * 100, 1) if total > 0 else 0,
    }
