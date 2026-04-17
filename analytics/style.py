"""
Trading style diagnosis.
Classifies the wallet's trading style based on behavioral patterns.
"""

import logging

log = logging.getLogger(__name__)


def diagnose_style(overall_stats, market_stats, time_stats, behavior_stats,
                   price_buckets, dual_side, market_preference, holding_period):
    """
    Diagnose the wallet's trading style.
    Returns a dict with style classification and evidence.
    """
    scores = {
        "high_frequency_market_making": 0.0,
        "directional_speculation": 0.0,
        "dual_side_arbitrage": 0.0,
        "late_high_winrate": 0.0,
        "event_driven": 0.0,
    }
    evidence = {
        "high_frequency_market_making": [],
        "directional_speculation": [],
        "dual_side_arbitrage": [],
        "late_high_winrate": [],
        "event_driven": [],
    }

    total_trades = overall_stats.get("total_trades", 0)
    win_rate = overall_stats.get("win_rate", 0)
    unique_markets = overall_stats.get("unique_markets", 0)

    # --- High frequency / market making ---
    if total_trades > 500:
        scores["high_frequency_market_making"] += 2.0
        evidence["high_frequency_market_making"].append(
            "High trade count: {}".format(total_trades))
    elif total_trades > 100:
        scores["high_frequency_market_making"] += 1.0
        evidence["high_frequency_market_making"].append(
            "Moderate trade count: {}".format(total_trades))

    avg_per_market = total_trades / unique_markets if unique_markets > 0 else 0
    if avg_per_market > 10:
        scores["high_frequency_market_making"] += 1.5
        evidence["high_frequency_market_making"].append(
            "High trades per market: {:.1f}".format(avg_per_market))

    # Check for many sells in same market (market making)
    if dual_side and len(dual_side) > 2:
        scores["dual_side_arbitrage"] += 2.0
        evidence["dual_side_arbitrage"].append(
            "{} markets with dual-side trading".format(len(dual_side)))

    # --- Directional speculation ---
    if avg_per_market <= 5 and total_trades > 20:
        scores["directional_speculation"] += 1.5
        evidence["directional_speculation"].append(
            "Low trades per market: {:.1f}".format(avg_per_market))

    if win_rate > 50:
        scores["directional_speculation"] += 1.0
        evidence["directional_speculation"].append("Win rate above 50%: {:.1f}%".format(win_rate))

    if win_rate > 65:
        scores["late_high_winrate"] += 1.5
        evidence["late_high_winrate"].append("High win rate: {:.1f}%".format(win_rate))

    # --- Late / high win rate ---
    holding = holding_period.get("avg_holding_hours")
    if holding is not None:
        if holding < 24:
            scores["late_high_winrate"] += 1.5
            evidence["late_high_winrate"].append(
                "Short holding period: {:.1f} hours avg".format(holding))
        elif holding < 72:
            scores["directional_speculation"] += 0.5
            evidence["directional_speculation"].append(
                "Medium holding period: {:.1f} hours avg".format(holding))

    # --- Event driven ---
    crypto_pct = market_preference.get("crypto_pct", 0)
    event_pct = market_preference.get("event_pct", 0)
    if event_pct > 30:
        scores["event_driven"] += 2.0
        evidence["event_driven"].append("Event market focus: {:.1f}%".format(event_pct))

    if crypto_pct > 60:
        scores["directional_speculation"] += 1.0
        evidence["directional_speculation"].append(
            "Crypto price market focus: {:.1f}%".format(crypto_pct))

    # --- Price bucket analysis ---
    buckets = price_buckets.get("buckets", [])
    if buckets:
        low_price = sum(b["trade_count"] for b in buckets
                        if b["price_range"].startswith(("0.0", "0.1", "0.2")))
        high_price = sum(b["trade_count"] for b in buckets
                         if b["price_range"].startswith(("0.7", "0.8", "0.9")))
        total_bucketed = sum(b["trade_count"] for b in buckets)

        if total_bucketed > 0:
            if high_price / total_bucketed > 0.5:
                scores["late_high_winrate"] += 1.0
                evidence["late_high_winrate"].append(
                    "Prefers high-price entries: {:.0f}% in 0.7-1.0 range".format(
                        high_price / total_bucketed * 100))
            if low_price / total_bucketed > 0.5:
                scores["directional_speculation"] += 0.5
                evidence["directional_speculation"].append(
                    "Prefers low-price entries: {:.0f}% in 0.0-0.3 range".format(
                        low_price / total_bucketed * 100))

    # --- Averaging behavior ---
    avg_behavior = behavior_stats
    if avg_behavior.get("averaging_down_count", 0) > 3:
        scores["directional_speculation"] += 0.5
        evidence["directional_speculation"].append(
            "Frequent averaging down: {} instances".format(avg_behavior["averaging_down_count"]))

    # Normalize scores
    max_score = max(scores.values()) if scores.values() else 1
    total_score = sum(scores.values())

    # Sort by score
    ranked = sorted(scores.items(), key=lambda x: -x[1])

    # Build style label
    primary_style = ranked[0][0] if ranked else "unknown"
    style_labels = {
        "high_frequency_market_making": "High-frequency / Market Making",
        "directional_speculation": "Directional Speculation",
        "dual_side_arbitrage": "Dual-side Arbitrage",
        "late_high_winrate": "Late / High Win-rate Strategy",
        "event_driven": "Event-driven Trading",
    }

    return {
        "primary_style": style_labels.get(primary_style, primary_style),
        "primary_key": primary_style,
        "scores": {style_labels.get(k, k): round(v, 2) for k, v in ranked},
        "evidence": {style_labels.get(k, k): v for k, v in evidence.items() if v},
        "confidence": round(max_score / total_score * 100, 1) if total_score > 0 else 0,
        "is_estimated": True,
    }


def generate_style_diagnosis(trades, overall_stats, market_stats, time_stats,
                              behavior_stats, price_buckets, dual_side,
                              market_preference, holding_period):
    """Generate a full style diagnosis dict."""
    style = diagnose_style(
        overall_stats, market_stats, time_stats, behavior_stats,
        price_buckets, dual_side, market_preference, holding_period
    )

    return {
        "style": style,
        "market_preference": market_preference,
        "price_buckets": price_buckets,
        "dual_side_markets": dual_side,
        "behavior_patterns": behavior_stats,
        "anomalies": _detect_anomalies(trades, overall_stats),
    }


def _detect_anomalies(trades, stats):
    """Detect any unusual patterns worth flagging."""
    anomalies = []

    # Very large single trades
    for t in trades:
        notional = float(t.get("notional", 0) or 0)
        if notional > 50000:
            anomalies.append({
                "type": "large_trade",
                "tx_hash": t.get("tx_hash", ""),
                "notional": notional,
                "market": t.get("market_question", ""),
                "note": "Single trade > $50,000",
            })

    # Many trades in very short time
    from collections import defaultdict
    tx_counts = defaultdict(int)
    for t in trades:
        tx_counts[t.get("tx_hash", "")] += 1

    for tx_hash, count in tx_counts.items():
        if count > 20:
            anomalies.append({
                "type": "batch_operation",
                "tx_hash": tx_hash,
                "count": count,
                "note": "Transaction with {} decoded events".format(count),
            })

    return anomalies
