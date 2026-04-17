"""
Report generator: creates report.md and wallet_style_diagnosis.md.
"""

import logging
import os
import datetime

log = logging.getLogger(__name__)


def _fmt(val, prefix="", suffix=""):
    """Format a value for report output."""
    if val is None or val == "":
        return "N/A"
    try:
        return "{}{}{}".format(prefix, str(val), suffix)
    except Exception:
        return str(val)


def _pct(val):
    """Format as percentage."""
    if val is None:
        return "N/A"
    return "{:.1f}%".format(val)


def _usd(val):
    """Format as USD."""
    if val is None:
        return "N/A"
    return "${:,.2f}".format(val)


def _is_estimated(note_dict, key):
    """Check if a metric is estimated."""
    if isinstance(note_dict, dict):
        return note_dict.get(key, False)
    return False


def generate_report(wallet, overall_stats, market_stats, daily_stats,
                    hourly_dist, active_periods, holding_period, price_buckets,
                    behavior_stats, dual_side, market_pref, style_result,
                    unrealized, output_dir):
    """Generate the main analysis report as report.md."""
    filepath = os.path.join(output_dir, "report.md")

    lines = []
    w = lines.append

    w("# Polymarket Wallet Analysis Report")
    w("")
    w("**Wallet:** `{}`".format(wallet))
    w("**Generated:** {}".format(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")))
    w("**Data sources:** Polygonscan API, Gamma API, on-chain log decoding")
    w("")

    # --- 1. Overall Statistics ---
    w("## 1. Overall Statistics")
    w("")
    ie = overall_stats.get("is_estimated", {})
    w("| Metric | Value | Type |")
    w("|--------|-------|------|")
    w("| Total trades | {} | Determined |".format(overall_stats.get("total_trades", 0)))
    w("| Buy trades | {} | Determined |".format(overall_stats.get("total_buy_count", 0)))
    w("| Sell trades | {} | Determined |".format(overall_stats.get("total_sell_count", 0)))
    w("| Settlement trades | {} | Determined |".format(overall_stats.get("total_settle_count", 0)))
    w("| Unique markets | {} | Determined |".format(overall_stats.get("unique_markets", 0)))
    w("| Total buy amount | {} | Determined |".format(_usd(overall_stats.get("total_buy_amount"))))
    w("| Total sell amount | {} | Determined |".format(_usd(overall_stats.get("total_sell_amount"))))
    w("| Total fees | {} | {} |".format(
        _usd(overall_stats.get("total_fee")),
        "Estimated" if ie.get("total_fee") else "Determined"))
    w("| Realized PnL | {} | {} |".format(
        _usd(overall_stats.get("total_realized_pnl")),
        "Estimated" if ie.get("total_realized_pnl") else "Determined"))
    w("| Win rate | {} | {} |".format(
        _pct(overall_stats.get("win_rate")),
        "Estimated" if ie.get("win_rate") else "Determined"))
    w("| Unrealized PnL (est.) | {} | Estimated |".format(
        _usd(unrealized.get("total_unrealized_pnl"))))
    w("")

    hp = holding_period or {}
    if hp.get("avg_holding_hours") is not None:
        w("| Avg holding period | {:.1f} hours | Estimated |".format(hp["avg_holding_hours"]))
        w("| Median holding period | {:.1f} hours | Estimated |".format(hp.get("median_holding_hours", 0)))
        w("| Min holding period | {:.1f} hours | Estimated |".format(hp.get("min_holding_hours", 0)))
        w("| Max holding period | {:.1f} hours | Estimated |".format(hp.get("max_holding_hours", 0)))
    else:
        w("| Avg holding period | Insufficient data | - |")
    w("")

    # --- 2. Per-Market Statistics ---
    w("## 2. Per-Market Statistics")
    w("")
    w("Top 20 markets by trade count:")
    w("")
    w("| Market | Trades | Invested | Returned | PnL | Win Rate | Dual-side? |")
    w("|--------|--------|----------|----------|-----|----------|------------|")

    for m in market_stats[:20]:
        market_name = m.get("market", "Unknown")[:60]
        w("| {} | {} | {} | {} | {} | {} | {} |".format(
            market_name,
            m.get("trade_count", 0),
            _usd(m.get("total_invested")),
            _usd(m.get("total_returned")),
            _usd(m.get("realized_pnl")),
            _pct(m.get("win_rate")),
            "Yes" if m.get("has_dual_side") else "No",
        ))
    w("")

    if dual_side:
        w("### Markets with Dual-Side Trading")
        w("")
        for d in dual_side[:10]:
            w("- **{}**: outcomes traded: {}".format(
                d.get("market", "Unknown")[:60],
                ", ".join(str(o) for o in d.get("outcomes", [])),
            ))
        w("")

    # --- 3. Time Statistics ---
    w("## 3. Time-Based Statistics")
    w("")

    ap = active_periods or {}
    if ap:
        w("- **Total active days:** {}".format(ap.get("total_active_days", 0)))
        w("- **Avg trades per active day:** {}".format(ap.get("avg_trades_per_day", 0)))
        if ap.get("most_active_day"):
            mad = ap["most_active_day"]
            w("- **Most active day:** {} ({} trades)".format(mad.get("date", ""), mad.get("trade_count", 0)))
        if ap.get("highest_volume_day"):
            hvd = ap["highest_volume_day"]
            w("- **Highest volume day:** {} (${} traded)".format(hvd.get("date", ""), hvd.get("buy_amount", 0) + hvd.get("sell_amount", 0)))
    w("")

    w("### Hourly Distribution (UTC)")
    w("")
    if hourly_dist:
        dist = hourly_dist.get("distribution", [])
        w("| Hour | Trades | Bar |")
        w("|------|--------|-----|")
        max_count = max((d["trade_count"] for d in dist), default=1) or 1
        for d in dist:
            bar = "#" * int(d["trade_count"] / max_count * 30)
            w("| {:02d}:00 | {} | {} |".format(d["hour_utc"], d["trade_count"], bar))
        if hourly_dist.get("peak_hour_utc") is not None:
            w("")
            w("**Peak hour (UTC):** {:02d}:00 ({} trades)".format(
                hourly_dist["peak_hour_utc"], hourly_dist.get("peak_hour_count", 0)))
    w("")

    # --- 4. Behavioral Analysis ---
    w("## 4. Micro Trading Behavior Analysis")
    w("")

    # Price buckets
    pb = price_buckets or {}
    if pb.get("buckets"):
        w("### Entry Price Distribution")
        w("")
        w("| Price Range | Trade Count | Total Size | Total Notional |")
        w("|-------------|-------------|------------|----------------|")
        for b in pb["buckets"]:
            w("| {} | {} | {} | {} |".format(
                b["price_range"], b["trade_count"],
                _fmt(b["total_size"]), _usd(b["total_notional"])))
        if pb.get("most_common_range"):
            w("")
            w("**Most common entry range:** {}".format(pb["most_common_range"]["price_range"]))
        w("")

    # Averaging behavior
    if behavior_stats:
        w("### Trading Patterns")
        w("")
        w("- Averaging down: {} instances".format(behavior_stats.get("averaging_down_count", 0)))
        w("- Averaging up: {} instances".format(behavior_stats.get("averaging_up_count", 0)))
        w("- Scale-out (partial sells): {} instances".format(behavior_stats.get("scale_out_count", 0)))
        w("- Stop-loss patterns: {} instances".format(behavior_stats.get("stop_loss_pattern_count", 0)))
        w("")

    # Market preference
    mp = market_pref or {}
    if mp:
        w("### Market Type Preference")
        w("")
        w("- Crypto price markets: {} trades ({})".format(
            mp.get("crypto_price_markets", 0), _pct(mp.get("crypto_pct"))))
        w("- Event markets: {} trades ({})".format(
            mp.get("event_markets", 0), _pct(mp.get("event_pct"))))
        w("- Other markets: {} trades".format(mp.get("other_markets", 0)))
        w("")

    # --- 5. Style Diagnosis ---
    w("## 5. Trading Style Diagnosis")
    w("")
    if style_result:
        style = style_result.get("style", {})
        w("### Primary Style: **{}**".format(style.get("primary_style", "Unknown")))
        w("- Confidence: {}%".format(style.get("confidence", 0)))
        w("")

        w("### Score Breakdown")
        w("")
        w("| Style | Score |")
        w("|-------|-------|")
        for style_name, score in style.get("scores", {}).items():
            w("| {} | {:.1f} |".format(style_name, score))
        w("")

        w("### Evidence")
        w("")
        for style_name, evidence_list in style.get("evidence", {}).items():
            if evidence_list:
                w("**{}:**".format(style_name))
                for e in evidence_list:
                    w("- {}".format(e))
                w("")

    # --- Anomalies ---
    if style_result and style_result.get("anomalies"):
        w("## 6. Notable Anomalies")
        w("")
        for a in style_result["anomalies"][:10]:
            w("- **{}**: {} (tx: `{}`)".format(
                a.get("type", "Unknown"),
                a.get("note", ""),
                a.get("tx_hash", "")[:16] + "..."
            ))
        w("")

    # --- Notes ---
    w("## Notes on Data Quality")
    w("")
    w("- Metrics marked as **Determined** are directly computed from on-chain data.")
    w("- Metrics marked as **Estimated** involve assumptions (e.g., FIFO PnL matching, price estimation).")
    w("- Fee data may be incomplete as on-chain logs don't always expose fee amounts directly.")
    w("- Some market metadata may be missing for delisted or very old markets.")
    w("- Transaction logs are decoded heuristically; rare or unknown event types may be missed.")
    w("")

    content = "\n".join(lines)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    log.info("Generated report at %s", filepath)
    return filepath


def generate_style_diagnosis(wallet, diagnosis, output_dir):
    """Generate wallet_style_diagnosis.md."""
    filepath = os.path.join(output_dir, "wallet_style_diagnosis.md")

    lines = []
    w = lines.append

    w("# Wallet Style Diagnosis")
    w("")
    w("**Wallet:** `{}`".format(wallet))
    w("")

    style = diagnosis.get("style", {})
    style_info = style

    # Q1: What markets?
    w("## 1. What markets does this wallet primarily trade?")
    w("")
    mp = diagnosis.get("market_preference", {})
    if mp:
        if mp.get("crypto_pct", 0) > 50:
            w("This wallet primarily trades **crypto price markets** (BTC/ETH up/down, above/below).")
            w("Crypto markets account for {:.1f}% of all trades.".format(mp["crypto_pct"]))
        elif mp.get("event_pct", 0) > 50:
            w("This wallet primarily trades **event-driven markets** (elections, outcomes, etc.).")
            w("Event markets account for {:.1f}% of all trades.".format(mp["event_pct"]))
        else:
            w("This wallet trades a **diverse set of markets** across categories.")
            w("- Crypto: {:.1f}%".format(mp.get("crypto_pct", 0)))
            w("- Events: {:.1f}%".format(mp.get("event_pct", 0)))
            w("- Other: {:.1f}%".format(100 - mp.get("crypto_pct", 0) - mp.get("event_pct", 0)))
    else:
        w("Insufficient data to determine market preference.")
    w("")

    # Q2: BTC/ETH short cycle preference?
    w("## 2. Does it prefer BTC/ETH short-cycle Up/Down markets?")
    w("")
    if mp and mp.get("crypto_pct", 0) > 30:
        w("**Yes.** Crypto price-related markets represent {:.1f}% of trades.".format(mp["crypto_pct"]))
    else:
        w("**No strong preference** for crypto short-cycle markets.")
    w("")

    # Q3: Dual-side betting?
    w("## 3. Does it frequently place dual-side bets?")
    w("")
    dual = diagnosis.get("dual_side_markets", [])
    if dual and len(dual) > 0:
        w("**Yes.** Found {} markets with dual-side trading:".format(len(dual)))
        for d in dual[:5]:
            outcomes_str = ", ".join(str(o) for o in d.get("outcomes", {}).keys())
            w("- {}: {} ({})".format(
                d.get("market", "Unknown")[:50],
                outcomes_str,
                d.get("trade_count", 0)
            ))
    else:
        w("**No.** No significant dual-side betting detected.")
    w("")

    # Q4: Common price bands?
    w("## 4. What are the common entry price bands?")
    w("")
    pb = diagnosis.get("price_buckets", {})
    buckets = pb.get("buckets", [])
    if buckets:
        sorted_buckets = sorted(buckets, key=lambda b: -b["trade_count"])
        w("Top 5 most common entry price ranges:")
        for b in sorted_buckets[:5]:
            w("- **{}**: {} trades (notional: {})".format(
                b["price_range"], b["trade_count"], _usd(b["total_notional"])))
    else:
        w("Insufficient price data.")
    w("")

    # Q5: Strategy classification
    w("## 5. Strategy Classification")
    w("")
    if style_info:
        w("### **{}**".format(style_info.get("primary_style", "Unknown")))
        w("Confidence: {}%".format(style_info.get("confidence", 0)))
        w("")

        w("Score breakdown:")
        for style_name, score in style_info.get("scores", {}).items():
            bar = "#" * int(score * 5) if score > 0 else ""
            w("- {}: {:.1f} {}".format(style_name, score, bar))
        w("")

        w("Key evidence:")
        for style_name, evidence_list in style_info.get("evidence", {}).items():
            if evidence_list:
                for e in evidence_list[:3]:
                    w("- [{}] {}".format(style_name, e))
    w("")

    # Q6: Anomalies
    w("## 6. Notable Anomaly Patterns")
    w("")
    anomalies = diagnosis.get("anomalies", [])
    if anomalies:
        for a in anomalies[:10]:
            w("- **{}**: {} (tx: `{}`)".format(
                a.get("type", "Unknown"),
                a.get("note", ""),
                a.get("tx_hash", "")[:20] + "..."
            ))
    else:
        w("No significant anomalies detected.")
    w("")

    w("---")
    w("*This diagnosis is based on on-chain data analysis and should be considered estimated.*")
    w("")

    content = "\n".join(lines)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    log.info("Generated style diagnosis at %s", filepath)
    return filepath
