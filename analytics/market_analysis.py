"""
Market analysis: per-market statistics.
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def compute_market_stats(trades):
    """Compute statistics grouped by market."""
    markets = defaultdict(lambda: {
        "trades": [],
        "buy_count": 0,
        "sell_count": 0,
        "total_buy_amount": 0.0,
        "total_sell_amount": 0.0,
        "realized_pnl": 0.0,
        "pnl_trades": 0,
        "win_count": 0,
        "loss_count": 0,
        "outcomes": set(),
        "prices": [],
    })

    for t in trades:
        market = t.get("market_slug", "") or t.get("market_question", "") or t.get("_token_id", "unknown")
        side = t.get("side", "")
        notional = float(t.get("notional", 0) or 0)
        pnl = t.get("realized_pnl", "")
        outcome = t.get("outcome", "")
        price = float(t.get("price", 0) or 0)

        m = markets[market]
        m["trades"].append(t)

        if side == "BUY":
            m["buy_count"] += 1
            m["total_buy_amount"] += notional
        elif side in ("SELL", "SETTLE"):
            m["sell_count"] += 1
            m["total_sell_amount"] += notional

        if outcome:
            m["outcomes"].add(outcome)
        if price > 0:
            m["prices"].append(price)

        if pnl != "" and pnl is not None:
            try:
                pnl_val = float(pnl)
                m["realized_pnl"] += pnl_val
                m["pnl_trades"] += 1
                if pnl_val > 0:
                    m["win_count"] += 1
                elif pnl_val < 0:
                    m["loss_count"] += 1
            except (ValueError, TypeError):
                pass

    results = []
    for market, data in sorted(markets.items(), key=lambda x: -len(x[1]["trades"])):
        total_closed = data["win_count"] + data["loss_count"]
        win_rate = (data["win_count"] / total_closed * 100) if total_closed > 0 else None
        avg_price = (sum(data["prices"]) / len(data["prices"])) if data["prices"] else None

        has_both_sides = len(data["outcomes"]) > 1

        results.append({
            "market": market,
            "trade_count": len(data["trades"]),
            "buy_count": data["buy_count"],
            "sell_count": data["sell_count"],
            "total_invested": round(data["total_buy_amount"], 2),
            "total_returned": round(data["total_sell_amount"], 2),
            "realized_pnl": round(data["realized_pnl"], 2),
            "win_rate": round(win_rate, 2) if win_rate is not None else None,
            "avg_entry_price": round(avg_price, 4) if avg_price is not None else None,
            "outcomes_traded": sorted(data["outcomes"]),
            "has_dual_side": has_both_sides,
            "is_estimated": {
                "realized_pnl": data["pnl_trades"] < len(data["trades"]),
                "win_rate": total_closed == 0,
            },
        })

    return results


def find_dual_side_markets(market_stats):
    """Find markets where the wallet traded both sides."""
    dual = []
    for m in market_stats:
        if m.get("has_dual_side"):
            dual.append({
                "market": m["market"],
                "outcomes": m["outcomes_traded"],
                "trade_count": m["trade_count"],
            })
    return dual
