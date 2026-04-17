"""
Statistics module: overall trading statistics.
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def compute_overall_stats(trades):
    """Compute overall trading statistics from normalized trades."""
    total_trades = 0
    total_buy_count = 0
    total_sell_count = 0
    total_settle_count = 0
    total_buy_amount = 0.0
    total_sell_amount = 0.0
    total_fee = 0.0
    total_realized_pnl = 0.0
    pnl_count = 0

    for t in trades:
        side = t.get("side", "")
        if side in ("BUY", "SELL", "SETTLE"):
            total_trades += 1

        notional = float(t.get("notional", 0) or 0)
        fee = float(t.get("fee", 0) or 0)
        pnl = t.get("realized_pnl", "")

        if side == "BUY":
            total_buy_count += 1
            total_buy_amount += notional
        elif side == "SELL":
            total_sell_count += 1
            total_sell_amount += notional
        elif side == "SETTLE":
            total_settle_count += 1
            total_sell_amount += notional

        total_fee += fee

        if pnl != "" and pnl is not None:
            try:
                total_realized_pnl += float(pnl)
                pnl_count += 1
            except (ValueError, TypeError):
                pass

    # Win rate: trades with positive realized PnL
    win_count = 0
    loss_count = 0
    for t in trades:
        pnl = t.get("realized_pnl", "")
        if pnl != "" and pnl is not None:
            try:
                if float(pnl) > 0:
                    win_count += 1
                elif float(pnl) < 0:
                    loss_count += 1
            except (ValueError, TypeError):
                pass

    total_closed = win_count + loss_count
    win_rate = (win_count / total_closed * 100) if total_closed > 0 else 0.0

    # Active markets
    markets = set()
    for t in trades:
        slug = t.get("market_slug", "") or t.get("market_question", "")
        if slug:
            markets.add(slug)

    return {
        "total_trades": total_trades,
        "total_buy_count": total_buy_count,
        "total_sell_count": total_sell_count,
        "total_settle_count": total_settle_count,
        "total_buy_amount": round(total_buy_amount, 2),
        "total_sell_amount": round(total_sell_amount, 2),
        "total_fee": round(total_fee, 2),
        "total_realized_pnl": round(total_realized_pnl, 2),
        "realized_pnl_trades_count": pnl_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": round(win_rate, 2),
        "unique_markets": len(markets),
        "is_estimated": {
            "total_realized_pnl": pnl_count < total_trades,
            "win_rate": pnl_count < total_trades,
            "total_fee": True,  # fees often not directly available
        },
    }


def estimate_unrealized_pnl(trades, market_prices=None):
    """
    Estimate unrealized PnL based on open positions.
    market_prices: dict of token_id -> current_price (optional)
    """
    positions = defaultdict(float)
    avg_cost = defaultdict(float)
    total_cost = defaultdict(float)

    for t in trades:
        token_id = t.get("_token_id", "")
        side = t.get("side", "")
        size = float(t.get("size", 0) or 0)
        price = float(t.get("price", 0) or 0)

        if not token_id:
            continue

        if side == "BUY" and size > 0:
            old_total = positions[token_id] * avg_cost[token_id]
            positions[token_id] += size
            total_cost[token_id] += size * price
            avg_cost[token_id] = total_cost[token_id] / positions[token_id] if positions[token_id] > 0 else 0
        elif side in ("SELL", "SETTLE") and size > 0:
            positions[token_id] -= size
            total_cost[token_id] -= size * avg_cost[token_id]
            if positions[token_id] <= 0:
                positions[token_id] = 0
                total_cost[token_id] = 0
                avg_cost[token_id] = 0

    # Calculate unrealized PnL
    unrealized = 0.0
    open_positions = {}
    for token_id, pos in positions.items():
        if pos > 0:
            current_price = (market_prices or {}).get(token_id, 0.5)  # default estimate
            upnl = pos * (current_price - avg_cost[token_id])
            unrealized += upnl
            open_positions[token_id] = {
                "size": round(pos, 2),
                "avg_cost": round(avg_cost[token_id], 4),
                "current_price": current_price,
                "unrealized_pnl": round(upnl, 2),
            }

    return {
        "total_unrealized_pnl": round(unrealized, 2),
        "open_position_count": len(open_positions),
        "positions": open_positions,
        "is_estimated": True,
    }
