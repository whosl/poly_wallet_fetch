"""
Normalizer: converts decoded events into standardized trade records.
Matches ERC1155 transfers with corresponding USDC transfers within the same
transaction to reconstruct BUY/SELL trades.
"""

import logging
from collections import defaultdict

from config import POLYMARKET_CONTRACTS, USDC_DECIMALS

log = logging.getLogger(__name__)

# CSV field order
CSV_FIELDS = [
    "timestamp", "block_number", "tx_hash", "wallet",
    "market_slug", "market_question", "event_slug",
    "outcome", "side", "price", "size", "notional",
    "fee", "realized_pnl", "position_after",
    "order_id", "trade_id", "source",
    "settlement_value", "notes",
]


def timestamp_to_iso(ts):
    """Convert Unix timestamp string to ISO format."""
    if not ts:
        return ""
    try:
        import datetime
        dt = datetime.datetime.utcfromtimestamp(int(ts))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return str(ts)


def match_trades_by_tx(events):
    """
    Group events by transaction hash, then match ERC1155 conditional token
    transfers with USDC transfers to reconstruct trades.
    """
    by_tx = defaultdict(list)
    for event in events:
        tx_hash = event.get("transaction_hash", "")
        if tx_hash:
            by_tx[tx_hash].append(event)

    trades = []
    for tx_hash, tx_events in by_tx.items():
        ct_transfers = []  # conditional token transfers
        usdc_transfers = []  # USDC transfers
        polymarket_txs = []  # direct contract interactions

        for event in tx_events:
            cat = event.get("_category", "")
            if cat == "erc1155_transfer" and event.get("_is_polymarket"):
                ct_transfers.append(event)
            elif cat == "erc20_transfer" and event.get("_is_usdc"):
                usdc_transfers.append(event)
            elif cat == "polymarket_tx":
                polymarket_txs.append(event)

        # If we have conditional token transfers, those are the trades
        if ct_transfers:
            for ct in ct_transfers:
                trade = _normalize_ct_transfer(ct, usdc_transfers, polymarket_txs, tx_events)
                if trade:
                    trades.append(trade)
        # If we have only USDC transfers to/from Polymarket contracts
        elif usdc_transfers:
            for usdc in usdc_transfers:
                trade = _normalize_usdc_transfer(usdc, polymarket_txs)
                if trade:
                    trades.append(trade)
        # If we only have polymarket contract interactions
        elif polymarket_txs:
            for ptx in polymarket_txs:
                trade = _normalize_polymarket_tx(ptx)
                if trade:
                    trades.append(trade)

    # Sort by block number
    trades.sort(key=lambda t: int(t.get("block_number", 0) or 0))
    log.info("Normalized %d trade records", len(trades))
    return trades


def _normalize_ct_transfer(ct, usdc_transfers, pm_txs, all_events):
    """Convert a conditional token transfer into a normalized trade record."""
    wallet = ""
    # Determine side based on direction
    ct_from = ct.get("from", "").lower()
    ct_to = ct.get("to", "").lower()

    # Find the wallet's role
    ct_contract = ct.get("contract", "").lower()
    exchange_addrs = {
        POLYMARKET_CONTRACTS["ctf_exchange"].lower(),
        POLYMARKET_CONTRACTS["neg_risk_ctf_exchange"].lower(),
    }
    proxy_addr = POLYMARKET_CONTRACTS["proxy_wallet"].lower()

    # If tokens are moving FROM exchange TO wallet (or proxy), it's a BUY
    # If tokens are moving FROM wallet (or proxy) TO exchange, it's a SELL
    if ct_from in exchange_addrs:
        side = "BUY"
        wallet = ct_to
    elif ct_to in exchange_addrs:
        side = "SELL"
        wallet = ct_from
    elif ct_from == proxy_addr:
        side = "BUY"
        wallet = ct_to if ct_to != proxy_addr else ct_from
    elif ct_to == proxy_addr:
        side = "SELL"
        wallet = ct_from if ct_from != proxy_addr else ct_to
    else:
        # Direct transfer between non-exchange addresses
        side = "TRANSFER"
        wallet = ct_from

    # Find matching USDC transfer for notional value
    notional = 0.0
    fee = 0.0
    for usdc in usdc_transfers:
        usdc_from = usdc.get("from", "").lower()
        usdc_to = usdc.get("to", "").lower()
        usdc_val = float(usdc.get("value_usdc", 0))

        # For a BUY: USDC goes FROM wallet TO exchange
        # For a SELL: USDC goes FROM exchange TO wallet
        if side == "BUY" and usdc_to in exchange_addrs:
            notional += usdc_val
        elif side == "SELL" and usdc_from in exchange_addrs:
            notional += usdc_val

    # Calculate price and size
    size = float(ct.get("value", 0))
    if size > 0 and notional > 0:
        price = notional / size
    elif size > 0:
        price = 0.0
        notional = 0.0
    else:
        price = 0.0

    # Check if this is a settlement/redemption
    is_settlement = False
    for ptx in pm_txs:
        if ptx.get("_action_type") in ("redeem_positions", "merge_positions"):
            is_settlement = True
            break

    # Get timestamp
    timestamp = ct.get("time_stamp", "")
    if not timestamp:
        # Try to find timestamp from other events in same tx
        for e in all_events:
            ts = e.get("time_stamp", "")
            if ts:
                timestamp = ts
                break

    notes = ""
    if is_settlement:
        notes = "Settlement/redemption event"
        side = "SETTLE"

    trade = {
        "timestamp": timestamp_to_iso(timestamp),
        "block_number": ct.get("block_number", 0),
        "tx_hash": ct.get("transaction_hash", ""),
        "wallet": wallet,
        "market_slug": "",
        "market_question": "",
        "event_slug": "",
        "outcome": _guess_outcome(ct.get("token_id", "")),
        "side": side,
        "price": round(price, 6),
        "size": size,
        "notional": round(notional, 6),
        "fee": round(fee, 6),
        "realized_pnl": "",
        "position_after": "",
        "order_id": "",
        "trade_id": "{}_{}".format(ct.get("transaction_hash", "")[:16], ct.get("log_index", "")),
        "source": "polygonscan_decoded",
        "settlement_value": round(notional, 6) if is_settlement else "",
        "notes": notes,
        "_token_id": ct.get("token_id", ""),
        "_contract": ct.get("contract", ""),
    }

    # Enrich with market data if available
    market = ct.get("_market")
    if market:
        trade["market_slug"] = market.get("slug", "")
        trade["market_question"] = market.get("question", "")
        trade["event_slug"] = market.get("events", [{}])[0].get("slug", "") if market.get("events") else ""

    return trade


def _normalize_usdc_transfer(usdc, pm_txs):
    """Convert a standalone USDC transfer into a normalized record."""
    usdc_from = usdc.get("from", "").lower()
    usdc_to = usdc.get("to", "").lower()
    value = float(usdc.get("value_usdc", 0))

    exchange_addrs = {
        POLYMARKET_CONTRACTS["ctf_exchange"].lower(),
        POLYMARKET_CONTRACTS["neg_risk_ctf_exchange"].lower(),
    }

    if usdc_to in exchange_addrs:
        side = "DEPOSIT"
    elif usdc_from in exchange_addrs:
        side = "WITHDRAW"
    else:
        side = "USDC_TRANSFER"

    return {
        "timestamp": timestamp_to_iso(usdc.get("time_stamp", "")),
        "block_number": usdc.get("block_number", 0),
        "tx_hash": usdc.get("transaction_hash", ""),
        "wallet": usdc_from,
        "market_slug": "",
        "market_question": "",
        "event_slug": "",
        "outcome": "",
        "side": side,
        "price": "",
        "size": "",
        "notional": round(value, 6),
        "fee": 0.0,
        "realized_pnl": "",
        "position_after": "",
        "order_id": "",
        "trade_id": "",
        "source": "polygonscan_erc20",
        "settlement_value": "",
        "notes": "USDC {}".format(side.lower()),
    }


def _normalize_polymarket_tx(ptx):
    """Convert a Polymarket contract interaction into a normalized record."""
    action = ptx.get("_action_type", "unknown")
    details = ptx.get("_details", {})

    notes_map = {
        "split_position": "Split positions (mint conditional tokens)",
        "merge_positions": "Merge positions (redeem conditional tokens)",
        "redeem_positions": "Redeem settled positions",
        "set_approval": "Approval for trading",
        "exchange_interaction": "Exchange interaction",
        "conditional_tokens_interaction": "Conditional tokens interaction",
    }

    return {
        "timestamp": timestamp_to_iso(ptx.get("time_stamp", "")),
        "block_number": ptx.get("block_number", 0),
        "tx_hash": ptx.get("tx_hash", ""),
        "wallet": ptx.get("from", ""),
        "market_slug": "",
        "market_question": "",
        "event_slug": "",
        "outcome": "",
        "side": action.upper() if action != "unknown" else "",
        "price": "",
        "size": "",
        "notional": "",
        "fee": "",
        "realized_pnl": "",
        "position_after": "",
        "order_id": "",
        "trade_id": "",
        "source": "polygonscan_tx",
        "settlement_value": "",
        "notes": notes_map.get(action, action),
    }


def _guess_outcome(token_id):
    """
    Try to guess YES/NO outcome from token ID.
    In Polymarket, even-index tokens are usually YES, odd are NO.
    This is a heuristic - the Gamma API provides definitive mapping.
    """
    if not token_id:
        return ""
    return ""  # We'll rely on Gamma API enrichment instead


def enrich_with_market_data(trades, market_lookup):
    """
    Enrich trades with market metadata from Gamma API.
    market_lookup: dict mapping token_id -> market info
    """
    for trade in trades:
        token_id = trade.get("_token_id", "")
        if token_id and token_id in market_lookup:
            m = market_lookup[token_id]
            trade["market_slug"] = m.get("slug", "")
            trade["market_question"] = m.get("question", "")
            if not trade.get("outcome"):
                # Try to determine outcome from market data
                outcomes = m.get("outcomes", [])
                if isinstance(outcomes, str):
                    import json
                    try:
                        outcomes = json.loads(outcomes)
                    except (ValueError, TypeError):
                        outcomes = []
                if isinstance(outcomes, list):
                    for o in outcomes:
                        if isinstance(o, dict) and o.get("token_id") == token_id:
                            trade["outcome"] = o.get("outcome", "")
                            break
    return trades


def compute_positions(trades, wallet):
    """
    Track running position sizes per market token.
    Adds position_after to each trade.
    """
    positions = defaultdict(float)
    wallet = wallet.lower()

    for trade in trades:
        token_id = trade.get("_token_id", "")
        side = trade.get("side", "")
        size = float(trade.get("size", 0) or 0)

        if not token_id or not size:
            continue

        if side == "BUY":
            positions[token_id] += size
        elif side == "SELL":
            positions[token_id] -= size
        elif side == "SETTLE":
            positions[token_id] -= size

        trade["position_after"] = round(positions.get(token_id, 0), 2)

    return trades


def compute_realized_pnl(trades, wallet):
    """
    Estimate realized PnL per trade using FIFO matching.
    For each SELL, match against earliest unmatched BUYs.
    """
    wallet = wallet.lower()
    # Track unmatched buys per token: list of (size, price)
    unmatched = defaultdict(list)

    for trade in trades:
        token_id = trade.get("_token_id", "")
        side = trade.get("side", "")
        size = float(trade.get("size", 0) or 0)
        price = float(trade.get("price", 0) or 0)

        if not token_id or not size:
            continue

        if side == "BUY":
            unmatched[token_id].append((size, price))
            trade["realized_pnl"] = ""
        elif side in ("SELL", "SETTLE"):
            remaining = size
            pnl = 0.0
            while remaining > 0 and unmatched[token_id]:
                buy_size, buy_price = unmatched[token_id][0]
                matched = min(remaining, buy_size)
                pnl += matched * (price - buy_price)
                remaining -= matched
                if matched >= buy_size:
                    unmatched[token_id].pop(0)
                else:
                    unmatched[token_id][0] = (buy_size - matched, buy_price)
            trade["realized_pnl"] = round(pnl, 6)

    return trades
