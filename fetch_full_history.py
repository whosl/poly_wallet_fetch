#!/usr/bin/env python3
"""
Full trade history fetcher for Polymarket wallets.

Uses Polygon RPC to scan ALL USDC transfers (bypassing the Data API's
4000-trade cap), then fetches receipts to decode every trade event.
Optionally merges with Data API for recent metadata enrichment.

Usage:
    python fetch_full_history.py --wallet 0x...
    python fetch_full_history.py --wallet 0x... --no-api
    python fetch_full_history.py --wallet 0x... --skip-rpc --raw-file data/trades_raw.json
    python fetch_full_history.py --wallet 0x... --resume data/trades_raw_partial.json
"""

import argparse
from collections import defaultdict
import datetime
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DEFAULT_WALLET, OUTPUT_DIR, DATA_API_PAGE_SIZE
from fetchers.rpc_scanner import (
    get_working_rpc,
    get_latest_block,
    get_block_timestamp,
    find_activity_range,
    scan_all_usdc_contracts,
    scan_all_usdc_contracts_backward,
    fetch_receipts,
    ERC20_TRANSFER_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    decode_usdc_value,
    extract_address_from_topic,
)
from decoders.erc1155 import decode_transfer_single, is_polymarket_conditional_token
from decoders.erc20 import decode_transfer_log, is_usdc
from config import POLYMARKET_CONTRACTS


def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger("fetch_full")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch full Polymarket trade history via RPC + Data API"
    )
    parser.add_argument("--wallet", "-w", default=DEFAULT_WALLET, help="Target wallet")
    parser.add_argument("--output-dir", "-o", default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--rpc-url", default="", help="Custom Polygon RPC URL")
    parser.add_argument("--no-api", action="store_true", help="Skip Data API enrichment")
    parser.add_argument("--activity-api", action="store_true", help="Use Data API activity endpoint with timestamp backfill")
    parser.add_argument("--skip-rpc", action="store_true", help="Skip RPC, only Data API (capped ~4000)")
    parser.add_argument("--no-gamma", action="store_true", help="Skip Gamma API enrichment")
    parser.add_argument("--resume", default="", help="Resume from previous raw JSON file")
    parser.add_argument("--raw-file", default="", help="Use existing raw JSON, skip fetch")
    parser.add_argument("--target-txs", type=int, default=0, help="Backward-scan until this many unique wallet txs are collected")
    parser.add_argument("--max-blocks", type=int, default=0, help="Maximum blocks to scan backward when --target-txs is set")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# RPC phase: scan on-chain USDC transfers -> decode trades
# ---------------------------------------------------------------------------

def fetch_via_rpc(wallet, rpc_url, resume_data=None, target_txs=0, max_blocks=0):
    """Full RPC-based fetch: find range, scan, decode."""
    log = logging.getLogger("fetch_full")
    url = get_working_rpc(rpc_url)
    if not url:
        log.error("No working RPC found")
        return None

    log.info("Using RPC: %s", url)
    wallet = wallet.lower()

    # If resuming, reuse known block range and tx hashes
    if resume_data and resume_data.get("_rpc_meta") and not target_txs:
        meta = resume_data["_rpc_meta"]
        earliest = meta.get("earliest_block", 0)
        latest = meta.get("latest_block", 0)
        known_txs = set(meta.get("tx_hashes", []))
        log.info("Resuming: blocks %d-%d, %d known txs", earliest, latest, len(known_txs))
    else:
        earliest, latest = find_activity_range(url, wallet)
        known_txs = set()

    if not latest:
        latest = get_latest_block(url)

    if target_txs:
        log.info("Backward scanning for up to %d unique wallet txs from block %d", target_txs, latest)
        tx_to_logs, earliest = scan_all_usdc_contracts_backward(
            url, wallet, latest, target_txs=target_txs, max_blocks=max_blocks
        )
        known_txs = set()
    elif not earliest:
        log.warning("No on-chain activity found")
        return {"trades": [], "_source": "rpc", "_rpc_meta": {}}
    else:
        # Scan for new USDC transfers
        log.info("Scanning USDC transfers, blocks %d to %d ...", earliest, latest)
        tx_to_logs = scan_all_usdc_contracts(url, wallet, earliest, latest)

    new_txs = set(tx_to_logs.keys()) - known_txs
    all_txs = known_txs | new_txs
    log.info("Total unique tx hashes: %d (%d new)", len(all_txs), len(new_txs))

    # Fetch receipts for new txs only
    if new_txs:
        log.info("Fetching %d new receipts...", len(new_txs))
        receipts = fetch_receipts(url, sorted(new_txs))
    else:
        receipts = {}

    # Decode receipts into trades
    log.info("Decoding receipts...")
    trades = decode_receipts_to_trades(receipts, wallet)

    # Merge with existing trades from resume
    if resume_data and resume_data.get("trades"):
        existing_keys = set(
            (t.get("tx_hash", ""), t.get("_token_id", ""), t.get("side", ""))
            for t in resume_data["trades"]
        )
        for t in trades:
            key = (t.get("tx_hash", ""), t.get("_token_id", ""), t.get("side", ""))
            if key not in existing_keys:
                resume_data["trades"].append(t)
        trades = resume_data["trades"]

    return {
        "trades": trades,
        "_source": "rpc",
        "_rpc_meta": {
            "earliest_block": earliest,
            "latest_block": latest,
            "tx_hashes": sorted(all_txs),
        },
    }


def decode_receipts_to_trades(receipts, wallet):
    """Decode all receipts into normalized trade records."""
    log = logging.getLogger("fetch_full")
    wallet = wallet.lower()
    contracts = POLYMARKET_CONTRACTS
    exchange_addrs = {
        contracts["ctf_exchange"].lower(),
        contracts["neg_risk_ctf_exchange"].lower(),
    }

    trades = []
    for tx_hash, receipt in receipts.items():
        block_num = int(receipt.get("blockNumber", "0x0"), 16)
        logs = receipt.get("logs", [])

        # Collect all events from this receipt
        usdc_transfers = []
        ct_transfers = []

        for log_entry in logs:
            address = log_entry.get("address", "").lower()
            topics = log_entry.get("topics", [])
            if not topics:
                continue

            topic0 = topics[0].lower()

            # ERC20 Transfer (USDC)
            if topic0 == ERC20_TRANSFER_TOPIC:
                decoded = decode_transfer_log(log_entry)
                if decoded and is_usdc(decoded["contract"], contracts):
                    usdc_transfers.append(decoded)

            # ERC1155 TransferSingle
            elif topic0 == TRANSFER_SINGLE_TOPIC:
                decoded = decode_transfer_single(log_entry)
                if decoded and is_polymarket_conditional_token(decoded["contract"], contracts):
                    ct_transfers.append(decoded)

        # Build trades from this wallet's conditional-token movements only.
        if ct_transfers:
            for trade in _build_wallet_trades_from_receipt(
                ct_transfers, usdc_transfers, wallet, tx_hash, block_num, exchange_addrs
            ):
                trades.append(trade)
        elif usdc_transfers:
            for usdc in usdc_transfers:
                trade = _build_trade_from_usdc(usdc, wallet, tx_hash, block_num, exchange_addrs)
                if trade:
                    trades.append(trade)

    # Sort by block number
    trades.sort(key=lambda t: int(t.get("block_number", 0) or 0))
    log.info("Decoded %d trades from %d receipts", len(trades), len(receipts))
    return trades


def _token_share_value(raw_value):
    return float(raw_value or 0) / (10 ** 6)


def _build_wallet_trades_from_receipt(ct_transfers, usdc_transfers, wallet, tx_hash, block_num, exchange_addrs):
    grouped = defaultdict(lambda: {"size": 0.0, "parts": []})

    for ct in ct_transfers:
        ct_from = ct.get("from", "").lower()
        ct_to = ct.get("to", "").lower()

        if ct_to == wallet and ct_from != wallet:
            side = "BUY"
        elif ct_from == wallet and ct_to != wallet:
            side = "SELL"
        else:
            continue

        token_id = ct.get("token_id", "")
        key = (side, token_id)
        grouped[key]["size"] += _token_share_value(ct.get("value", 0))
        grouped[key]["parts"].append(ct)

    if not grouped:
        return []

    usdc_out = sum(
        float(u.get("value_usdc", 0))
        for u in usdc_transfers
        if u.get("from", "").lower() == wallet and u.get("to", "").lower() in exchange_addrs
    )
    usdc_in = sum(
        float(u.get("value_usdc", 0))
        for u in usdc_transfers
        if u.get("to", "").lower() == wallet and u.get("from", "").lower() in exchange_addrs
    )

    side_totals = defaultdict(float)
    for side, _token_id in grouped:
        side_totals[side] += grouped[(side, _token_id)]["size"]

    trades = []
    for (side, token_id), group in grouped.items():
        size = group["size"]
        notional_total = usdc_out if side == "BUY" else usdc_in
        if side_totals[side] and notional_total:
            notional = notional_total * (size / side_totals[side])
        else:
            notional = 0.0
        price = round(notional / size, 6) if size and notional else 0.0
        first = min(group["parts"], key=lambda p: p.get("log_index", 0))

        trades.append({
            "timestamp": "",  # filled later from block timestamp
            "block_number": block_num,
            "tx_hash": tx_hash,
            "wallet": wallet,
            "market_slug": "",
            "market_question": "",
            "event_slug": "",
            "outcome": "",
            "side": side,
            "price": price,
            "size": round(size, 6),
            "notional": round(notional, 6),
            "fee": "",
            "realized_pnl": "",
            "position_after": "",
            "order_id": "",
            "trade_id": "{}_{}_{}".format(tx_hash[:16], side.lower(), first.get("log_index", "")),
            "source": "rpc",
            "settlement_value": "",
            "notes": "",
            "_token_id": token_id,
            "_condition_id": "",
        })

    return trades


def _build_trade_from_ct(ct, usdc_transfers, wallet, tx_hash, block_num, exchange_addrs):
    ct_from = ct.get("from", "").lower()
    ct_to = ct.get("to", "").lower()
    ct_contract = ct.get("contract", "").lower()

    if ct_from in exchange_addrs:
        side = "BUY"
    elif ct_to in exchange_addrs:
        side = "SELL"
    else:
        side = "TRANSFER"

    size = _token_share_value(ct.get("value", 0))

    # Match USDC notional
    notional = 0.0
    for usdc in usdc_transfers:
        usdc_from = usdc.get("from", "").lower()
        usdc_to = usdc.get("to", "").lower()
        usdc_val = float(usdc.get("value_usdc", 0))
        if side == "BUY" and usdc_to in exchange_addrs:
            notional += usdc_val
        elif side == "SELL" and usdc_from in exchange_addrs:
            notional += usdc_val

    price = round(notional / size, 6) if size > 0 and notional > 0 else 0.0

    return {
        "timestamp": "",  # filled later from block timestamp
        "block_number": block_num,
        "tx_hash": tx_hash,
        "wallet": wallet,
        "market_slug": "",
        "market_question": "",
        "event_slug": "",
        "outcome": "",
        "side": side,
        "price": price,
        "size": size,
        "notional": round(notional, 6),
        "fee": "",
        "realized_pnl": "",
        "position_after": "",
        "order_id": "",
        "trade_id": "{}_{}".format(tx_hash[:16], ct.get("log_index", "")),
        "source": "rpc",
        "settlement_value": "",
        "notes": "",
        "_token_id": ct.get("token_id", ""),
        "_condition_id": "",
    }


def _build_trade_from_usdc(usdc, wallet, tx_hash, block_num, exchange_addrs):
    usdc_from = usdc.get("from", "").lower()
    usdc_to = usdc.get("to", "").lower()
    value = float(usdc.get("value_usdc", 0))

    if usdc_to in exchange_addrs:
        side = "DEPOSIT"
    elif usdc_from in exchange_addrs:
        side = "WITHDRAW"
    elif usdc_from == wallet:
        side = "USDC_OUT"
    else:
        side = "USDC_IN"

    return {
        "timestamp": "",
        "block_number": block_num,
        "tx_hash": tx_hash,
        "wallet": wallet,
        "market_slug": "",
        "market_question": "",
        "event_slug": "",
        "outcome": "",
        "side": side,
        "price": "",
        "size": "",
        "notional": round(value, 6),
        "fee": "",
        "realized_pnl": "",
        "position_after": "",
        "order_id": "",
        "trade_id": "",
        "source": "rpc_usdc",
        "settlement_value": "",
        "notes": "USDC {}".format(side.lower()),
    }


# ---------------------------------------------------------------------------
# Data API phase: fetch recent trades with rich metadata
# ---------------------------------------------------------------------------

def fetch_via_data_api(wallet):
    """Fetch trades from Data API (capped at ~4000, but has metadata)."""
    import requests

    log = logging.getLogger("fetch_full")
    log.info("Fetching from Data API...")
    base = "https://data-api.polymarket.com"
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json",
    })

    all_trades = []
    offset = 0
    page = DATA_API_PAGE_SIZE

    while True:
        params = {"user": wallet, "limit": page, "offset": offset, "takerOnly": "false"}
        try:
            resp = session.get(base + "/trades", params=params, timeout=30)
            resp.raise_for_status()
            trades = resp.json()
        except Exception as e:
            log.warning("Data API error at offset %d: %s", offset, e)
            break

        if not isinstance(trades, list) or not trades:
            break

        all_trades.extend(trades)
        log.info("Data API: fetched %d (total: %d, offset: %d)", len(trades), len(all_trades), offset)

        if len(trades) < page:
            break
        offset += page
        time.sleep(0.3)
        if offset >= 3001:
            log.info("Data API: hit offset cap")
            break

    log.info("Data API total: %d trades", len(all_trades))

    # Normalize Data API trades
    normalized = []
    for i, rt in enumerate(all_trades):
        timestamp = rt.get("timestamp", "")
        if timestamp:
            try:
                dt = datetime.datetime.utcfromtimestamp(int(timestamp))
                timestamp_iso = dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError, OSError):
                timestamp_iso = str(timestamp)
        else:
            timestamp_iso = ""

        side = rt.get("side", "").upper()
        price = float(rt.get("price", 0) or 0)
        size = float(rt.get("size", 0) or 0)

        normalized.append({
            "timestamp": timestamp_iso,
            "block_number": "",
            "tx_hash": rt.get("transactionHash", ""),
            "wallet": wallet,
            "market_slug": rt.get("slug", ""),
            "market_question": rt.get("title", ""),
            "event_slug": rt.get("eventSlug", ""),
            "outcome": rt.get("outcome", ""),
            "side": side,
            "price": round(price, 6),
            "size": size,
            "notional": round(price * size, 6),
            "fee": "",
            "realized_pnl": "",
            "position_after": "",
            "order_id": "",
            "trade_id": "api_{}".format(i),
            "source": "data-api",
            "settlement_value": "",
            "notes": "",
            "_token_id": rt.get("asset", ""),
            "_condition_id": rt.get("conditionId", ""),
        })

    return normalized


def fetch_via_activity_api(wallet, target_records=0):
    """Fetch trade activity with timestamp paging, bypassing the offset cap."""
    import requests

    log = logging.getLogger("fetch_full")
    log.info("Fetching from Data API activity endpoint...")
    base = "https://data-api.polymarket.com"
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json",
    })

    seen = set()
    raw_rows = []
    page = 1000

    for side in ("BUY", "SELL"):
        end_ts = None
        side_total = 0

        while True:
            params = {
                "user": wallet,
                "limit": page,
                "type": "TRADE",
                "side": side,
                "sortDirection": "DESC",
            }
            if end_ts is not None:
                params["end"] = end_ts

            try:
                resp = session.get(base + "/activity", params=params, timeout=30)
                resp.raise_for_status()
                rows = resp.json()
            except Exception as e:
                log.warning("Activity API error for %s at end=%s: %s", side, end_ts, e)
                break

            if not isinstance(rows, list) or not rows:
                break

            added = 0
            min_ts = None
            for row in rows:
                if row.get("type") != "TRADE":
                    continue
                key = (
                    row.get("transactionHash", ""),
                    row.get("asset", ""),
                    row.get("side", ""),
                    row.get("size", ""),
                    row.get("timestamp", ""),
                )
                if key in seen:
                    continue
                seen.add(key)
                raw_rows.append(row)
                added += 1
                side_total += 1
                ts = row.get("timestamp")
                if ts is not None:
                    min_ts = int(ts) if min_ts is None else min(min_ts, int(ts))

            log.info("Activity API %s: fetched %d rows, added %d (side total: %d, all total: %d, end: %s)",
                     side, len(rows), added, side_total, len(raw_rows), end_ts)

            if target_records and side_total >= target_records:
                break
            if len(rows) < page or min_ts is None:
                break

            end_ts = min_ts - 1
            time.sleep(0.3)

    normalized = []
    for i, row in enumerate(raw_rows):
        timestamp = row.get("timestamp", "")
        if timestamp:
            try:
                dt = datetime.datetime.utcfromtimestamp(int(timestamp))
                timestamp_iso = dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError, OSError):
                timestamp_iso = str(timestamp)
        else:
            timestamp_iso = ""

        price = float(row.get("price", 0) or 0)
        size = float(row.get("size", 0) or 0)
        notional = float(row.get("usdcSize", 0) or 0) or price * size

        normalized.append({
            "timestamp": timestamp_iso,
            "block_number": "",
            "tx_hash": row.get("transactionHash", ""),
            "wallet": wallet,
            "market_slug": row.get("slug", ""),
            "market_question": row.get("title", ""),
            "event_slug": row.get("eventSlug", ""),
            "outcome": row.get("outcome", ""),
            "side": row.get("side", "").upper(),
            "price": round(price, 6),
            "size": size,
            "notional": round(notional, 6),
            "fee": "",
            "realized_pnl": "",
            "position_after": "",
            "order_id": "",
            "trade_id": "activity_{}".format(i),
            "source": "activity-api",
            "settlement_value": "",
            "notes": "",
            "_token_id": row.get("asset", ""),
            "_condition_id": row.get("conditionId", ""),
        })

    log.info("Activity API total: %d trade records", len(normalized))
    return normalized, raw_rows


# ---------------------------------------------------------------------------
# Merge: combine RPC + Data API, dedup by tx_hash
# ---------------------------------------------------------------------------

def merge_trades(rpc_trades, api_trades):
    """Merge RPC and Data API trades, using API data as metadata source."""
    log = logging.getLogger("fetch_full")

    # Build lookup: tx_hash -> API trade (for metadata)
    api_by_tx = {}
    for t in api_trades:
        tx = t.get("tx_hash", "")
        if tx:
            if tx not in api_by_tx:
                api_by_tx[tx] = []
            api_by_tx[tx].append(t)

    merged = list(rpc_trades)

    # For each RPC trade, enrich with API metadata if available
    for trade in merged:
        tx = trade.get("tx_hash", "")
        if tx and tx in api_by_tx and not trade.get("market_slug"):
            # Find matching API trade by token_id or side
            api_matches = api_by_tx[tx]
            token_id = trade.get("_token_id", "")
            for api_t in api_matches:
                if token_id and api_t.get("_token_id") == token_id:
                    trade["side"] = api_t.get("side", trade.get("side", ""))
                    trade["price"] = api_t.get("price", trade.get("price", ""))
                    trade["size"] = api_t.get("size", trade.get("size", ""))
                    trade["notional"] = api_t.get("notional", trade.get("notional", ""))
                    trade["market_slug"] = api_t.get("market_slug", "")
                    trade["market_question"] = api_t.get("market_question", "")
                    trade["event_slug"] = api_t.get("event_slug", "")
                    trade["outcome"] = api_t.get("outcome", "")
                    trade["notes"] = "Recent trade values replaced with Data API values"
                    break
            else:
                if api_matches:
                    best = api_matches[0]
                    trade["side"] = best.get("side", trade.get("side", ""))
                    trade["price"] = best.get("price", trade.get("price", ""))
                    trade["size"] = best.get("size", trade.get("size", ""))
                    trade["notional"] = best.get("notional", trade.get("notional", ""))
                    trade["market_slug"] = trade.get("market_slug") or best.get("market_slug", "")
                    trade["market_question"] = trade.get("market_question") or best.get("market_question", "")
                    trade["event_slug"] = trade.get("event_slug") or best.get("event_slug", "")
                    trade["outcome"] = trade.get("outcome") or best.get("outcome", "")
                    trade["notes"] = "Recent trade values replaced with Data API values"

    # Add API trades not in RPC data
    rpc_txs = set(t.get("tx_hash", "") for t in rpc_trades)
    for t in api_trades:
        if t.get("tx_hash", "") not in rpc_txs:
            merged.append(t)

    merged.sort(key=lambda t: int(t.get("block_number", 0) or 0) if t.get("block_number") else 0)
    log.info("Merged: %d total trades (RPC: %d, API-only: %d)",
             len(merged), len(rpc_trades), len(merged) - len(rpc_trades))
    return merged


# ---------------------------------------------------------------------------
# Enrich: fill block timestamps, Gamma API metadata
# ---------------------------------------------------------------------------

def enrich_timestamps(trades, rpc_url):
    """Fill in block timestamps for RPC-sourced trades missing them."""
    log = logging.getLogger("fetch_full")
    from fetchers.rpc_scanner import _rpc_call

    # Collect unique block numbers that need timestamps
    blocks_needed = set()
    for t in trades:
        if not t.get("timestamp") and t.get("block_number"):
            blocks_needed.add(int(t["block_number"]))

    if not blocks_needed:
        return trades

    log.info("Fetching timestamps for %d blocks...", len(blocks_needed))

    block_timestamps = {}
    for i, bn in enumerate(sorted(blocks_needed)):
        result = _rpc_call(rpc_url, "eth_getBlockByNumber", [hex(bn), False])
        if result and isinstance(result, dict):
            ts = int(result.get("timestamp", "0x0"), 16)
            block_timestamps[bn] = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        time.sleep(0.02)
        if (i + 1) % 1000 == 0:
            log.info("  Fetched %d/%d block timestamps", i + 1, len(blocks_needed))

    for t in trades:
        if not t.get("timestamp") and t.get("block_number"):
            ts = block_timestamps.get(int(t["block_number"]))
            if ts:
                t["timestamp"] = ts

    return trades


def enrich_with_gamma(trades):
    """Enrich trades with market metadata from Gamma API."""
    from fetchers.gamma_api import GammaAPIFetcher

    log = logging.getLogger("fetch_full")
    gamma = GammaAPIFetcher()

    token_ids = set(t.get("_token_id", "") for t in trades if t.get("_token_id") and not t.get("market_slug"))
    if not token_ids:
        return trades

    log.info("Enriching %d token IDs via Gamma API...", len(token_ids))
    market_lookup = {}
    for i, tid in enumerate(token_ids):
        m = gamma.get_market_by_token_id(tid)
        if m:
            market_lookup[tid] = m
        if (i + 1) % 50 == 0:
            log.info("  Gamma: %d/%d", i + 1, len(token_ids))

    from normalize.normalizer import enrich_with_market_data
    trades = enrich_with_market_data(trades, market_lookup)
    log.info("Enriched %d trades with Gamma data", len(market_lookup))
    return trades


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    log = setup_logging(args.verbose)
    wallet = args.wallet.lower()

    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    log.info("Polymarket Full History Fetcher")
    log.info("Wallet: %s", wallet)
    log.info("Output: %s", output_dir)
    log.info("")

    start_time = time.time()

    # Load resume data if specified
    resume_data = None
    if args.resume and os.path.exists(args.resume):
        log.info("Loading resume data from %s", args.resume)
        with open(args.resume, "r") as f:
            resume_data = json.load(f)

    # Step 1: Fetch
    activity_raw_rows = []
    if args.raw_file and os.path.exists(args.raw_file):
        log.info("Loading raw data from %s", args.raw_file)
        with open(args.raw_file, "r") as f:
            raw_data = json.load(f)
        rpc_trades = raw_data.get("trades", [])
        rpc_url = None
    elif args.activity_api:
        rpc_trades = []
        rpc_url = None
        raw_data = {"trades": [], "activity": [], "_source": "activity-api"}
    elif args.skip_rpc:
        rpc_trades = []
        rpc_url = get_working_rpc(args.rpc_url)
        raw_data = {"trades": [], "_source": "data-api"}
    else:
        rpc_url = get_working_rpc(args.rpc_url)
        rpc_result = fetch_via_rpc(
            wallet, args.rpc_url, resume_data,
            target_txs=args.target_txs,
            max_blocks=args.max_blocks,
        )
        if rpc_result:
            rpc_trades = rpc_result.get("trades", [])
        else:
            rpc_trades = []
            rpc_url = None
        raw_data = rpc_result or {"trades": []}

    # Step 2: Data API enrichment
    api_trades = []
    if args.activity_api:
        api_trades, activity_raw_rows = fetch_via_activity_api(wallet, args.target_txs)
        raw_data["activity"] = activity_raw_rows
        raw_data["trades"] = api_trades
    elif not args.no_api:
        api_trades = fetch_via_data_api(wallet)

    # Step 3: Merge
    if rpc_trades and api_trades:
        trades = merge_trades(rpc_trades, api_trades)
    elif rpc_trades:
        trades = rpc_trades
    elif api_trades:
        trades = api_trades
    else:
        trades = []

    trades.sort(key=lambda t: (t.get("timestamp", ""), int(t.get("block_number", 0) or 0)))

    # Step 4: Enrich timestamps
    if rpc_url and any(not t.get("timestamp") for t in trades):
        trades = enrich_timestamps(trades, rpc_url)
        trades.sort(key=lambda t: (t.get("timestamp", ""), int(t.get("block_number", 0) or 0)))

    # Step 5: Enrich with Gamma API
    if not args.no_gamma and trades:
        trades = enrich_with_gamma(trades)

    # Step 6: Compute positions and PnL
    if trades:
        from normalize.normalizer import compute_positions, compute_realized_pnl
        trades = compute_positions(trades, wallet)
        trades = compute_realized_pnl(trades, wallet)

    log.info("")
    log.info("=" * 60)
    log.info("FETCH COMPLETE")
    log.info("=" * 60)
    log.info("Total trades: %d", len(trades))

    buy_trades = [t for t in trades if t.get("side") == "BUY"]
    sell_trades = [t for t in trades if t.get("side") in ("SELL", "SETTLE")]
    log.info("Buys: %d, Sells/Settles: %d", len(buy_trades), len(sell_trades))
    log.info("Total buy amount: $%.2f", sum(float(t.get("notional", 0) or 0) for t in buy_trades))
    log.info("Total sell amount: $%.2f", sum(float(t.get("notional", 0) or 0) for t in sell_trades))

    # Save raw data
    raw_path = os.path.join(output_dir, "trades_raw_full.json")
    save_data = dict(raw_data) if raw_data else {}
    save_data["trades"] = trades
    with open(raw_path, "w") as f:
        json.dump(save_data, f, indent=2, default=str, ensure_ascii=False)
    log.info("Saved raw data: %s", raw_path)

    # Save normalized CSV
    from outputs.csv_writer import write_trades_csv
    csv_path = os.path.join(output_dir, "trades_normalized.csv")
    write_trades_csv(trades, csv_path)

    # Generate the same analytics/report artifacts as main.py from the collected data.
    try:
        from main import analyze_trades, generate_outputs
        analytics = analyze_trades(trades, wallet)
        generate_outputs(trades, save_data, analytics, wallet, output_dir)
    except Exception as e:
        log.warning("Analytics/report generation failed: %s", e, exc_info=True)

    elapsed = time.time() - start_time
    log.info("")
    log.info("Done in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
