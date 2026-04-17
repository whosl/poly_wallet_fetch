#!/usr/bin/env python3
"""
Polymarket Wallet Full Trade Export & Analysis Tool

Data sources (in priority order):
1. Polymarket Data API (data-api.polymarket.com) - PRIMARY
2. Polygonscan API - SECONDARY (needs API key for V2)
3. Polygon RPC log scanning - TERTIARY

Usage:
    python main.py --wallet 0xeebde7a0e019a63e6b476eb425505b7b3e6eba30
    python main.py --wallet 0x... --source data-api
    python main.py --wallet 0x... --source polygonscan --polygonscan-key KEY
    python main.py --wallet 0x... --source rpc --rpc-url https://polygon.drpc.org
    python main.py --wallet 0x... --skip-fetch --raw-file data/trades_raw.json
"""

import argparse
import json
import logging
import os
import sys
import time
import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DEFAULT_WALLET, OUTPUT_DIR


def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger("main")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Polymarket Wallet Full Trade Export & Analysis Tool"
    )
    parser.add_argument(
        "--wallet", "-w", default=DEFAULT_WALLET,
        help="Target wallet address",
    )
    parser.add_argument(
        "--source", "-s", default="auto",
        choices=["auto", "data-api", "polygonscan", "rpc", "file"],
        help="Data source (default: auto-try all)",
    )
    parser.add_argument(
        "--polygonscan-key", "-k", default="",
        help="Polygonscan API key",
    )
    parser.add_argument(
        "--output-dir", "-o", default=OUTPUT_DIR,
        help="Output directory",
    )
    parser.add_argument(
        "--rpc-url", default="",
        help="Custom Polygon RPC URL",
    )
    parser.add_argument(
        "--skip-fetch", action="store_true",
        help="Skip fetching, use existing raw data",
    )
    parser.add_argument(
        "--raw-file", default="",
        help="Path to existing raw JSON data file",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--no-gamma", action="store_true",
        help="Skip Gamma API enrichment",
    )
    return parser.parse_args()


def fetch_from_data_api(wallet):
    """Try to fetch from Polymarket Data API."""
    from fetchers.polymarket_data_api import PolymarketDataFetcher
    log = logging.getLogger("main")
    log.info("Trying Polymarket Data API...")
    try:
        fetcher = PolymarketDataFetcher()
        data = fetcher.fetch_all(wallet)
        if data.get("trades"):
            log.info("Data API: Got %d trades", len(data["trades"]))
            data["_source"] = "data-api"
            return data
        log.info("Data API: No trades returned")
    except Exception as e:
        log.warning("Data API failed: %s", e)
    return None


def fetch_from_polygonscan(wallet, api_key):
    """Try to fetch from Polygonscan."""
    from fetchers.polygonscan import PolygonscanFetcher
    log = logging.getLogger("main")
    log.info("Trying Polygonscan...")
    try:
        ps = PolygonscanFetcher(api_key=api_key)
        data = ps.get_all_wallet_data(wallet)
        data["_source"] = "polygonscan"
        return data
    except Exception as e:
        log.warning("Polygonscan failed: %s", e)
    return None


def fetch_from_rpc(wallet, rpc_url):
    """Try to fetch via Polygon RPC log scanning."""
    from fetchers.polymarket_rpc_fetcher import fetch_via_rpc
    log = logging.getLogger("main")
    log.info("Trying Polygon RPC log scanning...")
    try:
        data = fetch_via_rpc(wallet, rpc_url)
        if data:
            data["_source"] = "rpc"
            return data
    except Exception as e:
        log.warning("RPC failed: %s", e)
    return None


def fetch_data(wallet, source, polygonscan_key, rpc_url, skip_fetch, raw_file):
    """Step 1: Fetch raw data from best available source."""
    log = logging.getLogger("main")

    if skip_fetch and raw_file and os.path.exists(raw_file):
        log.info("Loading raw data from %s", raw_file)
        with open(raw_file, "r") as f:
            return json.load(f)

    log.info("=" * 60)
    log.info("Step 1: Fetching data for %s", wallet)
    log.info("=" * 60)

    data = None

    if source == "data-api":
        data = fetch_from_data_api(wallet)
    elif source == "polygonscan":
        data = fetch_from_polygonscan(wallet, polygonscan_key)
    elif source == "rpc":
        data = fetch_from_rpc(wallet, rpc_url)
    elif source == "auto":
        # Try all sources in order
        data = fetch_from_data_api(wallet)
        if not data or not data.get("trades"):
            data = fetch_from_polygonscan(wallet, polygonscan_key)
        if not data:
            data = fetch_from_rpc(wallet, rpc_url)

    if not data:
        data = {
            "_source": "none",
            "trades": [],
            "erc1155_transfers": [],
            "erc20_transfers": [],
            "normal_txs": [],
            "internal_txs": [],
        }
        log.warning("All data sources failed. Generating empty output.")

    return data


def normalize_data(raw_data, wallet, no_gamma):
    """Step 2+3: Decode and normalize into standardized trade records."""
    log = logging.getLogger("main")
    log.info("=" * 60)
    log.info("Step 2: Normalizing data (source: %s)", raw_data.get("_source", "unknown"))
    log.info("=" * 60)

    source = raw_data.get("_source", "unknown")

    if source == "data-api":
        trades = normalize_data_api_trades(raw_data, wallet)
    elif source in ("polygonscan", "rpc"):
        trades = normalize_chain_data(raw_data, wallet, no_gamma)
    elif source == "none":
        trades = []
    else:
        # Try data-api format first, then chain format
        if raw_data.get("trades") and isinstance(raw_data["trades"], list):
            trades = normalize_data_api_trades(raw_data, wallet)
        else:
            trades = normalize_chain_data(raw_data, wallet, no_gamma)

    from normalize.normalizer import compute_positions, compute_realized_pnl
    trades = compute_positions(trades, wallet)
    trades = compute_realized_pnl(trades, wallet)

    log.info("Normalized %d trade records", len(trades))
    return trades


def normalize_data_api_trades(raw_data, wallet):
    """Normalize trades from the Polymarket Data API format."""
    from normalize.normalizer import CSV_FIELDS

    raw_trades = raw_data.get("trades", [])
    log = logging.getLogger("main")
    trades = []

    for i, rt in enumerate(raw_trades):
        timestamp = rt.get("timestamp", "")
        if timestamp:
            try:
                ts_int = int(timestamp)
                dt = datetime.datetime.utcfromtimestamp(ts_int)
                timestamp_iso = dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError, OSError):
                timestamp_iso = str(timestamp)
        else:
            timestamp_iso = ""

        side = rt.get("side", "").upper()
        price = float(rt.get("price", 0) or 0)
        size = float(rt.get("size", 0) or 0)
        notional = price * size

        trade = {
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
            "notional": round(notional, 6),
            "fee": "",
            "realized_pnl": "",
            "position_after": "",
            "order_id": "",
            "trade_id": "api_{}".format(i),
            "source": "data-api",
            "settlement_value": "",
            "notes": "",
            # Internal fields for analytics
            "_token_id": rt.get("asset", ""),
            "_condition_id": rt.get("conditionId", ""),
        }
        trades.append(trade)

    log.info("Normalized %d Data API trades", len(trades))
    return trades


def normalize_chain_data(raw_data, wallet, no_gamma):
    """Normalize from on-chain decoded events."""
    from decoders.master_decoder import MasterDecoder
    from normalize.normalizer import match_trades_by_tx, enrich_with_market_data
    from fetchers.gamma_api import GammaAPIFetcher

    log = logging.getLogger("main")
    decoder = MasterDecoder(wallet)
    events = decoder.decode_all(raw_data)
    trades = match_trades_by_tx(events)

    if not no_gamma and trades:
        gamma = GammaAPIFetcher()
        token_ids = set(t.get("_token_id", "") for t in trades if t.get("_token_id"))
        market_lookup = {}
        for tid in token_ids:
            m = gamma.get_market_by_token_id(tid)
            if m:
                market_lookup[tid] = m
        trades = enrich_with_market_data(trades, market_lookup)

    return trades


def analyze_trades(trades, wallet):
    """Step 4: Run all analytics."""
    from analytics.statistics import compute_overall_stats, estimate_unrealized_pnl
    from analytics.market_analysis import compute_market_stats, find_dual_side_markets
    from analytics.time_analysis import (
        compute_daily_stats, compute_hourly_distribution,
        find_active_periods, compute_holding_period_estimate,
    )
    from analytics.behavior import (
        analyze_price_buckets, detect_averaging_behavior,
        detect_dual_side_asymmetry, analyze_market_preference,
    )
    from analytics.style import generate_style_diagnosis

    log = logging.getLogger("main")
    log.info("=" * 60)
    log.info("Step 3: Running analytics")
    log.info("=" * 60)

    overall_stats = compute_overall_stats(trades)
    market_stats = compute_market_stats(trades)
    dual_side = find_dual_side_markets(market_stats)
    dual_asymmetry = detect_dual_side_asymmetry(trades, market_stats)
    daily_stats = compute_daily_stats(trades)
    hourly_dist = compute_hourly_distribution(trades)
    active_periods = find_active_periods(daily_stats)
    holding_period = compute_holding_period_estimate(trades)
    price_buckets = analyze_price_buckets(trades)
    behavior_stats = detect_averaging_behavior(trades)
    market_pref = analyze_market_preference(trades)
    style_result = generate_style_diagnosis(
        trades, overall_stats, market_stats, hourly_dist,
        behavior_stats, price_buckets, dual_asymmetry,
        market_pref, holding_period,
    )
    unrealized = estimate_unrealized_pnl(trades)

    return {
        "overall_stats": overall_stats,
        "market_stats": market_stats,
        "dual_side": dual_side,
        "daily_stats": daily_stats,
        "hourly_dist": hourly_dist,
        "active_periods": active_periods,
        "holding_period": holding_period,
        "price_buckets": price_buckets,
        "behavior_stats": behavior_stats,
        "market_pref": market_pref,
        "style_result": style_result,
        "unrealized": unrealized,
    }


def generate_outputs(trades, raw_data, analytics, wallet, output_dir):
    """Step 5: Generate all output files."""
    from outputs.csv_writer import write_trades_csv, write_positions_csv, write_daily_csv, write_raw_json
    from outputs.report import generate_report, generate_style_diagnosis

    log = logging.getLogger("main")
    log.info("=" * 60)
    log.info("Step 4: Generating output files")
    log.info("=" * 60)

    os.makedirs(output_dir, exist_ok=True)

    raw_path = os.path.join(output_dir, "trades_raw.json")
    write_raw_json(raw_data, raw_path)

    csv_path = os.path.join(output_dir, "trades_normalized.csv")
    write_trades_csv(trades, csv_path)

    pos_path = os.path.join(output_dir, "positions_summary.csv")
    write_positions_csv(analytics["market_stats"], pos_path)

    daily_path = os.path.join(output_dir, "daily_summary.csv")
    write_daily_csv(analytics["daily_stats"], daily_path)

    report_path = generate_report(
        wallet, analytics["overall_stats"], analytics["market_stats"],
        analytics["daily_stats"], analytics["hourly_dist"],
        analytics["active_periods"], analytics["holding_period"],
        analytics["price_buckets"], analytics["behavior_stats"],
        analytics["dual_side"], analytics["market_pref"],
        analytics["style_result"], analytics["unrealized"],
        output_dir,
    )

    style_path = generate_style_diagnosis(
        wallet, analytics["style_result"], output_dir,
    )

    return {
        "raw_json": raw_path,
        "normalized_csv": csv_path,
        "positions_csv": pos_path,
        "daily_csv": daily_path,
        "report": report_path,
        "style_diagnosis": style_path,
    }


def main():
    args = parse_args()
    log = setup_logging(args.verbose)

    wallet = args.wallet.lower()
    output_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        args.output_dir,
    )

    log.info("Polymarket Wallet Analyzer")
    log.info("Wallet: %s", wallet)
    log.info("Output: %s", output_dir)
    log.info("")

    start_time = time.time()

    try:
        raw_data = fetch_data(
            wallet, args.source, args.polygonscan_key, args.rpc_url,
            args.skip_fetch, args.raw_file,
        )
        trades = normalize_data(raw_data, wallet, args.no_gamma)
        analytics = analyze_trades(trades, wallet)
        output_files = generate_outputs(trades, raw_data, analytics, wallet, output_dir)

        elapsed = time.time() - start_time
        log.info("")
        log.info("=" * 60)
        log.info("DONE in %.1f seconds", elapsed)
        log.info("=" * 60)
        for name, path in output_files.items():
            log.info("  %s: %s", name, path)

        stats = analytics["overall_stats"]
        log.info("")
        log.info("Summary:")
        log.info("  Trades: %d", stats.get("total_trades", 0))
        log.info("  Buy: $%.2f", stats.get("total_buy_amount", 0))
        log.info("  Sell: $%.2f", stats.get("total_sell_amount", 0))
        log.info("  PnL: $%.2f", stats.get("total_realized_pnl", 0))
        log.info("  Win rate: %.1f%%", stats.get("win_rate", 0))

    except KeyboardInterrupt:
        log.info("Interrupted")
        sys.exit(1)
    except Exception as e:
        log.error("Fatal: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
