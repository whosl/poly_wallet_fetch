#!/usr/bin/env python3
"""
Pre-fetch script: Uses web requests to get data from Polymarket's Data API.
Run this script separately if you have direct API access, or populate the
data file manually for constrained environments.

Usage:
    python prefetch_data.py --wallet 0x... --output data/trades_raw.json
"""

import argparse
import json
import logging
import os
import sys
import time
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("prefetch")

DATA_API = "https://data-api.polymarket.com"


def fetch_trades(wallet, limit=10000):
    """Fetch all trades for a wallet from the Data API."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json",
    })

    all_trades = []
    offset = 0
    page = min(limit, 10000)

    while True:
        url = "{}/trades".format(DATA_API)
        params = {"user": wallet, "limit": page, "offset": offset, "takerOnly": "false"}

        try:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            trades = resp.json()
        except Exception as e:
            log.error("Failed at offset %d: %s", offset, e)
            break

        if not isinstance(trades, list):
            log.error("Unexpected response at offset %d", offset)
            break

        all_trades.extend(trades)
        log.info("Fetched %d trades (total: %d, offset: %d)", len(trades), len(all_trades), offset)

        if len(trades) < page:
            break

        offset += len(trades)
        time.sleep(0.3)

        if offset >= 100000:
            log.warning("Reached 100k limit")
            break

    return all_trades


def fetch_positions(wallet):
    """Fetch current positions."""
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    try:
        r = session.get("{}/positions".format(DATA_API), params={"user": wallet}, timeout=30)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


def fetch_closed_positions(wallet):
    """Fetch closed positions."""
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    try:
        r = session.get("{}/positions/closed".format(DATA_API), params={"user": wallet}, timeout=30)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


def fetch_activity(wallet, limit=10000):
    """Fetch activity feed."""
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    try:
        r = session.get("{}/activity".format(DATA_API), params={"user": wallet, "limit": limit}, timeout=30)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


def main():
    parser = argparse.ArgumentParser(description="Pre-fetch Polymarket data via API")
    parser.add_argument("--wallet", "-w", required=True, help="Wallet address")
    parser.add_argument("--output", "-o", default="data/trades_raw.json", help="Output file")
    args = parser.parse_args()

    wallet = args.wallet.lower()
    output = args.output

    log.info("Fetching data for %s", wallet)

    data = {
        "_source": "data-api",
        "trades": fetch_trades(wallet),
        "positions": fetch_positions(wallet),
        "closed_positions": fetch_closed_positions(wallet),
        "activity": fetch_activity(wallet),
    }

    log.info("Total trades: %d", len(data["trades"]))
    log.info("Current positions: %d", len(data.get("positions", [])))
    log.info("Closed positions: %d", len(data.get("closed_positions", [])))
    log.info("Activity items: %d", len(data.get("activity", [])))

    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    with open(output, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info("Saved to %s", output)


if __name__ == "__main__":
    main()
