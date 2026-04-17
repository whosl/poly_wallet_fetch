"""
Polymarket Data API fetcher.
Uses the public data-api.polymarket.com endpoints to fetch trade history,
positions, and activity for a wallet.

This is the PRIMARY data source - much more reliable than on-chain decoding
for Polymarket trades since Polymarket uses a relayer/proxy wallet system.
"""

import time
import logging
import json
import os

log = logging.getLogger(__name__)

# Data API base URL
DATA_API_BASE = "https://data-api.polymarket.com"

# Endpoints
ENDPOINTS = {
    "trades": "/trades",
    "positions": "/positions",
    "closed_positions": "/positions/closed",
    "activity": "/activity",
    "profile": "/profile/{}",
    "total_value": "/value",
    "total_markets": "/markets-traded",
}


class PolymarketDataFetcher:
    """Fetch trade data from Polymarket's Data API."""

    def __init__(self):
        self.base = DATA_API_BASE
        self._last_call = 0.0
        self._session = None
        self._use_web_reader = False  # Will try requests first, then web reader

    def _get_via_requests(self, endpoint, params=None):
        """Try to fetch via requests library."""
        import requests
        if not self._session:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "application/json",
            })

        url = self.base + endpoint
        elapsed = time.time() - self._last_call
        if elapsed < 0.3:
            time.sleep(0.3 - elapsed)
        self._last_call = time.time()

        try:
            resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning("Requests failed for %s: %s", url, e)
            return None

    def _get_via_web_reader(self, url):
        """Fallback: use MCP web reader tool (not available in CLI mode)."""
        return None

    def _get(self, endpoint, params=None):
        """Make an API call with fallback."""
        data = self._get_via_requests(endpoint, params)
        if data is not None:
            return data
        return None

    def get_trades(self, user, limit=10000, offset=0, taker_only=False):
        """
        Get all trades for a user.
        Handles pagination to fetch all available trades.
        """
        all_trades = []
        page_offset = offset
        page_size = min(limit, 1000)

        while True:
            params = {
                "user": user,
                "limit": page_size,
                "offset": page_offset,
                "takerOnly": str(taker_only).lower(),
            }
            trades = self._get("/trades", params)

            if trades is None:
                log.warning("Failed to fetch trades at offset %d, stopping", page_offset)
                break
            if not isinstance(trades, list):
                log.warning("Unexpected trades response: %s", str(trades)[:200])
                break

            all_trades.extend(trades)
            log.info("Fetched %d trades (total: %d, offset: %d)",
                     len(trades), len(all_trades), page_offset)

            if len(trades) < page_size:
                break  # No more pages

            page_offset += len(trades)

            # Data API hard cap
            if page_offset >= 3001:
                log.warning("Reached Data API offset cap")
                break

        return all_trades

    def get_positions(self, user):
        """Get current open positions for a user."""
        params = {"user": user}
        positions = self._get("/positions", params)
        if isinstance(positions, list):
            return positions
        return []

    def get_closed_positions(self, user):
        """Get closed positions for a user."""
        params = {"user": user}
        positions = self._get("/positions/closed", params)
        if isinstance(positions, list):
            return positions
        return []

    def get_activity(self, user, limit=10000):
        """Get user activity feed."""
        params = {"user": user, "limit": limit}
        activity = self._get("/activity", params)
        if isinstance(activity, list):
            return activity
        return []

    def get_profile(self, user):
        """Get user public profile."""
        data = self._get("/profile/{}".format(user))
        if isinstance(data, dict):
            return data
        return {}

    def get_total_markets(self, user):
        """Get total markets traded count."""
        params = {"user": user}
        data = self._get("/markets-traded", params)
        return data

    def fetch_all(self, wallet):
        """
        Fetch all available data for a wallet from the Data API.
        Returns a dict with all fetched data.
        """
        log.info("Starting Polymarket Data API fetch for %s", wallet)
        result = {
            "trades": [],
            "positions": [],
            "closed_positions": [],
            "activity": [],
            "profile": {},
            "source": "data-api",
        }

        # 1. Fetch profile
        log.info("Fetching profile...")
        result["profile"] = self.get_profile(wallet)

        # 2. Fetch trades (this is the main data)
        log.info("Fetching trades...")
        result["trades"] = self.get_trades(wallet)
        log.info("Total trades fetched: %d", len(result["trades"]))

        # 3. Fetch current positions
        log.info("Fetching current positions...")
        result["positions"] = self.get_positions(wallet)
        log.info("Current positions: %d", len(result["positions"]))

        # 4. Fetch closed positions
        log.info("Fetching closed positions...")
        result["closed_positions"] = self.get_closed_positions(wallet)
        log.info("Closed positions: %d", len(result["closed_positions"]))

        # 5. Fetch activity
        log.info("Fetching activity...")
        result["activity"] = self.get_activity(wallet)
        log.info("Activity items: %d", len(result["activity"]))

        return result


def fetch_via_polygonscan_v2(wallet, api_key=""):
    """
    Fetch data from Polygonscan V2 API (Etherscan V2 compatible).
    Returns raw transaction data.
    """
    import requests

    base = "https://api.etherscan.io/v2/api"
    session = requests.Session()
    session.headers.update({"User-Agent": "PolyWhaleAnalyzer/1.0"})

    result = {"normal_txs": [], "erc20": [], "erc1155": [], "internal": []}
    params_base = {
        "chainid": "137",
        "apikey": api_key,
    }

    # ERC1155 transfers (most important for Polymarket)
    try:
        params = dict(params_base)
        params.update({
            "module": "account",
            "action": "token1155tx",
            "address": wallet,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 10000,
            "sort": "asc",
        })
        r = session.get(base, params=params, timeout=30)
        data = r.json()
        if isinstance(data.get("result"), list):
            result["erc1155"] = data["result"]
    except Exception as e:
        log.warning("V2 ERC1155 fetch failed: %s", e)

    # ERC20 transfers
    try:
        params = dict(params_base)
        params.update({
            "module": "account",
            "action": "tokentx",
            "address": wallet,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 10000,
            "sort": "asc",
        })
        r = session.get(base, params=params, timeout=30)
        data = r.json()
        if isinstance(data.get("result"), list):
            result["erc20"] = data["result"]
    except Exception as e:
        log.warning("V2 ERC20 fetch failed: %s", e)

    return result


def load_from_mcp_fetch(wallet, data_dir):
    """
    If we've pre-fetched data via MCP web tools, load it from disk.
    This supports offline / constrained environments.
    """
    trades_file = os.path.join(data_dir, "mcp_trades.json")
    if os.path.exists(trades_file):
        with open(trades_file, "r") as f:
            return json.load(f)
    return None
