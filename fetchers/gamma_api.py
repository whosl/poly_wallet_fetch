"""
Polymarket Gamma API fetcher.
Gets market metadata (questions, outcomes, slugs) for condition IDs.
"""

import time
import logging
import requests

from config import GAMMA_API, GAMMA_API_DELAY, GAMMA_PAGE_SIZE

log = logging.getLogger(__name__)


class GammaAPIFetcher:
    """Fetch market metadata from Polymarket's Gamma API."""

    def __init__(self):
        self.base = GAMMA_API
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PolyWhaleAnalyzer/1.0",
            "Accept": "application/json",
        })
        self._last_call = 0.0
        # Cache: condition_id -> market data
        self._cache = {}

    def _rate_limit(self):
        elapsed = time.time() - self._last_call
        if elapsed < GAMMA_API_DELAY:
            time.sleep(GAMMA_API_DELAY - elapsed)
        self._last_call = time.time()

    def _get(self, path, params=None):
        self._rate_limit()
        url = self.base + path
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.error("Gamma API error for %s: %s", path, e)
            return None

    def get_market_by_condition_id(self, condition_id):
        """Look up market by condition ID."""
        if condition_id in self._cache:
            return self._cache[condition_id]
        # Try markets endpoint
        data = self._get("/markets", {"condition_id": condition_id})
        if data and isinstance(data, list) and len(data) > 0:
            self._cache[condition_id] = data[0]
            return data[0]
        # Try single market
        data = self._get("/markets/{}".format(condition_id))
        if data and isinstance(data, dict):
            self._cache[condition_id] = data
            return data
        return None

    def get_market_by_token_id(self, token_id):
        """Look up market by ERC1155 token ID."""
        data = self._get("/markets", {"token_id": token_id})
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None

    def get_event(self, event_slug):
        """Get event details by slug."""
        data = self._get("/events", {"slug": event_slug})
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None

    def search_markets(self, query, limit=50):
        """Search markets by text query."""
        data = self._get("/markets", {
            "closed": "true",
            "limit": limit,
            "order": "volume",
            "ascending": "false",
            "text_query": query,
        })
        return data if isinstance(data, list) else []

    def enrich_transfers(self, transfers):
        """
        Given a list of ERC1155 transfers (with tokenID fields),
        enrich each with market metadata.
        """
        token_ids = set()
        for t in transfers:
            tid = t.get("tokenID", t.get("token_id", ""))
            if tid:
                token_ids.add(tid)

        log.info("Enriching %d unique token IDs", len(token_ids))
        token_to_market = {}
        for tid in token_ids:
            market = self.get_market_by_token_id(tid)
            if market:
                token_to_market[tid] = market
            time.sleep(0.05)

        for t in transfers:
            tid = t.get("tokenID", t.get("token_id", ""))
            if tid in token_to_market:
                t["_market"] = token_to_market[tid]
        return transfers

    def get_all_markets_batch(self, condition_ids):
        """Batch-fetch market data for multiple condition IDs."""
        results = {}
        for cid in condition_ids:
            market = self.get_market_by_condition_id(cid)
            if market:
                results[cid] = market
        return results
