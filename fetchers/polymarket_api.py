"""
Polygon RPC fetcher.
Direct blockchain queries for transaction receipts and logs.
Fallback when Polygonscan API is insufficient.
"""

import time
import logging
import requests

from config import POLYGON_RPC_URLS

log = logging.getLogger(__name__)


class PolygonRPCFetcher:
    """Direct Polygon RPC for fetching transaction data."""

    def __init__(self, rpc_url=None):
        self.rpc_url = rpc_url or POLYGON_RPC_URLS[0]
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "PolyWhaleAnalyzer/1.0",
        })
        self._last_call = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_call
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)
        self._last_call = time.time()

    def _call(self, method, params):
        self._rate_limit()
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        try:
            resp = self.session.post(self.rpc_url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                log.error("RPC error: %s", data["error"])
                return None
            return data.get("result")
        except Exception as e:
            log.error("RPC call error (%s): %s", method, e)
            # Try next RPC URL
            for alt_url in POLYGON_RPC_URLS:
                if alt_url != self.rpc_url:
                    try:
                        resp = self.session.post(alt_url, json=payload, timeout=30)
                        resp.raise_for_status()
                        data = resp.json()
                        if "result" in data:
                            self.rpc_url = alt_url
                            return data["result"]
                    except Exception:
                        continue
            return None

    def get_transaction_receipt(self, tx_hash):
        """Get full transaction receipt with logs."""
        return self._call("eth_getTransactionReceipt", [tx_hash])

    def get_transaction_by_hash(self, tx_hash):
        """Get transaction details."""
        return self._call("eth_getTransactionByHash", [tx_hash])

    def get_block(self, block_number_hex):
        """Get block details by number."""
        return self._call("eth_getBlockByNumber", [block_number_hex, False])

    def get_logs(self, address, from_block, to_block, topics=None):
        """
        Get filtered logs for a contract address.
        Useful for scanning Polymarket exchange events.
        """
        params = {
            "fromBlock": hex(from_block) if isinstance(from_block, int) else from_block,
            "toBlock": hex(to_block) if isinstance(to_block, int) else to_block,
            "address": address,
        }
        if topics:
            params["topics"] = topics
        return self._call("eth_getLogs", [params])

    def batch_get_receipts(self, tx_hashes, batch_size=5):
        """Get receipts for multiple transactions with rate limiting."""
        receipts = {}
        for i, tx_hash in enumerate(tx_hashes):
            receipt = self.get_transaction_receipt(tx_hash)
            if receipt:
                receipts[tx_hash] = receipt
            if (i + 1) % 10 == 0:
                log.info("Fetched %d/%d receipts", i + 1, len(tx_hashes))
        return receipts
