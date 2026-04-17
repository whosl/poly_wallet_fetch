"""
Polygonscan API fetcher.
Gets all transactions, ERC20 transfers, and ERC1155 transfers for a wallet.
Handles pagination and rate limiting.
"""

import time
import logging
import requests

from config import POLYGONSCAN_API, POLYGONSCAN_DELAY, POLYGONSCAN_PAGE_SIZE

log = logging.getLogger(__name__)


class PolygonscanFetcher:
    """Fetch wallet data from Polygonscan API."""

    def __init__(self, api_key=""):
        self.api_key = api_key
        self.base = POLYGONSCAN_API
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "PolyWhaleAnalyzer/1.0"})
        self._last_call = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_call
        if elapsed < POLYGONSCAN_DELAY:
            time.sleep(POLYGONSCAN_DELAY - elapsed)
        self._last_call = time.time()

    def _get(self, params):
        """Make a rate-limited GET request to Polygonscan."""
        self._rate_limit()
        params["apikey"] = self.api_key
        try:
            resp = self.session.get(self.base, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") == "0" and data.get("message") == "No transactions found":
                return []
            if data.get("status") == "0":
                log.warning("Polygonscan API note: %s", data.get("result", "")[:200])
                return []
            return data.get("result", [])
        except Exception as e:
            log.error("Polygonscan API error: %s", e)
            return []

    def get_normal_transactions(self, address, start_block=0, end_block=99999999):
        """Get all normal transactions for address."""
        all_txs = []
        page = 1
        while True:
            params = {
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": POLYGONSCAN_PAGE_SIZE,
                "sort": "asc",
            }
            txs = self._get(params)
            if not txs:
                break
            all_txs.extend(txs)
            if len(txs) < POLYGONSCAN_PAGE_SIZE:
                break
            page += 1
        log.info("Fetched %d normal transactions for %s", len(all_txs), address)
        return all_txs

    def get_erc20_transfers(self, address, start_block=0, end_block=99999999):
        """Get all ERC20 token transfers for address."""
        all_txs = []
        page = 1
        while True:
            params = {
                "module": "account",
                "action": "tokentx",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": POLYGONSCAN_PAGE_SIZE,
                "sort": "asc",
                "contractaddress": "",
            }
            txs = self._get(params)
            if not txs:
                break
            all_txs.extend(txs)
            if len(txs) < POLYGONSCAN_PAGE_SIZE:
                break
            page += 1
        log.info("Fetched %d ERC20 transfers for %s", len(all_txs), address)
        return all_txs

    def get_erc1155_transfers(self, address, start_block=0, end_block=99999999):
        """Get all ERC1155 token transfers for address."""
        all_txs = []
        page = 1
        while True:
            params = {
                "module": "account",
                "action": "token1155tx",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": POLYGONSCAN_PAGE_SIZE,
                "sort": "asc",
            }
            txs = self._get(params)
            if not txs:
                break
            all_txs.extend(txs)
            if len(txs) < POLYGONSCAN_PAGE_SIZE:
                break
            page += 1
        log.info("Fetched %d ERC1155 transfers for %s", len(all_txs), address)
        return all_txs

    def get_internal_transactions(self, address, start_block=0, end_block=99999999):
        """Get internal transactions for address."""
        all_txs = []
        page = 1
        while True:
            params = {
                "module": "account",
                "action": "txlistinternal",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": POLYGONSCAN_PAGE_SIZE,
                "sort": "asc",
            }
            txs = self._get(params)
            if not txs:
                break
            all_txs.extend(txs)
            if len(txs) < POLYGONSCAN_PAGE_SIZE:
                break
            page += 1
        log.info("Fetched %d internal transactions for %s", len(all_txs), address)
        return all_txs

    def get_transaction_logs(self, tx_hash):
        """Get logs for a specific transaction."""
        params = {
            "module": "proxy",
            "action": "eth_getTransactionReceipt",
            "txhash": tx_hash,
        }
        self._rate_limit()
        try:
            resp = self.session.get(self.base, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            result = data.get("result", {})
            return result.get("logs", [])
        except Exception as e:
            log.error("Error fetching logs for %s: %s", tx_hash, e)
            return []

    def get_all_wallet_data(self, address):
        """Fetch all available data for a wallet from Polygonscan."""
        log.info("Starting full Polygonscan fetch for %s", address)
        result = {
            "normal_txs": self.get_normal_transactions(address),
            "erc20_transfers": self.get_erc20_transfers(address),
            "erc1155_transfers": self.get_erc1155_transfers(address),
            "internal_txs": self.get_internal_transactions(address),
        }
        # Collect unique tx hashes for log fetching
        tx_hashes = set()
        for tx in result["normal_txs"]:
            h = tx.get("hash", "")
            if h:
                tx_hashes.add(h)
        result["tx_hashes"] = list(tx_hashes)
        log.info(
            "Polygonscan fetch complete: %d normal txs, %d ERC20, %d ERC1155, %d internal",
            len(result["normal_txs"]),
            len(result["erc20_transfers"]),
            len(result["erc1155_transfers"]),
            len(result["internal_txs"]),
        )
        return result
