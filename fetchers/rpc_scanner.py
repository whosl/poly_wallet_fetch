"""
Reusable Polygon RPC scanner for Polymarket trade history.
Scans USDC transfers in small block-range chunks to bypass
the Data API's 3000-offset cap.
"""

import logging
import time
import requests

from config import (
    RPC_CHUNK_SIZE,
    RPC_CANDIDATE_URLS,
    BINARY_SEARCH_CHUNK,
    USDC_DECIMALS,
)

log = logging.getLogger(__name__)

ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"


def _pad_address(address):
    return "0x" + "0" * 24 + address[2:].lower()


def get_working_rpc(rpc_url=None):
    """Find a working RPC endpoint with eth_getLogs support."""
    from config import POLYMARKET_CONTRACTS

    candidates = [rpc_url] + RPC_CANDIDATE_URLS if rpc_url else RPC_CANDIDATE_URLS
    for url in candidates:
        if not url:
            continue
        try:
            r = requests.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
                timeout=10,
            )
            if r.status_code != 200 or "result" not in r.json():
                continue

            latest = int(r.json()["result"], 16)
            logs_resp = requests.post(
                url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_getLogs",
                    "params": [{
                        "fromBlock": hex(max(latest - 5, 0)),
                        "toBlock": hex(latest),
                        "address": POLYMARKET_CONTRACTS["usdc_e"],
                        "topics": [ERC20_TRANSFER_TOPIC, _pad_address("0x0000000000000000000000000000000000000001")],
                    }],
                },
                timeout=30,
            )
            if logs_resp.status_code == 200 and "result" in logs_resp.json():
                return url
        except Exception:
            continue
    return None


def _rpc_post(url, method, params, timeout=120):
    """Make an RPC call that returns a list result (for eth_getLogs)."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        data = r.json()
        result = data.get("result", [])
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        log.debug("RPC %s failed: %s", method, e)
        return []


def _rpc_call(url, method, params, timeout=120):
    """Make an RPC call that returns a scalar or dict result (for eth_blockNumber, eth_call, etc)."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        data = r.json()
        return data.get("result")
    except Exception as e:
        log.debug("RPC %s failed: %s", method, e)
        return None


def get_latest_block(rpc_url):
    """Get latest block number from RPC."""
    result = _rpc_call(rpc_url, "eth_blockNumber", [])
    if result:
        return int(result, 16)
    return 0


def get_block_timestamp(rpc_url, block_number):
    """Get timestamp for a block."""
    result = _rpc_call(rpc_url, "eth_getBlockByNumber", [hex(block_number), False])
    if result and isinstance(result, dict):
        ts_hex = result.get("timestamp", "0x0")
        return int(ts_hex, 16)
    return 0


def find_activity_range(rpc_url, wallet, usdc_address=None):
    """
    Binary search to find the earliest and latest blocks with USDC activity.
    Returns (earliest_block, latest_block).
    """
    from config import POLYMARKET_CONTRACTS

    if usdc_address is None:
        usdc_address = POLYMARKET_CONTRACTS["usdc_e"]

    wallet_padded = _pad_address(wallet)
    latest = get_latest_block(rpc_url)
    if not latest:
        log.error("Cannot get latest block")
        return 0, 0

    log.info("Latest block: %d", latest)

    # Check if there's any activity at all
    # Scan in 500-block chunks near the tip
    logs = []
    for start in range(latest - 5000, latest + 1, 500):
        end = min(start + 499, latest)
        chunk_logs = _rpc_post(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(start), "toBlock": hex(end),
            "address": usdc_address,
            "topics": [ERC20_TRANSFER_TOPIC, wallet_padded],
        }])
        if chunk_logs:
            logs.extend(chunk_logs)
        time.sleep(0.05)
        if logs:
            break

    if not logs:
        log.info("No USDC activity found for %s", wallet)
        return 0, 0

    earliest_found = min(int(l["blockNumber"], 16) for l in logs)
    log.info("Found activity around block %d", earliest_found)

    # Binary search backwards using fixed-size windows
    # Test 10k-block windows going backwards until we find one with no activity
    step = BINARY_SEARCH_CHUNK
    lo = max(earliest_found - 100 * step, 0)
    hi = earliest_found

    while hi - lo > step:
        mid = (lo + hi) // 2
        # Check a single chunk at 'mid' to see if activity exists
        chunk_logs = _rpc_post(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(mid),
            "toBlock": hex(min(mid + 499, hi)),
            "address": usdc_address,
            "topics": [ERC20_TRANSFER_TOPIC, wallet_padded],
        }])
        if chunk_logs:
            hi = mid
        else:
            lo = mid
        time.sleep(0.1)

    # Refine: scan lo..hi in 500-block chunks to find exact earliest
    earliest = hi
    for start in range(lo, hi + 1, 500):
        end = min(start + 499, hi)
        chunk_logs = _rpc_post(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(start), "toBlock": hex(end),
            "address": usdc_address,
            "topics": [ERC20_TRANSFER_TOPIC, wallet_padded],
        }])
        if chunk_logs:
            earliest = min(int(l["blockNumber"], 16) for l in chunk_logs)
            break
        time.sleep(0.05)

    log.info("Activity range: block %d to %d", earliest, latest)
    return earliest, latest


def scan_usdc_transfers(rpc_url, wallet, start_block, end_block, usdc_address=None):
    """
    Scan for all USDC Transfer events involving the wallet.
    Returns a dict of tx_hash -> list of USDC transfer logs.
    Also returns total counts.
    """
    from config import POLYMARKET_CONTRACTS

    if usdc_address is None:
        usdc_address = POLYMARKET_CONTRACTS["usdc_e"]

    wallet_padded = _pad_address(wallet)
    chunk = RPC_CHUNK_SIZE

    tx_to_logs = {}
    total_from = 0
    total_to = 0
    total_chunks = (end_block - start_block) // chunk + 1
    last_progress = time.time()

    for i, start in enumerate(range(start_block, end_block + 1, chunk)):
        end = min(start + chunk - 1, end_block)

        # FROM wallet
        logs_from = _rpc_post(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(start), "toBlock": hex(end),
            "address": usdc_address,
            "topics": [ERC20_TRANSFER_TOPIC, wallet_padded],
        }])

        # TO wallet
        logs_to = _rpc_post(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(start), "toBlock": hex(end),
            "address": usdc_address,
            "topics": [ERC20_TRANSFER_TOPIC, None, wallet_padded],
        }])

        f = len(logs_from)
        t = len(logs_to)
        total_from += f
        total_to += t

        for log_entry in logs_from + logs_to:
            tx_hash = log_entry.get("transactionHash", "")
            if tx_hash:
                if tx_hash not in tx_to_logs:
                    tx_to_logs[tx_hash] = []
                tx_to_logs[tx_hash].append(log_entry)

        # Progress logging every 10 seconds
        now = time.time()
        if now - last_progress >= 10:
            pct = (i + 1) / total_chunks * 100
            log.info("  Scan progress: %d/%d chunks (%.0f%%), %d FROM + %d TO = %d txs",
                     i + 1, total_chunks, pct, total_from, total_to, len(tx_to_logs))
            last_progress = now

        time.sleep(0.05)

    log.info("USDC scan complete: %d FROM, %d TO, %d unique txs",
             total_from, total_to, len(tx_to_logs))
    return tx_to_logs


def scan_all_usdc_contracts(rpc_url, wallet, start_block, end_block):
    """Scan both USDC.e and native USDC contracts."""
    from config import POLYMARKET_CONTRACTS

    all_tx_logs = {}

    for label in ("usdc_e", "usdc_native"):
        addr = POLYMARKET_CONTRACTS[label]
        log.info("Scanning %s (%s)...", label, addr)
        tx_logs = scan_usdc_transfers(rpc_url, wallet, start_block, end_block, addr)

        for tx_hash, logs in tx_logs.items():
            if tx_hash not in all_tx_logs:
                all_tx_logs[tx_hash] = []
            all_tx_logs[tx_hash].extend(logs)

    return all_tx_logs


def scan_usdc_transfers_backward(rpc_url, wallet, end_block, target_txs=0, max_blocks=0, usdc_address=None):
    """
    Scan backward for USDC Transfer events involving the wallet.
    Stops when target_txs is reached or max_blocks have been scanned.
    Returns (tx_hash -> logs, earliest_scanned_block).
    """
    from config import POLYMARKET_CONTRACTS

    if usdc_address is None:
        usdc_address = POLYMARKET_CONTRACTS["usdc_e"]

    wallet_padded = _pad_address(wallet)
    chunk = RPC_CHUNK_SIZE
    min_block = max(end_block - max_blocks + 1, 0) if max_blocks else 0
    tx_to_logs = {}
    total_from = 0
    total_to = 0
    chunks = 0
    last_progress = time.time()
    earliest_scanned = end_block

    block_end = end_block
    while block_end >= min_block:
        block_start = max(block_end - chunk + 1, min_block)
        earliest_scanned = block_start

        logs_from = _rpc_post(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(block_start), "toBlock": hex(block_end),
            "address": usdc_address,
            "topics": [ERC20_TRANSFER_TOPIC, wallet_padded],
        }])

        logs_to = _rpc_post(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(block_start), "toBlock": hex(block_end),
            "address": usdc_address,
            "topics": [ERC20_TRANSFER_TOPIC, None, wallet_padded],
        }])

        total_from += len(logs_from)
        total_to += len(logs_to)
        for log_entry in logs_from + logs_to:
            tx_hash = log_entry.get("transactionHash", "")
            if tx_hash:
                tx_to_logs.setdefault(tx_hash, []).append(log_entry)

        chunks += 1
        now = time.time()
        if now - last_progress >= 10:
            log.info(
                "  Backfill progress: scanned to block %d, %d chunks, %d FROM + %d TO, %d unique txs",
                block_start, chunks, total_from, total_to, len(tx_to_logs),
            )
            last_progress = now

        if target_txs and len(tx_to_logs) >= target_txs:
            break

        block_end = block_start - 1
        time.sleep(0.05)

    log.info(
        "Backward USDC scan complete: %d FROM, %d TO, %d unique txs, earliest scanned block %d",
        total_from, total_to, len(tx_to_logs), earliest_scanned,
    )
    return tx_to_logs, earliest_scanned


def scan_all_usdc_contracts_backward(rpc_url, wallet, end_block, target_txs=0, max_blocks=0):
    """Backward scan both USDC.e and native USDC contracts."""
    from config import POLYMARKET_CONTRACTS

    all_tx_logs = {}
    earliest = end_block

    for label in ("usdc_e", "usdc_native"):
        remaining_target = max(target_txs - len(all_tx_logs), 0) if target_txs else 0
        addr = POLYMARKET_CONTRACTS[label]
        log.info("Backward scanning %s (%s)...", label, addr)
        tx_logs, scanned_to = scan_usdc_transfers_backward(
            rpc_url, wallet, end_block, remaining_target, max_blocks, addr
        )
        earliest = min(earliest, scanned_to)

        for tx_hash, logs in tx_logs.items():
            all_tx_logs.setdefault(tx_hash, []).extend(logs)

        if target_txs and len(all_tx_logs) >= target_txs:
            break

    return all_tx_logs, earliest


def fetch_receipts(rpc_url, tx_hashes, batch_delay=0.05):
    """Fetch transaction receipts for a list of tx hashes."""
    receipts = {}
    for i, tx_hash in enumerate(tx_hashes):
        receipt = _rpc_call(rpc_url, "eth_getTransactionReceipt", [tx_hash])
        if receipt and isinstance(receipt, dict):
            receipts[tx_hash] = receipt
        time.sleep(batch_delay)
        if (i + 1) % 500 == 0:
            log.info("Fetched %d/%d receipts", i + 1, len(tx_hashes))
    log.info("Fetched %d receipts from %d tx hashes", len(receipts), len(tx_hashes))
    return receipts


def decode_usdc_value(data_hex):
    """Decode USDC amount from Transfer event data."""
    if not data_hex or data_hex == "0x":
        return 0.0
    try:
        raw = int(data_hex, 16)
        return raw / (10 ** USDC_DECIMALS)
    except (ValueError, TypeError):
        return 0.0


def extract_address_from_topic(topic):
    """Extract 20-byte address from a 32-byte topic."""
    if not topic or len(topic) < 26:
        return ""
    return "0x" + topic[-40:].lower()
