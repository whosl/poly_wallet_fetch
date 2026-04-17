"""
RPC-based fetcher for constrained environments.
Scans for Polymarket-related events on-chain via Polygon RPC.
"""

import logging
import requests
import time
import json

from config import POLYMARKET_CONTRACTS

log = logging.getLogger(__name__)

RPC_URLS = [
    "https://polygon.drpc.org",
    "https://1rpc.io/matic",
]

TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _get_rpc(rpc_url):
    """Get a working RPC URL."""
    for url in [rpc_url] + RPC_URLS:
        if not url:
            continue
        try:
            payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200 and "result" in r.json():
                return url
        except Exception:
            continue
    return None


def _rpc_call(url, method, params, timeout=60):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        data = r.json()
        return data.get("result", [])
    except Exception as e:
        log.debug("RPC call %s failed: %s", method, e)
        return []


def fetch_via_rpc(wallet, rpc_url=None):
    """
    Fetch all Polymarket-related events for a wallet via RPC log scanning.
    Scans both the wallet address and any discovered proxy wallet.
    """
    url = _get_rpc(rpc_url)
    if not url:
        log.error("No working RPC found")
        return None

    log.info("Using RPC: %s", url)
    wallet = wallet.lower()
    wallet_padded = "0x" + "0" * 24 + wallet[2:]

    # Get latest block
    latest = int(_rpc_call(url, "eth_blockNumber", []), 16) if _rpc_call(url, "eth_blockNumber", []) else 0
    if not latest:
        log.error("Could not get latest block")
        return None
    log.info("Latest block: %d", latest)

    result = {
        "erc1155_transfers": [],
        "erc20_transfers": [],
        "normal_txs": [],
        "internal_txs": [],
        "proxy_wallet": None,
    }

    # Try to find proxy wallet
    proxy = _find_proxy_wallet(url, wallet)
    addresses_to_scan = [wallet]
    if proxy and proxy != wallet:
        addresses_to_scan.append(proxy)
        result["proxy_wallet"] = proxy
        log.info("Found proxy wallet: %s", proxy)

    # Scan for events
    for addr in addresses_to_scan:
        addr_padded = "0x" + "0" * 24 + addr[2:]
        log.info("Scanning events for %s", addr)

        # Scan Conditional Tokens for ERC1155 transfers
        ct_addresses = [
            POLYMARKET_CONTRACTS["conditional_tokens"],
            POLYMARKET_CONTRACTS["neg_risk_conditional_tokens"],
        ]
        for ct_addr in ct_addresses:
            log.info("Scanning %s for transfers to/from %s", ct_addr[:10] + "...", addr[:10] + "...")
            # Scan in chunks
            chunk = 5000000
            for start in range(0, latest, chunk):
                end = min(start + chunk, latest)
                # TO wallet
                logs = _rpc_call(url, "eth_getLogs", [{
                    "fromBlock": hex(start), "toBlock": hex(end),
                    "address": ct_addr,
                    "topics": [TRANSFER_SINGLE_TOPIC, None, None, addr_padded]
                }])
                if logs:
                    result["erc1155_transfers"].extend(logs)
                    log.info("  Found %d TransferSingle TO at blocks %d-%d", len(logs), start, end)

                # FROM wallet
                logs2 = _rpc_call(url, "eth_getLogs", [{
                    "fromBlock": hex(start), "toBlock": hex(end),
                    "address": ct_addr,
                    "topics": [TRANSFER_SINGLE_TOPIC, None, addr_padded]
                }])
                if logs2:
                    result["erc1155_transfers"].extend(logs2)
                    log.info("  Found %d TransferSingle FROM at blocks %d-%d", len(logs2), start, end)

        # Scan for USDC transfers
        for usdc_addr in [POLYMARKET_CONTRACTS["usdc_e"], POLYMARKET_CONTRACTS["usdc_native"]]:
            for start in range(0, latest, chunk):
                end = min(start + chunk, latest)
                logs = _rpc_call(url, "eth_getLogs", [{
                    "fromBlock": hex(start), "toBlock": hex(end),
                    "address": usdc_addr,
                    "topics": [ERC20_TRANSFER_TOPIC, addr_padded]
                }])
                if logs:
                    result["erc20_transfers"].extend(logs)

                logs2 = _rpc_call(url, "eth_getLogs", [{
                    "fromBlock": hex(start), "toBlock": hex(end),
                    "address": usdc_addr,
                    "topics": [ERC20_TRANSFER_TOPIC, None, addr_padded]
                }])
                if logs2:
                    result["erc20_transfers"].extend(logs2)

    log.info("RPC scan results: %d ERC1155, %d ERC20 transfers",
             len(result["erc1155_transfers"]), len(result["erc20_transfers"]))
    return result


def _find_proxy_wallet(url, wallet):
    """Try to find the Polymarket proxy wallet for an EOA."""
    # Try known factory contracts
    factories = [
        "0x66845F425C00f09AEed00183573156cA564C5E15",  # ProxyWalletFactory
    ]
    # Try proxyAddressMapping(address)
    selector = "0x2dd6ed0b"
    wallet_param = "0x" + "0" * 24 + wallet[2:].lower()

    for factory in factories:
        data = selector + wallet_param[2:]
        result = _rpc_call(url, "eth_call", [{
            "to": factory, "data": data
        }, "latest"])

        if result and len(result) >= 66:
            proxy = "0x" + result[-40:]
            if proxy != "0x" + "0" * 40:
                return proxy
    return None
