"""
ERC20 (USDC) transfer decoder.
Decodes USDC.e and native USDC Transfer events related to Polymarket.
"""

import logging
from config import USDC_DECIMALS

log = logging.getLogger(__name__)

# ERC20 Transfer event topic
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def hex_to_int(hex_str):
    if not hex_str:
        return 0
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return 0


def decode_transfer_log(log_entry):
    """
    Decode an ERC20 Transfer event from a log entry.
    Topics[1] = from, Topics[2] = to
    Data = value (uint256)
    """
    topics = log_entry.get("topics", [])
    data = log_entry.get("data", "0x")

    if len(topics) < 3:
        return None

    try:
        from_addr = "0x" + topics[1][-40:]
        to_addr = "0x" + topics[2][-40:]
        value = int(data, 16) if data and data != "0x" else 0

        return {
            "event": "Transfer",
            "from": from_addr.lower(),
            "to": to_addr.lower(),
            "value_raw": value,
            "value_usdc": value / (10 ** USDC_DECIMALS),
            "contract": log_entry.get("address", "").lower(),
            "log_index": hex_to_int(log_entry.get("logIndex", "0x0")),
            "transaction_hash": log_entry.get("transactionHash", ""),
            "block_number": hex_to_int(log_entry.get("blockNumber", "0x0")),
        }
    except Exception as e:
        log.debug("Failed to decode ERC20 Transfer: %s", e)
        return None


def decode_polygonscan_erc20(transfer):
    """Decode ERC20 transfer from Polygonscan API format."""
    value_raw = int(transfer.get("value", "0")) if transfer.get("value") else 0
    decimals = int(transfer.get("tokenDecimal", "6")) if transfer.get("tokenDecimal") else 6
    return {
        "event": "Transfer",
        "from": transfer.get("from", "").lower(),
        "to": transfer.get("to", "").lower(),
        "value_raw": value_raw,
        "value_usdc": value_raw / (10 ** decimals),
        "contract": transfer.get("contractAddress", "").lower(),
        "transaction_hash": transfer.get("hash", ""),
        "block_number": int(transfer.get("blockNumber", "0")),
        "time_stamp": transfer.get("timeStamp", ""),
        "token_name": transfer.get("tokenName", ""),
        "token_symbol": transfer.get("tokenSymbol", ""),
    }


def is_usdc(contract_address, polymarket_contracts):
    """Check if a contract is a USDC token."""
    addr = contract_address.lower()
    usdc_e = polymarket_contracts.get("usdc_e", "").lower()
    usdc_native = polymarket_contracts.get("usdc_native", "").lower()
    return addr in (usdc_e, usdc_native)
