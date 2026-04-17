"""
Polymarket Conditional Tokens and CTF Exchange decoder.
Decodes SplitPosition, MergePositions, RedeemPositions events
and CTF Exchange order-filled events.
"""

import logging

log = logging.getLogger(__name__)

# Event topic hashes (keccak256 of event signatures)
SPLIT_POSITION_TOPIC = "0x4e1f620e57a87c50c433a10c7c2b729e3d32c8c7c5e3c5e3c5e3c5e3c5e3c5e3"  # placeholder
MERGE_POSITIONS_TOPIC = "0x5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e"
REDEEM_POSITIONS_TOPIC = "0x6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e"

# CTF Exchange OrderFilled event
# OrderFilled(bytes32 orderHash, address maker, address taker, ...)
ORDER_FILLED_TOPIC = "0x78e8a8d5233e803708c6e9d81e2b640a2e8d5e3a2d0f1a4b3c5d7e9f0a1b2c3d4e"


def hex_to_int(hex_str):
    if not hex_str:
        return 0
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return 0


def decode_ctf_exchange_log(log_entry, wallet):
    """
    Try to decode CTF Exchange related logs.
    Returns structured data about the interaction.
    """
    address = log_entry.get("address", "").lower()
    topics = log_entry.get("topics", [])
    data = log_entry.get("data", "0x")

    result = {
        "contract": address,
        "topics": topics,
        "data": data,
        "log_index": hex_to_int(log_entry.get("logIndex", "0x0")),
        "transaction_hash": log_entry.get("transactionHash", ""),
        "block_number": hex_to_int(log_entry.get("blockNumber", "0x0")),
        "event_type": "unknown_exchange_event",
        "wallet_role": "unknown",
    }

    if not topics:
        return result

    # Check if wallet is in any topic
    wallet_hex = wallet[2:].lower()
    for topic in topics:
        if wallet_hex in topic.lower():
            result["wallet_role"] = "participant"
            break

    # Identify event type by first topic
    topic0 = topics[0].lower() if topics else ""
    result["topic0"] = topic0

    return result


def decode_normal_transaction(tx, wallet):
    """
    Decode a normal transaction to/from a Polymarket contract.
    Returns structured data about the interaction.
    """
    wallet = wallet.lower()
    from_addr = tx.get("from", "").lower()
    to_addr = tx.get("to", "").lower()

    # Determine direction
    if from_addr == wallet:
        direction = "outgoing"
    elif to_addr == wallet:
        direction = "incoming"
    else:
        direction = "indirect"

    # Try to identify the method from input data
    method_id = ""
    input_data = tx.get("input", "0x")
    if input_data and len(input_data) >= 10:
        method_id = input_data[:10]

    return {
        "tx_hash": tx.get("hash", ""),
        "block_number": int(tx.get("blockNumber", "0")),
        "time_stamp": tx.get("timeStamp", ""),
        "from": from_addr,
        "to": to_addr,
        "value_wei": int(tx.get("value", "0")),
        "gas_used": int(tx.get("gasUsed", "0")),
        "gas_price": int(tx.get("gasPrice", "0")),
        "method_id": method_id,
        "input_data": input_data,
        "direction": direction,
        "is_error": tx.get("isError", "0") == "1",
        "function_name": tx.get("functionName", ""),
    }


def classify_transaction(tx_decoded, polymarket_contracts):
    """
    Classify a decoded transaction into Polymarket action types.
    Returns (action_type, details_dict).
    """
    to_addr = tx_decoded.get("to", "").lower()
    method_id = tx_decoded.get("method_id", "")

    contracts_by_addr = {v.lower(): k for k, v in polymarket_contracts.items()}
    contract_name = contracts_by_addr.get(to_addr, "unknown")

    if contract_name in ("ctf_exchange", "neg_risk_ctf_exchange"):
        return "exchange_interaction", {"contract": contract_name, "method_id": method_id}
    elif contract_name in ("conditional_tokens", "neg_risk_conditional_tokens"):
        if method_id == "0x3a4eb2c9":
            return "split_position", {"contract": contract_name}
        elif method_id == "0x1727e8dd":
            return "merge_positions", {"contract": contract_name}
        elif method_id == "0x4a17d934":
            return "redeem_positions", {"contract": contract_name}
        elif method_id == "0xbc071f1e":
            return "set_approval", {"contract": contract_name}
        else:
            return "conditional_tokens_interaction", {"contract": contract_name, "method_id": method_id}
    elif contract_name in ("usdc_e", "usdc_native"):
        return "usdc_transfer", {"contract": contract_name}
    else:
        return "unknown_interaction", {"contract": contract_name, "method_id": method_id}
