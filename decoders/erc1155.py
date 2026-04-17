"""
ERC1155 transfer decoder.
Decodes TransferSingle and TransferBatch events for Polymarket conditional tokens.
"""

import logging
from config import USDC_DECIMALS

log = logging.getLogger(__name__)

# ERC1155 TransferSingle event topic
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
# ERC1155 TransferBatch event topic
TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70a903fff4e6e4d2a4e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8"


def hex_to_int(hex_str):
    """Convert hex string to int, safely."""
    if not hex_str:
        return 0
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return 0


def decode_transfer_single(log_entry):
    """
    Decode an ERC1155 TransferSingle event.
    Topics[1] = operator, Topics[2] = from, Topics[3] = to
    Data = id (uint256) + value (uint256)
    """
    topics = log_entry.get("topics", [])
    data = log_entry.get("data", "0x")

    if len(topics) < 4:
        return None

    try:
        operator = "0x" + topics[1][-40:]
        from_addr = "0x" + topics[2][-40:]
        to_addr = "0x" + topics[3][-40:]

        # Data is packed: id (32 bytes) + value (32 bytes)
        data_clean = data.replace("0x", "")
        if len(data_clean) < 128:
            return None

        token_id = int(data_clean[:64], 16)
        value = int(data_clean[64:128], 16)

        return {
            "event": "TransferSingle",
            "operator": operator.lower(),
            "from": from_addr.lower(),
            "to": to_addr.lower(),
            "token_id": str(token_id),
            "value": value,
            "contract": log_entry.get("address", "").lower(),
            "log_index": hex_to_int(log_entry.get("logIndex", "0x0")),
            "transaction_hash": log_entry.get("transactionHash", ""),
            "block_number": hex_to_int(log_entry.get("blockNumber", "0x0")),
        }
    except Exception as e:
        log.debug("Failed to decode TransferSingle: %s", e)
        return None


def decode_transfer_batch(log_entry):
    """
    Decode an ERC1155 TransferBatch event.
    Topics[1] = operator, Topics[2] = from, Topics[3] = to
    Data = ids[] + values[]
    """
    topics = log_entry.get("topics", [])
    data = log_entry.get("data", "0x")

    if len(topics) < 4:
        return []

    try:
        operator = "0x" + topics[1][-40:]
        from_addr = "0x" + topics[2][-40:]
        to_addr = "0x" + topics[3][-40:]

        data_clean = data.replace("0x", "")
        # Dynamic arrays: offset(32) + count(32) + ids(32*count) + offset(32) + count(32) + values(32*count)
        offset1 = int(data_clean[:64], 16)
        count = int(data_clean[offset1 * 2: offset1 * 2 + 64], 16)

        results = []
        ids_start = offset1 * 2 + 64
        for i in range(count):
            token_id = int(data_clean[ids_start + i * 64: ids_start + i * 64 + 64], 16)

            # Find values offset
            offset2_pos = ids_start + count * 64
            offset2 = int(data_clean[offset2_pos: offset2_pos + 64], 16)
            count2 = int(data_clean[offset2 * 2: offset2 * 2 + 64], 16)
            values_start = offset2 * 2 + 64

            value = int(data_clean[values_start + i * 64: values_start + i * 64 + 64], 16)

            results.append({
                "event": "TransferBatch",
                "operator": operator.lower(),
                "from": from_addr.lower(),
                "to": to_addr.lower(),
                "token_id": str(token_id),
                "value": value,
                "contract": log_entry.get("address", "").lower(),
                "log_index": hex_to_int(log_entry.get("logIndex", "0x0")),
                "transaction_hash": log_entry.get("transactionHash", ""),
                "block_number": hex_to_int(log_entry.get("blockNumber", "0x0")),
            })
        return results
    except Exception as e:
        log.debug("Failed to decode TransferBatch: %s", e)
        return []


def decode_polygonscan_erc1155(transfer):
    """
    Decode ERC1155 transfer data from Polygonscan's API format.
    Polygonscan returns already-decoded fields.
    """
    return {
        "event": "TransferSingle",
        "from": transfer.get("from", "").lower(),
        "to": transfer.get("to", "").lower(),
        "token_id": transfer.get("tokenID", ""),
        "value": int(transfer.get("tokenValue", "0")) if transfer.get("tokenValue") else 0,
        "contract": transfer.get("contractAddress", "").lower(),
        "transaction_hash": transfer.get("hash", ""),
        "block_number": int(transfer.get("blockNumber", "0")),
        "time_stamp": transfer.get("timeStamp", ""),
        "log_index": int(transfer.get("logIndex", "0")) if transfer.get("logIndex") else 0,
    }


def is_polymarket_conditional_token(contract_address, polymarket_contracts):
    """Check if a contract is a Polymarket conditional token contract."""
    addr = contract_address.lower()
    ct = polymarket_contracts.get("conditional_tokens", "").lower()
    neg_ct = polymarket_contracts.get("neg_risk_conditional_tokens", "").lower()
    return addr in (ct, neg_ct)
