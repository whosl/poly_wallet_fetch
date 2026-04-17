"""
Master decoder: coordinates all decoders to produce unified decoded output.
"""

import logging
from config import POLYMARKET_CONTRACTS
from decoders.erc1155 import (
    decode_polygonscan_erc1155,
    is_polymarket_conditional_token,
)
from decoders.erc20 import (
    decode_polygonscan_erc20,
    is_usdc,
)
from decoders.conditional_tokens import (
    decode_normal_transaction,
    classify_transaction,
)

log = logging.getLogger(__name__)


class MasterDecoder:
    """Coordinates decoding of all raw data into structured events."""

    def __init__(self, wallet):
        self.wallet = wallet.lower()
        self.contracts = POLYMARKET_CONTRACTS
        self.contract_addrs = set(v.lower() for v in POLYMARKET_CONTRACTS.values())

    def decode_all(self, raw_data):
        """
        Decode all raw data from Polygonscan into structured events.
        Returns a list of decoded events sorted by timestamp.
        """
        events = []

        # 1. Decode ERC1155 transfers (conditional tokens = positions)
        for tx in raw_data.get("erc1155_transfers", []):
            decoded = decode_polygonscan_erc1155(tx)
            decoded["_category"] = "erc1155_transfer"
            decoded["_is_polymarket"] = is_polymarket_conditional_token(
                decoded.get("contract", ""), self.contracts
            )
            events.append(decoded)

        # 2. Decode ERC20 transfers (USDC movements)
        for tx in raw_data.get("erc20_transfers", []):
            decoded = decode_polygonscan_erc20(tx)
            decoded["_category"] = "erc20_transfer"
            decoded["_is_usdc"] = is_usdc(
                decoded.get("contract", ""), self.contracts
            )
            # Only include USDC-related transfers
            if decoded["_is_usdc"]:
                events.append(decoded)

        # 3. Decode normal transactions to Polymarket contracts
        for tx in raw_data.get("normal_txs", []):
            to_addr = tx.get("to", "").lower()
            from_addr = tx.get("from", "").lower()
            # Only include txs involving Polymarket contracts
            if to_addr in self.contract_addrs or from_addr == self.wallet:
                decoded = decode_normal_transaction(tx, self.wallet)
                if to_addr in self.contract_addrs:
                    action_type, details = classify_transaction(decoded, self.contracts)
                    decoded["_action_type"] = action_type
                    decoded["_details"] = details
                    decoded["_category"] = "polymarket_tx"
                    events.append(decoded)

        # Sort by block_number (timestamp)
        events.sort(key=lambda e: int(e.get("block_number", 0) or 0))

        log.info("Decoded %d total events", len(events))
        return events

    def decode_receipt_logs(self, receipt, tx_hash=""):
        """
        Decode all logs from a transaction receipt.
        Returns structured events for each decoded log.
        """
        from decoders.erc1155 import decode_transfer_single, decode_transfer_batch
        from decoders.erc20 import decode_transfer_log

        events = []
        logs = receipt.get("logs", [])

        for log_entry in logs:
            address = log_entry.get("address", "").lower()
            topics = log_entry.get("topics", [])

            if not topics:
                continue

            topic0 = topics[0].lower()

            # ERC20 Transfer
            if topic0 == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
                decoded = decode_transfer_log(log_entry)
                if decoded and is_usdc(decoded["contract"], self.contracts):
                    decoded["_category"] = "erc20_transfer"
                    decoded["_is_usdc"] = True
                    events.append(decoded)

            # ERC1155 TransferSingle
            elif topic0 == "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62":
                decoded = decode_transfer_single(log_entry)
                if decoded:
                    decoded["_category"] = "erc1155_transfer"
                    decoded["_is_polymarket"] = is_polymarket_conditional_token(
                        decoded["contract"], self.contracts
                    )
                    events.append(decoded)

            # ERC1155 TransferBatch
            elif topic0 == "0x4a39dc06d4c0dbc64b70a903fff4e6e4d2a4e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8":
                decoded_list = decode_transfer_batch(log_entry)
                for decoded in decoded_list:
                    decoded["_category"] = "erc1155_transfer"
                    decoded["_is_polymarket"] = is_polymarket_conditional_token(
                        decoded["contract"], self.contracts
                    )
                    events.append(decoded)

        return events
