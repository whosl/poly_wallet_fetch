"""
Polymarket wallet analysis tool configuration.
All contract addresses are on Polygon.
"""

# Target wallet (default)
DEFAULT_WALLET = "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30"

# Polymarket core contracts on Polygon
POLYMARKET_CONTRACTS = {
    "ctf_exchange": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "neg_risk_ctf_exchange": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "conditional_tokens": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
    "neg_risk_conditional_tokens": "0x60Ab063A85244240B8A3E1e4825e7cE2E7727BdE",
    "usdc_e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    "usdc_native": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
    "proxy_wallet": "0x66845F425C00f09AEed00183573156cA564C5E15",
}

# Flatten for quick lookup
POLYMARKET_CONTRACT_ADDRESSES = set(v.lower() for v in POLYMARKET_CONTRACTS.values())

# Polymarket event topic hashes (keccak)
EVENT_TOPICS = {
    # CTF Exchange
    "OrderFilled": "0x885b247a779d0e63e2a7b9e2f7e03f1e9e1b7e0f1e0f1e0f1e0f1e0f1e0f1e0f",  # placeholder, computed below
    # ERC1155
    "TransferSingle": "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
    "TransferBatch": "0x4a39dc06d4c0dbc64b70a903fff4e6e4d2a4e8e8e8e8e8e8e8e8e8e8e8e8e8e8",  # placeholder
    # Conditional Tokens
    "SplitPosition": "0x9ece69e7c7069efa0a2dcdee1e3c6b9e6c7b9e6c7b9e6c7b9e6c7b9e6c7b9e6c",  # placeholder
    "MergePositions": "0x5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e",  # placeholder
    "RedeemPositions": "0x6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e",  # placeholder
}

# API endpoints
POLYGONSCAN_API = "https://api.polygonscan.com/api"
GAMMA_API = "https://gamma-api.polymarket.com"
POLYGON_RPC_URLS = [
    "https://polygon-rpc.com",
    "https://rpc-mainnet.matic.quiknode.pro",
]

# Rate limiting
POLYGONSCAN_DELAY = 0.25  # seconds between API calls (5/sec for free tier)
GAMMA_API_DELAY = 0.2
RPC_DELAY = 0.1

# Pagination
POLYGONSCAN_PAGE_SIZE = 10000  # max rows per call
GAMMA_PAGE_SIZE = 100
DATA_API_PAGE_SIZE = 1000  # actual max per page from data-api.polymarket.com

# RPC scanning
RPC_CHUNK_SIZE = 500  # max blocks per eth_getLogs query (larger values return empty)
RPC_CANDIDATE_URLS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://1rpc.io/matic",
]
BINARY_SEARCH_CHUNK = 10000  # block range for binary search steps

# Output directory
OUTPUT_DIR = "data"

# USDC decimals
USDC_DECIMALS = 6
