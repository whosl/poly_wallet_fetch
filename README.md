# Polymarket Wallet Full Trade Export & Analysis Tool

Export and analyze all Polymarket trading activity for any wallet address.

## Data Sources (Priority Order)

1. **Polymarket Data API** (`data-api.polymarket.com`) - Primary. Returns structured trade data with market metadata.
2. **Polygonscan API** - Secondary. On-chain transaction data. V2 requires API key.
3. **Polygon RPC** - Tertiary. Direct blockchain log scanning. Slow but works without API keys.

## Quick Start

```bash
pip install -r requirements.txt

# Option A: Pre-fetch via Data API (recommended), then analyze
python prefetch_data.py --wallet 0xeebde7a0e019a63e6b476eb425505b7b3e6eba30
python main.py --wallet 0xeebde7a0e019a63e6b476eb425505b7b3e6eba30 --skip-fetch --raw-file data/trades_raw.json

# Option B: Auto-try all sources
python main.py --wallet 0xeebde7a0e019a63e6b476eb425505b7b3e6eba30

# Option C: Use specific source
python main.py --wallet 0x... --source data-api
python main.py --wallet 0x... --source polygonscan --polygonscan-key YOUR_KEY
python main.py --wallet 0x... --source rpc --rpc-url https://polygon.drpc.org
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `--wallet, -w` | Target wallet address |
| `--source, -s` | Data source: auto, data-api, polygonscan, rpc, file |
| `--polygonscan-key, -k` | Polygonscan API key |
| `--output-dir, -o` | Output directory (default: `data/`) |
| `--rpc-url` | Custom Polygon RPC URL |
| `--skip-fetch` | Skip fetching, use existing raw data |
| `--raw-file` | Path to existing raw JSON file |
| `--no-gamma` | Skip Gamma API enrichment |
| `--verbose, -v` | Enable verbose logging |

## Output Files

| File | Description |
|------|-------------|
| `trades_raw.json` | Raw fetched data |
| `trades_normalized.csv` | Standardized per-trade CSV with 20 fields |
| `positions_summary.csv` | Per-market aggregated statistics |
| `daily_summary.csv` | Daily aggregated statistics |
| `report.md` | Comprehensive analysis report |
| `wallet_style_diagnosis.md` | Trading style diagnosis |

## trades_normalized.csv Fields

| # | Field | Description |
|---|-------|-------------|
| 1 | timestamp | Trade timestamp (UTC) |
| 2 | block_number | Polygon block number |
| 3 | tx_hash | Transaction hash |
| 4 | wallet | Wallet address |
| 5 | market_slug | Polymarket market slug |
| 6 | market_question | Market question text |
| 7 | event_slug | Event slug |
| 8 | outcome | YES / NO / UP / DOWN |
| 9 | side | BUY / SELL / SETTLE / DEPOSIT / WITHDRAW |
| 10 | price | Price per share |
| 11 | size | Number of shares |
| 12 | notional | Total value (price * size) |
| 13 | fee | Fee amount |
| 14 | realized_pnl | Realized P&L (FIFO) |
| 15 | position_after | Running position after trade |
| 16 | order_id | Order ID (if available) |
| 17 | trade_id | Generated trade identifier |
| 18 | source | Data source identifier |
| 19 | settlement_value | Settlement/redemption value |
| 20 | notes | Additional notes |

## Project Structure

```
├── main.py                    # Entry point
├── prefetch_data.py           # Pre-fetch via Data API
├── config.py                  # Contracts, APIs, rate limits
├── requirements.txt
├── fetchers/
│   ├── polymarket_data_api.py # Polymarket Data API (primary)
│   ├── polygonscan.py         # Polygonscan API
│   ├── gamma_api.py           # Polymarket Gamma API
│   ├── polymarket_rpc_fetcher.py # Polygon RPC log scanning
│   └── polymarket_api.py      # Polygon RPC helpers
├── decoders/
│   ├── erc1155.py             # ERC1155 transfer decoding
│   ├── erc20.py               # ERC20 USDC decoding
│   ├── conditional_tokens.py  # CTF Exchange decoding
│   └── master_decoder.py      # Coordinates all decoders
├── normalize/
│   └── normalizer.py          # Event to trade record matching
├── analytics/
│   ├── statistics.py          # Overall statistics
│   ├── market_analysis.py     # Per-market analysis
│   ├── time_analysis.py       # Time-based analysis
│   ├── behavior.py            # Behavioral pattern detection
│   └── style.py               # Trading style diagnosis
└── outputs/
    ├── csv_writer.py          # CSV file writers
    └── report.py              # Markdown report generators
```

## Important Notes

- **Polymarket uses proxy wallets**: The EOA only has 1 on-chain tx (proxy deployment). All trading happens through Polymarket's relayer. The Data API resolves this automatically.
- **Data API is the best source**: It returns fully structured trades with market metadata (title, slug, outcome).
- **On-chain data alone is insufficient**: Because trades go through a relayer, scanning ERC1155 events for the EOA yields no results.
- **Realized PnL uses FIFO matching**: This is an approximation.
- **Fee data**: Not always available; often estimated.
- **Metrics are tagged**: `report.md` marks each metric as "Determined" or "Estimated".

## Getting API Keys

- **Polymarket Data API**: No key needed (public endpoints)
- **Polygonscan**: Register at https://polygonscan.com/register, create API key at https://polygonscan.com/myapikey
