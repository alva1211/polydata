# collector-rs

Rust data collector for Polymarket + Binance market data.

## Current Scope

This collector currently implements the first-stage ingestion pipeline:

- `Module A`: market discovery
- `Module B`: websocket ingestion
- `Module C`: event router
- `Module D`: local orderbook manager
- `Module E`: parquet writer

Tables currently produced:

- `market_metadata`
- `polymarket_events_raw`
- `polymarket_market_events`
- `binance_events_raw`
- `polymarket_trades`
- `polymarket_book_snapshots`

## What It Does

- tracks BTC 5-minute Polymarket markets by time bucket slug (`btc-updown-5m-<start_ts>`)
- on startup, skips the currently-open 5-minute market and starts recording from the next bucket
- pre-subscribes the following bucket shortly before the tracked bucket ends so both markets can be captured in parallel
- bootstraps YES/NO books from CLOB REST
- continuously appends enriched `market_metadata` rows with lifecycle updates
- runs one Polymarket websocket worker per tracked market so rollover does not depend on a single reconnect
- subscribes to Binance BTCUSDT `aggTrade` + `bookTicker` + optional `kline_1s`
- stamps every inbound message with local `recv_ts_ns`
- writes immutable Polymarket raw events to parquet
- writes normalized Binance event rows to parquet
- writes normalized Polymarket market-event rows to parquet
- writes enriched market metadata including start/end, outcome labels, resolution fields, and created/updated timestamps
- maintains a local per-token orderbook from `book + price_change`
- exports top-10 orderbook snapshots every second

## Environment Variables

The collector reuses the same `.env` file convention as the Python app.

Important variables:

- `POLYMARKET_GAMMA_URL`
- `POLYMARKET_CLOB_URL`
- `POLYMARKET_WS_URL`
- `BINANCE_WS_URL`
- `POLY_MARKET_QUERY`
- `POLY_MARKET_SLUG`
- `POLY_YES_TOKEN_ID`
- `POLY_NO_TOKEN_ID`

Collector-specific variables:

- `COLLECTOR_DISCOVERY_INTERVAL_SECS`
- `COLLECTOR_SNAPSHOT_INTERVAL_SECS`
- `COLLECTOR_PRE_SUBSCRIBE_SECONDS`
- `COLLECTOR_WRITER_FLUSH_INTERVAL_SECS`
- `COLLECTOR_WRITER_BATCH_ROWS`
- `COLLECTOR_DATA_DIR`

Defaults:

```env
COLLECTOR_DISCOVERY_INTERVAL_SECS=20
COLLECTOR_SNAPSHOT_INTERVAL_SECS=1
COLLECTOR_PRE_SUBSCRIBE_SECONDS=30
COLLECTOR_WRITER_FLUSH_INTERVAL_SECS=5
COLLECTOR_WRITER_BATCH_ROWS=500
COLLECTOR_DATA_DIR=./data/collector
```

## Run

```powershell
cd C:\Users\Administrator\source\repos\PolyTrader\PolyTrader.Python\collector-rs
cargo run --target x86_64-pc-windows-msvc
```

## Output Layout

Parquet files are written under:

```text
data/collector/
  market_metadata/dt=YYYY-MM-DD/
  polymarket_events_raw/dt=YYYY-MM-DD/
  polymarket_market_events/dt=YYYY-MM-DD/
  binance_events_raw/dt=YYYY-MM-DD/
  polymarket_trades/dt=YYYY-MM-DD/
  polymarket_book_snapshots/dt=YYYY-MM-DD/
```

## Important Limitation Right Now

This is an MVP ingestion layer.

Not implemented yet:

- user websocket / live execution response capture
- exchange-order / fill / cancel ledgers for `live_execution_logs`
- `market_snapshots_1s`
- `execution_labels`
- partitioning by `market_slug`
- writer-side file rolling and compaction policy
- reconnect checksum validation and automatic REST recovery on hash mismatch

## Recommended Next Step

Build the next layer in this order:

1. derive `market_snapshots_1s`
2. build replay simulator and `execution_labels`
3. add user-channel and order-response capture for `live_execution_logs`
4. add replay validation against collected book history
