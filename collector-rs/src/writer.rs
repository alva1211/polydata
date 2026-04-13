use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc;
use tracing::info;

use crate::models::{
    BinanceEventRow, BookSnapshotRow, MarketMetadataRow, PolymarketMarketEventRow,
    PolymarketRawEventRow, PolymarketTradeRow, WriterCommand,
};
use crate::time::ns_to_dt_string;

#[derive(Debug, Default)]
struct WriterTotals {
    market_metadata: u64,
    polymarket_events_raw: u64,
    binance_events_raw: u64,
    polymarket_market_events: u64,
    polymarket_trades: u64,
    polymarket_book_snapshots: u64,
}

pub async fn run_parquet_writer(
    data_dir: PathBuf,
    flush_interval_secs: u64,
    batch_rows: usize,
    mut writer_rx: mpsc::Receiver<WriterCommand>,
) -> Result<()> {
    let mut metadata_rows = Vec::new();
    let mut polymarket_raw_rows = Vec::new();
    let mut binance_rows = Vec::new();
    let mut market_event_rows = Vec::new();
    let mut trade_rows = Vec::new();
    let mut book_rows = Vec::new();
    let mut totals = WriterTotals::default();
    let mut interval = tokio::time::interval(Duration::from_secs(flush_interval_secs));

    loop {
        tokio::select! {
            command = writer_rx.recv() => {
                let Some(command) = command else {
                    flush_all(
                        &data_dir,
                        &mut metadata_rows,
                        &mut polymarket_raw_rows,
                        &mut binance_rows,
                        &mut market_event_rows,
                        &mut trade_rows,
                        &mut book_rows,
                        &mut totals,
                        "channel_closed",
                    )?;
                    break;
                };

                match command {
                    WriterCommand::MarketMetadata(row) => metadata_rows.push(row),
                    WriterCommand::PolymarketRawEvent(row) => polymarket_raw_rows.push(row),
                    WriterCommand::BinanceEvent(row) => binance_rows.push(row),
                    WriterCommand::PolymarketMarketEvent(row) => market_event_rows.push(row),
                    WriterCommand::PolymarketTrade(row) => trade_rows.push(row),
                    WriterCommand::BookSnapshot(row) => book_rows.push(row),
                    WriterCommand::FlushAndShutdown => {
                        flush_all(
                            &data_dir,
                            &mut metadata_rows,
                            &mut polymarket_raw_rows,
                        &mut binance_rows,
                        &mut market_event_rows,
                        &mut trade_rows,
                        &mut book_rows,
                        &mut totals,
                        "shutdown",
                    )?;
                        break;
                    }
                }

                if metadata_rows.len() >= batch_rows
                    || polymarket_raw_rows.len() >= batch_rows
                    || binance_rows.len() >= batch_rows
                    || market_event_rows.len() >= batch_rows
                    || trade_rows.len() >= batch_rows
                    || book_rows.len() >= batch_rows
                {
                    flush_all(
                        &data_dir,
                        &mut metadata_rows,
                        &mut polymarket_raw_rows,
                        &mut binance_rows,
                        &mut market_event_rows,
                        &mut trade_rows,
                        &mut book_rows,
                        &mut totals,
                        "batch_size",
                    )?;
                }
            }
            _ = interval.tick() => {
                flush_all(
                    &data_dir,
                    &mut metadata_rows,
                    &mut polymarket_raw_rows,
                    &mut binance_rows,
                    &mut market_event_rows,
                    &mut trade_rows,
                    &mut book_rows,
                    &mut totals,
                    "interval",
                )?;
            }
        }
    }

    Ok(())
}

fn flush_all(
    data_dir: &Path,
    metadata_rows: &mut Vec<MarketMetadataRow>,
    polymarket_raw_rows: &mut Vec<PolymarketRawEventRow>,
    binance_rows: &mut Vec<BinanceEventRow>,
    market_event_rows: &mut Vec<PolymarketMarketEventRow>,
    trade_rows: &mut Vec<PolymarketTradeRow>,
    book_rows: &mut Vec<BookSnapshotRow>,
    totals: &mut WriterTotals,
    trigger: &str,
) -> Result<()> {
    if !metadata_rows.is_empty() {
        write_market_metadata(data_dir, metadata_rows)?;
        totals.market_metadata += metadata_rows.len() as u64;
        log_flush("market_metadata", metadata_rows.len(), totals.market_metadata, trigger);
        metadata_rows.clear();
    }
    if !polymarket_raw_rows.is_empty() {
        write_polymarket_raw_events(data_dir, polymarket_raw_rows)?;
        totals.polymarket_events_raw += polymarket_raw_rows.len() as u64;
        log_flush(
            "polymarket_events_raw",
            polymarket_raw_rows.len(),
            totals.polymarket_events_raw,
            trigger,
        );
        polymarket_raw_rows.clear();
    }
    if !binance_rows.is_empty() {
        write_binance_events(data_dir, binance_rows)?;
        totals.binance_events_raw += binance_rows.len() as u64;
        log_flush(
            "binance_events_raw",
            binance_rows.len(),
            totals.binance_events_raw,
            trigger,
        );
        binance_rows.clear();
    }
    if !market_event_rows.is_empty() {
        write_polymarket_market_events(data_dir, market_event_rows)?;
        totals.polymarket_market_events += market_event_rows.len() as u64;
        log_flush(
            "polymarket_market_events",
            market_event_rows.len(),
            totals.polymarket_market_events,
            trigger,
        );
        market_event_rows.clear();
    }
    if !trade_rows.is_empty() {
        write_polymarket_trades(data_dir, trade_rows)?;
        totals.polymarket_trades += trade_rows.len() as u64;
        log_flush(
            "polymarket_trades",
            trade_rows.len(),
            totals.polymarket_trades,
            trigger,
        );
        trade_rows.clear();
    }
    if !book_rows.is_empty() {
        write_book_snapshots(data_dir, book_rows)?;
        totals.polymarket_book_snapshots += book_rows.len() as u64;
        log_flush(
            "polymarket_book_snapshots",
            book_rows.len(),
            totals.polymarket_book_snapshots,
            trigger,
        );
        book_rows.clear();
    }
    Ok(())
}

fn log_flush(table_name: &str, rows: usize, total_rows: u64, trigger: &str) {
    info!(
        table = table_name,
        rows,
        total_rows,
        trigger,
        "parquet flush completed"
    );
}

fn write_market_metadata(data_dir: &Path, rows: &[MarketMetadataRow]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("created_recv_ts_ns", DataType::Int64, false),
        Field::new("updated_recv_ts_ns", DataType::Int64, false),
        Field::new("market_slug", DataType::Utf8, false),
        Field::new("question", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("condition_id", DataType::Utf8, true),
        Field::new("yes_token_id", DataType::Utf8, false),
        Field::new("no_token_id", DataType::Utf8, false),
        Field::new("yes_outcome_label", DataType::Utf8, true),
        Field::new("no_outcome_label", DataType::Utf8, true),
        Field::new("market_start_ts", DataType::Int64, true),
        Field::new("market_end_ts", DataType::Int64, true),
        Field::new("tick_size", DataType::Float64, true),
        Field::new("min_order_size", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
        Field::new("closed", DataType::Boolean, false),
        Field::new("resolved", DataType::Boolean, false),
        Field::new("winner", DataType::Utf8, true),
        Field::new("winning_token_id", DataType::Utf8, true),
        Field::new("resolution_ts", DataType::Int64, true),
        Field::new("neg_risk", DataType::Boolean, true),
        Field::new("event_id", DataType::Utf8, true),
        Field::new("series_id", DataType::Utf8, true),
        Field::new("total_volume", DataType::Float64, true),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.created_recv_ts_ns).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.updated_recv_ts_ns).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(rows.iter().map(|row| row.market_slug.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.question.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.description.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.condition_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.yes_token_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.no_token_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.yes_outcome_label.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.no_outcome_label.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.market_start_ts).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.market_end_ts).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.tick_size).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.min_order_size).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|row| row.active).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|row| row.closed).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|row| row.resolved).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.winner.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.winning_token_id.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.resolution_ts).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|row| row.neg_risk).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.event_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.series_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.total_volume).collect::<Vec<_>>())),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    write_batch(
        table_dir(data_dir, "market_metadata", rows[0].updated_recv_ts_ns)?,
        schema,
        batch,
    )
}

fn write_polymarket_raw_events(data_dir: &Path, rows: &[PolymarketRawEventRow]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("recv_ts_ns", DataType::Int64, false),
        Field::new("source_ts_ms", DataType::Int64, true),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("market_slug", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, true),
        Field::new("token_id", DataType::Utf8, true),
        Field::new("asset_side", DataType::Utf8, true),
        Field::new("book_hash", DataType::Utf8, true),
        Field::new("payload_json", DataType::Utf8, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|row| row.recv_ts_ns).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.source_ts_ms).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.event_type.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.market_slug.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.condition_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.token_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.asset_side.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.book_hash.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.payload_json.as_str()).collect::<Vec<_>>())),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    write_batch(table_dir(data_dir, "polymarket_events_raw", rows[0].recv_ts_ns)?, schema, batch)
}

fn write_binance_events(data_dir: &Path, rows: &[BinanceEventRow]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("recv_ts_ns", DataType::Int64, false),
        Field::new("source_ts_ms", DataType::Int64, true),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("trade_price", DataType::Float64, true),
        Field::new("trade_qty", DataType::Float64, true),
        Field::new("is_buyer_maker", DataType::Boolean, true),
        Field::new("best_bid", DataType::Float64, true),
        Field::new("best_bid_qty", DataType::Float64, true),
        Field::new("best_ask", DataType::Float64, true),
        Field::new("best_ask_qty", DataType::Float64, true),
        Field::new("kline_open", DataType::Float64, true),
        Field::new("kline_high", DataType::Float64, true),
        Field::new("kline_low", DataType::Float64, true),
        Field::new("kline_close", DataType::Float64, true),
        Field::new("kline_volume", DataType::Float64, true),
        Field::new("payload_json", DataType::Utf8, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|row| row.recv_ts_ns).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.source_ts_ms).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.symbol.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.stream.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.event_type.as_str()).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.trade_price).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.trade_qty).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|row| row.is_buyer_maker).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.best_bid).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.best_bid_qty).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.best_ask).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.best_ask_qty).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.kline_open).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.kline_high).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.kline_low).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.kline_close).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.kline_volume).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.payload_json.as_str()).collect::<Vec<_>>())),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    write_batch(table_dir(data_dir, "binance_events_raw", rows[0].recv_ts_ns)?, schema, batch)
}

fn write_polymarket_market_events(data_dir: &Path, rows: &[PolymarketMarketEventRow]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("recv_ts_ns", DataType::Int64, false),
        Field::new("source_ts_ms", DataType::Int64, true),
        Field::new("market_slug", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, true),
        Field::new("token_id", DataType::Utf8, true),
        Field::new("asset_side", DataType::Utf8, true),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Float64, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("best_bid", DataType::Float64, true),
        Field::new("best_ask", DataType::Float64, true),
        Field::new("spread", DataType::Float64, true),
        Field::new("event_hash", DataType::Utf8, true),
        Field::new("old_tick_size", DataType::Float64, true),
        Field::new("new_tick_size", DataType::Float64, true),
        Field::new("winning_asset_id", DataType::Utf8, true),
        Field::new("winning_outcome", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("closed", DataType::Boolean, true),
        Field::new("resolved", DataType::Boolean, true),
        Field::new("payload_json", DataType::Utf8, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|row| row.recv_ts_ns).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.source_ts_ms).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.market_slug.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.condition_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.token_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.asset_side.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.event_type.as_str()).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.price).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.size).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.side.as_deref()).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.best_bid).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.best_ask).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.spread).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.event_hash.as_deref()).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.old_tick_size).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.new_tick_size).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.winning_asset_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.winning_outcome.as_deref()).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|row| row.active).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|row| row.closed).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(rows.iter().map(|row| row.resolved).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.payload_json.as_str()).collect::<Vec<_>>())),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    write_batch(table_dir(data_dir, "polymarket_market_events", rows[0].recv_ts_ns)?, schema, batch)
}

fn write_polymarket_trades(data_dir: &Path, rows: &[PolymarketTradeRow]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("recv_ts_ns", DataType::Int64, false),
        Field::new("source_ts_ms", DataType::Int64, true),
        Field::new("market_slug", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, true),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("asset_side", DataType::Utf8, false),
        Field::new("trade_price", DataType::Float64, false),
        Field::new("trade_size", DataType::Float64, false),
        Field::new("trade_side", DataType::Utf8, true),
        Field::new("transaction_hash", DataType::Utf8, true),
        Field::new("payload_json", DataType::Utf8, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|row| row.recv_ts_ns).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.source_ts_ms).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.market_slug.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.condition_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.token_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.asset_side.as_str()).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.trade_price).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.trade_size).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.trade_side.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.transaction_hash.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.payload_json.as_str()).collect::<Vec<_>>())),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    write_batch(table_dir(data_dir, "polymarket_trades", rows[0].recv_ts_ns)?, schema, batch)
}

fn write_book_snapshots(data_dir: &Path, rows: &[BookSnapshotRow]) -> Result<()> {
    let mut fields = vec![
        Field::new("snapshot_ts_ns", DataType::Int64, false),
        Field::new("snapshot_ts_sec", DataType::Int64, false),
        Field::new("recv_ts_ns", DataType::Int64, false),
        Field::new("source_ts_ms", DataType::Int64, true),
        Field::new("market_slug", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, true),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("asset_side", DataType::Utf8, false),
        Field::new("book_hash", DataType::Utf8, true),
    ];

    for index in 1..=10 {
        fields.push(Field::new(format!("bid_px_l{}", index), DataType::Float64, false));
        fields.push(Field::new(format!("bid_sz_l{}", index), DataType::Float64, false));
        fields.push(Field::new(format!("ask_px_l{}", index), DataType::Float64, false));
        fields.push(Field::new(format!("ask_sz_l{}", index), DataType::Float64, false));
    }

    fields.extend_from_slice(&[
        Field::new("mid_px", DataType::Float64, false),
        Field::new("spread_px", DataType::Float64, false),
        Field::new("spread_bps", DataType::Float64, false),
        Field::new("total_bid_depth_l5", DataType::Float64, false),
        Field::new("total_ask_depth_l5", DataType::Float64, false),
        Field::new("total_bid_depth_l10", DataType::Float64, false),
        Field::new("total_ask_depth_l10", DataType::Float64, false),
    ]);

    let schema = Arc::new(Schema::new(fields));
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|row| row.snapshot_ts_ns).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.snapshot_ts_sec).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.recv_ts_ns).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|row| row.source_ts_ms).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.market_slug.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.condition_id.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.token_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.asset_side.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|row| row.book_hash.as_deref()).collect::<Vec<_>>())),
    ];

    for index in 0..10 {
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.bid_px[index]).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.bid_sz[index]).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.ask_px[index]).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Float64Array::from(
            rows.iter().map(|row| row.ask_sz[index]).collect::<Vec<_>>(),
        )));
    }

    columns.extend_from_slice(&[
        Arc::new(Float64Array::from(rows.iter().map(|row| row.mid_px).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.spread_px).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|row| row.spread_bps).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(
            rows.iter().map(|row| row.total_bid_depth_l5).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|row| row.total_ask_depth_l5).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|row| row.total_bid_depth_l10).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|row| row.total_ask_depth_l10).collect::<Vec<_>>(),
        )),
    ]);

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    write_batch(table_dir(data_dir, "polymarket_book_snapshots", rows[0].snapshot_ts_ns)?, schema, batch)
}

fn table_dir(base_dir: &Path, table_name: &str, ts_ns: i64) -> Result<PathBuf> {
    let path = base_dir.join(table_name).join(format!("dt={}", ns_to_dt_string(ts_ns)));
    fs::create_dir_all(&path)?;
    Ok(path)
}

fn write_batch(dir: PathBuf, schema: Arc<Schema>, batch: RecordBatch) -> Result<()> {
    let file_name = format!(
        "part-{}-{}.parquet",
        chrono::Utc::now().timestamp(),
        chrono::Utc::now().timestamp_subsec_millis()
    );
    let file = File::create(dir.join(file_name))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
