mod config;
mod discovery;
mod ingestion;
mod models;
mod orderbook;
mod router;
mod time;
mod writer;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::info;

use crate::config::Settings;
use crate::discovery::MarketDiscovery;
use crate::models::{BookCommand, IngestedEvent, TrackingPlan, WriterCommand};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();

    let settings = Arc::new(Settings::from_env());
    let (plan_tx, plan_rx) = watch::channel::<TrackingPlan>(TrackingPlan::default());
    let (ingest_tx, ingest_rx) = mpsc::channel::<IngestedEvent>(10_000);
    let (book_tx, book_rx) = mpsc::channel::<BookCommand>(10_000);
    let (writer_tx, writer_rx) = mpsc::channel::<WriterCommand>(10_000);

    let discovery = MarketDiscovery::new(settings.clone())?;

    let discovery_handle = tokio::spawn(discovery.run(plan_tx, book_tx.clone(), writer_tx.clone()));
    let poly_handle = tokio::spawn(ingestion::run_polymarket_ingestion_manager(
        settings.clone(),
        plan_rx.clone(),
        ingest_tx.clone(),
    ));
    let binance_handle = tokio::spawn(ingestion::run_binance_ingestion(settings.clone(), ingest_tx));
    let router_handle = tokio::spawn(router::run_event_router(ingest_rx, book_tx, writer_tx.clone()));
    let orderbook_handle = tokio::spawn(orderbook::run_orderbook_manager(
        settings.snapshot_interval_secs,
        book_rx,
        writer_tx.clone(),
    ));
    let writer_handle = tokio::spawn(writer::run_parquet_writer(
        settings.data_dir.clone(),
        settings.writer_flush_interval_secs,
        settings.writer_batch_rows,
        writer_rx,
    ));

    info!(data_dir = %settings.data_dir.display(), "collector started");
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");

    let _ = writer_tx.send(WriterCommand::FlushAndShutdown).await;

    for handle in [discovery_handle, poly_handle, binance_handle, router_handle, orderbook_handle] {
        handle.abort();
    }

    let _ = writer_handle.await?;
    Ok(())
}
