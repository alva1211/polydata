use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::config::Settings;
use crate::models::{IngestedEvent, MarketInfo, TrackingPlan};
use crate::time::{now_ns, ts_to_utc_string};

struct MarketWorkerHandle {
    market: MarketInfo,
    handle: JoinHandle<()>,
}

pub async fn run_polymarket_ingestion_manager(
    settings: Arc<Settings>,
    mut plan_rx: watch::Receiver<TrackingPlan>,
    ingest_tx: mpsc::Sender<IngestedEvent>,
) -> Result<()> {
    let mut workers: HashMap<String, MarketWorkerHandle> = HashMap::new();

    sync_workers(&settings, &mut workers, &plan_rx.borrow().clone(), &ingest_tx);

    loop {
        if plan_rx.changed().await.is_err() {
            break;
        }

        sync_workers(&settings, &mut workers, &plan_rx.borrow().clone(), &ingest_tx);
    }

    for (_, worker) in workers.drain() {
        worker.handle.abort();
    }

    Ok(())
}

fn sync_workers(
    settings: &Arc<Settings>,
    workers: &mut HashMap<String, MarketWorkerHandle>,
    plan: &TrackingPlan,
    ingest_tx: &mpsc::Sender<IngestedEvent>,
) {
    let desired_by_slug: HashMap<String, MarketInfo> = plan
        .markets
        .iter()
        .cloned()
        .map(|market| (market.market_slug.clone(), market))
        .collect();

    let stale_slugs: Vec<String> = workers
        .keys()
        .filter(|slug| !desired_by_slug.contains_key(*slug))
        .cloned()
        .collect();
    for slug in stale_slugs {
        if let Some(worker) = workers.remove(&slug) {
            info!(market_slug = slug, "stopping polymarket worker");
            worker.handle.abort();
        }
    }

    for (slug, market) in desired_by_slug {
        let restart = workers
            .get(&slug)
            .map(|worker| worker.market != market)
            .unwrap_or(true);

        if !restart {
            continue;
        }

        if let Some(existing) = workers.remove(&slug) {
            existing.handle.abort();
        }

        let worker_settings = settings.clone();
        let worker_ingest_tx = ingest_tx.clone();
        let worker_market = market.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = run_polymarket_market_worker(worker_settings, worker_market.clone(), worker_ingest_tx).await {
                warn!(market_slug = %worker_market.market_slug, error = %err, "polymarket worker exited with error");
            }
        });

        info!(
            market_slug = %market.market_slug,
            market_window = %format_market_window(&market),
            yes_token_id = %market.yes_token_id,
            no_token_id = %market.no_token_id,
            "starting polymarket worker"
        );
        workers.insert(
            slug,
            MarketWorkerHandle {
                market,
                handle,
            },
        );
    }
}

async fn run_polymarket_market_worker(
    settings: Arc<Settings>,
    market: MarketInfo,
    ingest_tx: mpsc::Sender<IngestedEvent>,
) -> Result<()> {
    loop {
        let connect_result = connect_async(&settings.polymarket_ws_url).await;
        let (mut ws_stream, _) = match connect_result {
            Ok(pair) => pair,
            Err(err) => {
                warn!(market_slug = %market.market_slug, error = %err, "polymarket websocket connect failed");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let subscribe_payload = json!({
            "type": "market",
            "assets_ids": [market.yes_token_id.clone(), market.no_token_id.clone()],
            "custom_feature_enabled": true
        });
        ws_stream
            .send(Message::Text(subscribe_payload.to_string().into()))
            .await
            .context("failed to send polymarket subscribe")?;

        info!(market_slug = %market.market_slug, "polymarket websocket subscribed");

        while let Some(message) = ws_stream.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    if let Ok(value) = serde_json::from_str::<Value>(text.as_str()) {
                        for payload in split_payloads(value) {
                            ingest_tx
                                .send(IngestedEvent::Polymarket {
                                    market: market.clone(),
                                    recv_ts_ns: now_ns(),
                                    payload,
                                })
                                .await?;
                        }
                    }
                }
                Ok(Message::Ping(payload)) => {
                    ws_stream.send(Message::Pong(payload)).await?;
                }
                Ok(Message::Close(_)) => break,
                Ok(_) => {}
                Err(err) => {
                    warn!(market_slug = %market.market_slug, error = %err, "polymarket websocket read failed");
                    break;
                }
            }
        }

        warn!(market_slug = %market.market_slug, "polymarket websocket closed, reconnecting");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

pub async fn run_binance_ingestion(
    settings: Arc<Settings>,
    ingest_tx: mpsc::Sender<IngestedEvent>,
) -> Result<()> {
    loop {
        let connect_result = connect_async(&settings.binance_ws_url).await;
        let (mut ws_stream, _) = match connect_result {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = %err, "binance websocket connect failed");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        info!("binance websocket subscribed");

        while let Some(message) = ws_stream.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    if let Ok(value) = serde_json::from_str::<Value>(text.as_str()) {
                        ingest_tx
                            .send(IngestedEvent::Binance {
                                recv_ts_ns: now_ns(),
                                payload: value,
                            })
                            .await?;
                    }
                }
                Ok(Message::Ping(payload)) => {
                    ws_stream.send(Message::Pong(payload)).await?;
                }
                Ok(Message::Close(_)) => break,
                Ok(_) => {}
                Err(err) => {
                    warn!(error = %err, "binance websocket read failed");
                    break;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn split_payloads(value: Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items,
        other => vec![other],
    }
}

fn format_market_window(market: &MarketInfo) -> String {
    let start = market
        .market_end_ts
        .map(|end_ts| end_ts - 300)
        .or(market.market_start_ts)
        .map(ts_to_utc_string)
        .unwrap_or_else(|| "unknown-start".to_string());
    let end = market
        .market_end_ts
        .map(ts_to_utc_string)
        .unwrap_or_else(|| "unknown-end".to_string());
    format!("{} -> {}", start, end)
}
