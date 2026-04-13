use anyhow::Result;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::models::{
    BinanceEventRow, BookCommand, IngestedEvent, MarketInfo, MarketMetadataRow,
    PolymarketMarketEventRow, PolymarketRawEventRow, PolymarketTradeRow, WriterCommand,
};

pub async fn run_event_router(
    mut ingest_rx: mpsc::Receiver<IngestedEvent>,
    book_tx: mpsc::Sender<BookCommand>,
    writer_tx: mpsc::Sender<WriterCommand>,
) -> Result<()> {
    while let Some(event) = ingest_rx.recv().await {
        match event {
            IngestedEvent::Polymarket {
                market,
                recv_ts_ns,
                payload,
            } => {
                let event_type = value_as_str(&payload, "event_type").unwrap_or_else(|| "unknown".to_string());
                let token_id = value_as_str(&payload, "asset_id");
                let asset_side = token_id
                    .as_ref()
                    .map(|token| classify_asset_side(token, &market.yes_token_id, &market.no_token_id));
                let source_ts_ms = value_as_i64(&payload, "timestamp");
                let event_hash = value_as_str(&payload, "hash");

                writer_tx
                    .send(WriterCommand::PolymarketRawEvent(PolymarketRawEventRow {
                        recv_ts_ns,
                        source_ts_ms,
                        event_type: event_type.clone(),
                        market_slug: market.market_slug.clone(),
                        condition_id: market.condition_id.clone(),
                        token_id: token_id.clone(),
                        asset_side: asset_side.clone(),
                        book_hash: event_hash.clone(),
                        payload_json: payload.to_string(),
                    }))
                    .await?;

                match event_type.as_str() {
                    "book" => {
                        writer_tx
                            .send(WriterCommand::PolymarketMarketEvent(PolymarketMarketEventRow {
                                recv_ts_ns,
                                source_ts_ms,
                                market_slug: market.market_slug.clone(),
                                condition_id: market.condition_id.clone(),
                                token_id: token_id.clone(),
                                asset_side: asset_side.clone(),
                                event_type: event_type.clone(),
                                price: None,
                                size: None,
                                side: None,
                                best_bid: best_bid_from_book(&payload),
                                best_ask: best_ask_from_book(&payload),
                                spread: spread_from_book(&payload),
                                event_hash: event_hash.clone(),
                                old_tick_size: None,
                                new_tick_size: None,
                                winning_asset_id: None,
                                winning_outcome: None,
                                active: None,
                                closed: None,
                                resolved: None,
                                payload_json: payload.to_string(),
                            }))
                            .await?;

                        book_tx
                            .send(BookCommand::Snapshot {
                                market: market.clone(),
                                recv_ts_ns,
                                payload: payload.clone(),
                            })
                            .await?;
                    }
                    "price_change" => {
                        let best_bid = value_as_f64(&payload, "best_bid");
                        let best_ask = value_as_f64(&payload, "best_ask");
                        let changes = payload
                            .get("changes")
                            .or_else(|| payload.get("price_changes"))
                            .and_then(|item| item.as_array())
                            .cloned()
                            .unwrap_or_default();

                        if changes.is_empty() {
                            writer_tx
                                .send(WriterCommand::PolymarketMarketEvent(PolymarketMarketEventRow {
                                    recv_ts_ns,
                                    source_ts_ms,
                                    market_slug: market.market_slug.clone(),
                                    condition_id: market.condition_id.clone(),
                                    token_id: token_id.clone(),
                                    asset_side: asset_side.clone(),
                                    event_type: event_type.clone(),
                                    price: None,
                                    size: None,
                                    side: None,
                                    best_bid,
                                    best_ask,
                                    spread: spread(best_bid, best_ask),
                                    event_hash: event_hash.clone(),
                                    old_tick_size: None,
                                    new_tick_size: None,
                                    winning_asset_id: None,
                                    winning_outcome: None,
                                    active: None,
                                    closed: None,
                                    resolved: None,
                                    payload_json: payload.to_string(),
                                }))
                                .await?;
                        } else {
                            for change in changes {
                                let change_price = value_as_f64(&change, "price");
                                let change_size = value_as_f64(&change, "size");
                                let change_side = value_as_str(&change, "side")
                                    .or_else(|| value_as_str(&change, "type"))
                                    .unwrap_or_else(|| "UNKNOWN".to_string());

                                writer_tx
                                    .send(WriterCommand::PolymarketMarketEvent(PolymarketMarketEventRow {
                                        recv_ts_ns,
                                        source_ts_ms,
                                        market_slug: market.market_slug.clone(),
                                        condition_id: market.condition_id.clone(),
                                        token_id: token_id.clone(),
                                        asset_side: asset_side.clone(),
                                        event_type: event_type.clone(),
                                        price: change_price,
                                        size: change_size,
                                        side: Some(change_side.clone()),
                                        best_bid,
                                        best_ask,
                                        spread: spread(best_bid, best_ask),
                                        event_hash: event_hash.clone(),
                                        old_tick_size: None,
                                        new_tick_size: None,
                                        winning_asset_id: None,
                                        winning_outcome: None,
                                        active: None,
                                        closed: None,
                                        resolved: None,
                                        payload_json: change.to_string(),
                                    }))
                                    .await?;

                                if let (Some(token_id), Some(asset_side), Some(price), Some(size)) = (
                                    token_id.clone(),
                                    asset_side.clone(),
                                    change_price,
                                    change_size,
                                ) {
                                    book_tx
                                        .send(BookCommand::PriceChange {
                                            market: market.clone(),
                                            recv_ts_ns,
                                            source_ts_ms,
                                            token_id,
                                            asset_side,
                                            price,
                                            size,
                                            side: change_side,
                                            best_bid,
                                            best_ask,
                                            event_hash: event_hash.clone(),
                                        })
                                        .await?;
                                }
                            }
                        }
                    }
                    "best_bid_ask" => {
                        let best_bid = value_as_f64(&payload, "best_bid");
                        let best_ask = value_as_f64(&payload, "best_ask");
                        writer_tx
                            .send(WriterCommand::PolymarketMarketEvent(PolymarketMarketEventRow {
                                recv_ts_ns,
                                source_ts_ms,
                                market_slug: market.market_slug.clone(),
                                condition_id: market.condition_id.clone(),
                                token_id: token_id.clone(),
                                asset_side: asset_side.clone(),
                                event_type: event_type.clone(),
                                price: None,
                                size: None,
                                side: None,
                                best_bid,
                                best_ask,
                                spread: spread(best_bid, best_ask),
                                event_hash: event_hash.clone(),
                                old_tick_size: None,
                                new_tick_size: None,
                                winning_asset_id: None,
                                winning_outcome: None,
                                active: None,
                                closed: None,
                                resolved: None,
                                payload_json: payload.to_string(),
                            }))
                            .await?;
                    }
                    "tick_size_change" => {
                        writer_tx
                            .send(WriterCommand::MarketMetadata(metadata_row_from_event(
                                &market,
                                recv_ts_ns,
                                &payload,
                            )))
                            .await?;
                        writer_tx
                            .send(WriterCommand::PolymarketMarketEvent(PolymarketMarketEventRow {
                                recv_ts_ns,
                                source_ts_ms,
                                market_slug: market.market_slug.clone(),
                                condition_id: market.condition_id.clone(),
                                token_id: token_id.clone(),
                                asset_side: asset_side.clone(),
                                event_type: event_type.clone(),
                                price: None,
                                size: None,
                                side: None,
                                best_bid: None,
                                best_ask: None,
                                spread: None,
                                event_hash: event_hash.clone(),
                                old_tick_size: value_as_f64(&payload, "old_tick_size"),
                                new_tick_size: value_as_f64(&payload, "new_tick_size")
                                    .or_else(|| value_as_f64(&payload, "tick_size")),
                                winning_asset_id: None,
                                winning_outcome: None,
                                active: None,
                                closed: None,
                                resolved: None,
                                payload_json: payload.to_string(),
                            }))
                            .await?;
                    }
                    "market_resolved" | "new_market" => {
                        writer_tx
                            .send(WriterCommand::MarketMetadata(metadata_row_from_event(
                                &market,
                                recv_ts_ns,
                                &payload,
                            )))
                            .await?;
                        writer_tx
                            .send(WriterCommand::PolymarketMarketEvent(PolymarketMarketEventRow {
                                recv_ts_ns,
                                source_ts_ms,
                                market_slug: market.market_slug.clone(),
                                condition_id: market.condition_id.clone(),
                                token_id: token_id.clone(),
                                asset_side: asset_side.clone(),
                                event_type: event_type.clone(),
                                price: None,
                                size: None,
                                side: None,
                                best_bid: None,
                                best_ask: None,
                                spread: None,
                                event_hash: event_hash.clone(),
                                old_tick_size: None,
                                new_tick_size: None,
                                winning_asset_id: value_as_str(&payload, "winning_asset_id")
                                    .or_else(|| value_as_str(&payload, "winner")),
                                winning_outcome: value_as_str(&payload, "winning_outcome"),
                                active: value_as_bool(&payload, "active"),
                                closed: value_as_bool(&payload, "closed"),
                                resolved: value_as_bool(&payload, "resolved").or(Some(event_type == "market_resolved")),
                                payload_json: payload.to_string(),
                            }))
                            .await?;
                    }
                    "last_trade_price" => {
                        if let (Some(token_id), Some(asset_side)) = (token_id.clone(), asset_side.clone()) {
                            writer_tx
                                .send(WriterCommand::PolymarketTrade(PolymarketTradeRow {
                                    recv_ts_ns,
                                    source_ts_ms,
                                    market_slug: market.market_slug.clone(),
                                    condition_id: market.condition_id.clone(),
                                    token_id,
                                    asset_side,
                                    trade_price: value_as_f64(&payload, "price").unwrap_or(0.0),
                                    trade_size: value_as_f64(&payload, "size").unwrap_or(0.0),
                                    trade_side: value_as_str(&payload, "side"),
                                    transaction_hash: value_as_str(&payload, "transaction_hash"),
                                    payload_json: payload.to_string(),
                                }))
                                .await?;
                        }
                    }
                    _ => {}
                }
            }
            IngestedEvent::Binance { recv_ts_ns, payload } => {
                let stream = value_as_str(&payload, "stream")
                    .or_else(|| infer_binance_stream(&payload))
                    .unwrap_or_else(|| "unknown".to_string());
                let data = payload.get("data").cloned().unwrap_or(Value::Null);
                let source_ts_ms = value_as_i64(&data, "E").or_else(|| value_as_i64(&data, "T"));
                let event_type = value_as_str(&data, "e").unwrap_or_else(|| stream.clone());

                writer_tx
                    .send(WriterCommand::BinanceEvent(BinanceEventRow {
                        recv_ts_ns,
                        source_ts_ms,
                        symbol: value_as_str(&data, "s").unwrap_or_else(|| "BTCUSDT".to_string()),
                        stream,
                        event_type,
                        trade_price: value_as_f64(&data, "p"),
                        trade_qty: value_as_f64(&data, "q"),
                        is_buyer_maker: value_as_bool(&data, "m"),
                        best_bid: value_as_f64(&data, "b"),
                        best_bid_qty: value_as_f64(&data, "B"),
                        best_ask: value_as_f64(&data, "a"),
                        best_ask_qty: value_as_f64(&data, "A"),
                        kline_open: data.get("k").and_then(|k| value_as_f64(k, "o")),
                        kline_high: data.get("k").and_then(|k| value_as_f64(k, "h")),
                        kline_low: data.get("k").and_then(|k| value_as_f64(k, "l")),
                        kline_close: data.get("k").and_then(|k| value_as_f64(k, "c")),
                        kline_volume: data.get("k").and_then(|k| value_as_f64(k, "v")),
                        payload_json: payload.to_string(),
                    }))
                    .await?;
            }
        }
    }

    Ok(())
}

fn classify_asset_side(token_id: &str, yes_token_id: &str, no_token_id: &str) -> String {
    if token_id == yes_token_id {
        "YES".to_string()
    } else if token_id == no_token_id {
        "NO".to_string()
    } else {
        "UNKNOWN".to_string()
    }
}

fn best_bid_from_book(payload: &Value) -> Option<f64> {
    payload
        .get("bids")
        .and_then(|value| value.as_array())
        .and_then(|levels| levels.first())
        .and_then(|level| value_as_f64(level, "price"))
}

fn best_ask_from_book(payload: &Value) -> Option<f64> {
    payload
        .get("asks")
        .and_then(|value| value.as_array())
        .and_then(|levels| levels.first())
        .and_then(|level| value_as_f64(level, "price"))
}

fn spread_from_book(payload: &Value) -> Option<f64> {
    spread(best_bid_from_book(payload), best_ask_from_book(payload))
}

fn spread(best_bid: Option<f64>, best_ask: Option<f64>) -> Option<f64> {
    match (best_bid, best_ask) {
        (Some(bid), Some(ask)) if ask >= bid => Some(ask - bid),
        _ => None,
    }
}

fn value_as_str(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|item| match item {
        Value::String(text) => Some(text.to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_as_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|item| match item {
        Value::String(text) => text.parse::<i64>().ok(),
        Value::Number(number) => number.as_i64(),
        _ => None,
    })
}

fn value_as_f64(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|item| match item {
        Value::String(text) => text.parse::<f64>().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn value_as_bool(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(|item| match item {
        Value::Bool(flag) => Some(*flag),
        Value::String(text) => match text.to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    })
}

fn infer_binance_stream(payload: &Value) -> Option<String> {
    let data = payload.get("data")?;
    if data.get("b").is_some() && data.get("a").is_some() {
        return Some("btcusdt@bookTicker".to_string());
    }
    if data.get("p").is_some() && data.get("q").is_some() {
        return Some("btcusdt@aggTrade".to_string());
    }
    if data.get("k").is_some() {
        return Some("btcusdt@kline_1s".to_string());
    }
    None
}

fn metadata_row_from_event(market: &MarketInfo, updated_recv_ts_ns: i64, payload: &Value) -> MarketMetadataRow {
    let mut row = MarketMetadataRow::from_market(market, updated_recv_ts_ns);
    let event_type = value_as_str(payload, "event_type").unwrap_or_default();

    if event_type == "tick_size_change" {
        row.tick_size = value_as_f64(payload, "new_tick_size")
            .or_else(|| value_as_f64(payload, "tick_size"))
            .or(row.tick_size);
    }

    if event_type == "market_resolved" {
        row.resolved = true;
        row.closed = true;
        row.winner = value_as_str(payload, "winning_outcome")
            .or_else(|| value_as_str(payload, "winner"))
            .or(row.winner);
        row.winning_token_id = value_as_str(payload, "winning_asset_id")
            .or_else(|| value_as_str(payload, "winning_token_id"))
            .or(row.winning_token_id);
        row.resolution_ts = value_as_i64(payload, "timestamp").or(row.resolution_ts);
    }

    if event_type == "new_market" {
        row.active = value_as_bool(payload, "active").unwrap_or(row.active);
        row.closed = value_as_bool(payload, "closed").unwrap_or(row.closed);
        row.resolved = value_as_bool(payload, "resolved").unwrap_or(row.resolved);
    }

    row.updated_recv_ts_ns = updated_recv_ts_ns;
    row
}
