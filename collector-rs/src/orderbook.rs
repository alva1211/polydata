use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::Result;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::models::{BookCommand, BookSnapshotRow, WriterCommand};
use crate::time::now_ns;

type PriceKey = i64;

struct LocalBook {
    market_slug: String,
    condition_id: Option<String>,
    token_id: String,
    asset_side: String,
    book_hash: Option<String>,
    source_ts_ms: Option<i64>,
    recv_ts_ns: i64,
    bids: BTreeMap<PriceKey, f64>,
    asks: BTreeMap<PriceKey, f64>,
}

pub async fn run_orderbook_manager(
    snapshot_interval_secs: u64,
    mut book_rx: mpsc::Receiver<BookCommand>,
    writer_tx: mpsc::Sender<WriterCommand>,
) -> Result<()> {
    let mut books: HashMap<String, LocalBook> = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(snapshot_interval_secs));

    loop {
        tokio::select! {
            command = book_rx.recv() => {
                let Some(command) = command else {
                    break;
                };

                match command {
                    BookCommand::Snapshot { market, recv_ts_ns, payload } => {
                        let Some(token_id) = value_as_str(&payload, "asset_id") else {
                            continue;
                        };
                        let asset_side = value_as_str(&payload, "asset_side")
                            .or_else(|| {
                                if token_id == market.yes_token_id {
                                    Some("YES".to_string())
                                } else if token_id == market.no_token_id {
                                    Some("NO".to_string())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_else(|| "UNKNOWN".to_string());

                        let entry = books
                            .entry(book_key(&market.market_slug, &token_id))
                            .or_insert_with(|| empty_book(&market.market_slug, market.condition_id.clone(), &token_id, &asset_side, recv_ts_ns));

                        entry.market_slug = market.market_slug;
                        entry.condition_id = market.condition_id;
                        entry.asset_side = asset_side;
                        entry.book_hash = value_as_str(&payload, "hash");
                        entry.source_ts_ms = value_as_i64(&payload, "timestamp");
                        entry.recv_ts_ns = recv_ts_ns;
                        entry.bids = parse_ladder(payload.get("bids"));
                        entry.asks = parse_ladder(payload.get("asks"));
                    }
                    BookCommand::PriceChange {
                        market,
                        recv_ts_ns,
                        source_ts_ms,
                        token_id,
                        asset_side,
                        price,
                        size,
                        side,
                        best_bid,
                        best_ask,
                        event_hash,
                    } => {
                        let entry = books
                            .entry(book_key(&market.market_slug, &token_id))
                            .or_insert_with(|| empty_book(&market.market_slug, market.condition_id.clone(), &token_id, &asset_side, recv_ts_ns));

                        entry.market_slug = market.market_slug;
                        entry.condition_id = market.condition_id;
                        entry.asset_side = asset_side;
                        entry.source_ts_ms = source_ts_ms;
                        entry.recv_ts_ns = recv_ts_ns;
                        entry.book_hash = event_hash;

                        apply_price_change(entry, price, size, &side);
                        reconcile_top_of_book(entry, best_bid, best_ask);
                    }
                }
            }
            _ = interval.tick() => {
                let snapshot_ts_ns = now_ns();
                let snapshot_ts_sec = snapshot_ts_ns / 1_000_000_000;

                for book in books.values() {
                    if book.bids.is_empty() || book.asks.is_empty() {
                        continue;
                    }

                    writer_tx.send(WriterCommand::BookSnapshot(to_snapshot_row(
                        book,
                        snapshot_ts_ns,
                        snapshot_ts_sec,
                    ))).await?;
                }
            }
        }
    }

    Ok(())
}

fn empty_book(
    market_slug: &str,
    condition_id: Option<String>,
    token_id: &str,
    asset_side: &str,
    recv_ts_ns: i64,
) -> LocalBook {
    LocalBook {
        market_slug: market_slug.to_string(),
        condition_id,
        token_id: token_id.to_string(),
        asset_side: asset_side.to_string(),
        book_hash: None,
        source_ts_ms: None,
        recv_ts_ns,
        bids: BTreeMap::new(),
        asks: BTreeMap::new(),
    }
}

fn book_key(market_slug: &str, token_id: &str) -> String {
    format!("{}:{}", market_slug, token_id)
}

fn apply_price_change(book: &mut LocalBook, price: f64, size: f64, side: &str) {
    let key = price_to_key(price);
    let target = if side.eq_ignore_ascii_case("BUY") || side.eq_ignore_ascii_case("BID") {
        &mut book.bids
    } else if side.eq_ignore_ascii_case("SELL") || side.eq_ignore_ascii_case("ASK") {
        &mut book.asks
    } else {
        return;
    };

    if size <= 0.0 {
        target.remove(&key);
    } else {
        target.insert(key, size);
    }
}

fn reconcile_top_of_book(book: &mut LocalBook, best_bid: Option<f64>, best_ask: Option<f64>) {
    if let Some(best_bid) = best_bid {
        trim_crossed_side(&mut book.bids, true, best_bid);
    }
    if let Some(best_ask) = best_ask {
        trim_crossed_side(&mut book.asks, false, best_ask);
    }

    while let (Some((bid_key, _)), Some((ask_key, _))) = (book.bids.last_key_value(), book.asks.first_key_value()) {
        if bid_key < ask_key {
            break;
        }
        let crossed_bid = *bid_key;
        book.bids.remove(&crossed_bid);
    }
}

fn trim_crossed_side(levels: &mut BTreeMap<PriceKey, f64>, is_bid: bool, best_px: f64) {
    let threshold = price_to_key(best_px);
    let stale_prices: Vec<PriceKey> = if is_bid {
        levels
            .keys()
            .copied()
            .filter(|price_key| *price_key > threshold)
            .collect()
    } else {
        levels
            .keys()
            .copied()
            .filter(|price_key| *price_key < threshold)
            .collect()
    };

    for price_key in stale_prices {
        levels.remove(&price_key);
    }
}

fn to_snapshot_row(book: &LocalBook, snapshot_ts_ns: i64, snapshot_ts_sec: i64) -> BookSnapshotRow {
    let mut bid_px = [0.0; 10];
    let mut bid_sz = [0.0; 10];
    let mut ask_px = [0.0; 10];
    let mut ask_sz = [0.0; 10];

    for (index, (price_key, size)) in book.bids.iter().rev().take(10).enumerate() {
        bid_px[index] = key_to_price(*price_key);
        bid_sz[index] = *size;
    }
    for (index, (price_key, size)) in book.asks.iter().take(10).enumerate() {
        ask_px[index] = key_to_price(*price_key);
        ask_sz[index] = *size;
    }

    let best_bid = bid_px[0];
    let best_ask = ask_px[0];
    let mid_px = if best_bid > 0.0 && best_ask > 0.0 {
        (best_bid + best_ask) / 2.0
    } else {
        0.0
    };
    let spread_px = if best_bid > 0.0 && best_ask > 0.0 {
        best_ask - best_bid
    } else {
        0.0
    };
    let spread_bps = if mid_px > 0.0 {
        spread_px / mid_px * 10_000.0
    } else {
        0.0
    };

    BookSnapshotRow {
        snapshot_ts_ns,
        snapshot_ts_sec,
        recv_ts_ns: book.recv_ts_ns,
        source_ts_ms: book.source_ts_ms,
        market_slug: book.market_slug.clone(),
        condition_id: book.condition_id.clone(),
        token_id: book.token_id.clone(),
        asset_side: book.asset_side.clone(),
        book_hash: book.book_hash.clone(),
        bid_px,
        bid_sz,
        ask_px,
        ask_sz,
        mid_px,
        spread_px,
        spread_bps,
        total_bid_depth_l5: bid_sz.iter().take(5).sum(),
        total_ask_depth_l5: ask_sz.iter().take(5).sum(),
        total_bid_depth_l10: bid_sz.iter().sum(),
        total_ask_depth_l10: ask_sz.iter().sum(),
    }
}

fn parse_ladder(value: Option<&Value>) -> BTreeMap<PriceKey, f64> {
    let mut ladder = BTreeMap::new();
    let Some(levels) = value.and_then(|item| item.as_array()) else {
        return ladder;
    };

    for level in levels {
        let Some(price) = value_as_f64(level, "price") else {
            continue;
        };
        let size = value_as_f64(level, "size").unwrap_or(0.0);
        if size <= 0.0 {
            continue;
        }
        ladder.insert(price_to_key(price), size);
    }

    ladder
}

fn price_to_key(price: f64) -> PriceKey {
    (price * 10_000.0).round() as i64
}

fn key_to_price(key: PriceKey) -> f64 {
    key as f64 / 10_000.0
}

fn value_as_str(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|item| match item {
        Value::String(text) => Some(text.to_string()),
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
