use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketInfo {
    pub market_slug: String,
    pub question: String,
    pub description: Option<String>,
    pub condition_id: Option<String>,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub yes_outcome_label: Option<String>,
    pub no_outcome_label: Option<String>,
    pub market_start_ts: Option<i64>,
    pub market_end_ts: Option<i64>,
    pub tick_size: Option<f64>,
    pub min_order_size: Option<f64>,
    pub active: bool,
    pub closed: bool,
    pub resolved: bool,
    pub winner: Option<String>,
    pub winning_token_id: Option<String>,
    pub resolution_ts: Option<i64>,
    pub neg_risk: Option<bool>,
    pub event_id: Option<String>,
    pub series_id: Option<String>,
    pub total_volume: Option<f64>,
    pub created_recv_ts_ns: Option<i64>,
}

impl PartialEq for MarketInfo {
    fn eq(&self, other: &Self) -> bool {
        self.market_slug == other.market_slug
            && self.question == other.question
            && self.description == other.description
            && self.condition_id == other.condition_id
            && self.yes_token_id == other.yes_token_id
            && self.no_token_id == other.no_token_id
            && self.yes_outcome_label == other.yes_outcome_label
            && self.no_outcome_label == other.no_outcome_label
            && self.market_start_ts == other.market_start_ts
            && self.market_end_ts == other.market_end_ts
            && self.tick_size == other.tick_size
            && self.min_order_size == other.min_order_size
            && self.active == other.active
            && self.closed == other.closed
            && self.resolved == other.resolved
            && self.winner == other.winner
            && self.winning_token_id == other.winning_token_id
            && self.resolution_ts == other.resolution_ts
            && self.neg_risk == other.neg_risk
            && self.event_id == other.event_id
            && self.series_id == other.series_id
            && self.total_volume == other.total_volume
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMetadataRow {
    pub created_recv_ts_ns: i64,
    pub updated_recv_ts_ns: i64,
    pub market_slug: String,
    pub question: String,
    pub description: Option<String>,
    pub condition_id: Option<String>,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub yes_outcome_label: Option<String>,
    pub no_outcome_label: Option<String>,
    pub market_start_ts: Option<i64>,
    pub market_end_ts: Option<i64>,
    pub tick_size: Option<f64>,
    pub min_order_size: Option<f64>,
    pub active: bool,
    pub closed: bool,
    pub resolved: bool,
    pub winner: Option<String>,
    pub winning_token_id: Option<String>,
    pub resolution_ts: Option<i64>,
    pub neg_risk: Option<bool>,
    pub event_id: Option<String>,
    pub series_id: Option<String>,
    pub total_volume: Option<f64>,
}

impl MarketMetadataRow {
    pub fn from_market(market: &MarketInfo, updated_recv_ts_ns: i64) -> Self {
        Self {
            created_recv_ts_ns: market.created_recv_ts_ns.unwrap_or(updated_recv_ts_ns),
            updated_recv_ts_ns,
            market_slug: market.market_slug.clone(),
            question: market.question.clone(),
            description: market.description.clone(),
            condition_id: market.condition_id.clone(),
            yes_token_id: market.yes_token_id.clone(),
            no_token_id: market.no_token_id.clone(),
            yes_outcome_label: market.yes_outcome_label.clone(),
            no_outcome_label: market.no_outcome_label.clone(),
            market_start_ts: market.market_start_ts,
            market_end_ts: market.market_end_ts,
            tick_size: market.tick_size,
            min_order_size: market.min_order_size,
            active: market.active,
            closed: market.closed,
            resolved: market.resolved,
            winner: market.winner.clone(),
            winning_token_id: market.winning_token_id.clone(),
            resolution_ts: market.resolution_ts,
            neg_risk: market.neg_risk,
            event_id: market.event_id.clone(),
            series_id: market.series_id.clone(),
            total_volume: market.total_volume,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketRawEventRow {
    pub recv_ts_ns: i64,
    pub source_ts_ms: Option<i64>,
    pub event_type: String,
    pub market_slug: String,
    pub condition_id: Option<String>,
    pub token_id: Option<String>,
    pub asset_side: Option<String>,
    pub book_hash: Option<String>,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketTradeRow {
    pub recv_ts_ns: i64,
    pub source_ts_ms: Option<i64>,
    pub market_slug: String,
    pub condition_id: Option<String>,
    pub token_id: String,
    pub asset_side: String,
    pub trade_price: f64,
    pub trade_size: f64,
    pub trade_side: Option<String>,
    pub transaction_hash: Option<String>,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketMarketEventRow {
    pub recv_ts_ns: i64,
    pub source_ts_ms: Option<i64>,
    pub market_slug: String,
    pub condition_id: Option<String>,
    pub token_id: Option<String>,
    pub asset_side: Option<String>,
    pub event_type: String,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub side: Option<String>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub event_hash: Option<String>,
    pub old_tick_size: Option<f64>,
    pub new_tick_size: Option<f64>,
    pub winning_asset_id: Option<String>,
    pub winning_outcome: Option<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub resolved: Option<bool>,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceEventRow {
    pub recv_ts_ns: i64,
    pub source_ts_ms: Option<i64>,
    pub symbol: String,
    pub stream: String,
    pub event_type: String,
    pub trade_price: Option<f64>,
    pub trade_qty: Option<f64>,
    pub is_buyer_maker: Option<bool>,
    pub best_bid: Option<f64>,
    pub best_bid_qty: Option<f64>,
    pub best_ask: Option<f64>,
    pub best_ask_qty: Option<f64>,
    pub kline_open: Option<f64>,
    pub kline_high: Option<f64>,
    pub kline_low: Option<f64>,
    pub kline_close: Option<f64>,
    pub kline_volume: Option<f64>,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshotRow {
    pub snapshot_ts_ns: i64,
    pub snapshot_ts_sec: i64,
    pub recv_ts_ns: i64,
    pub source_ts_ms: Option<i64>,
    pub market_slug: String,
    pub condition_id: Option<String>,
    pub token_id: String,
    pub asset_side: String,
    pub book_hash: Option<String>,
    pub bid_px: [f64; 10],
    pub bid_sz: [f64; 10],
    pub ask_px: [f64; 10],
    pub ask_sz: [f64; 10],
    pub mid_px: f64,
    pub spread_px: f64,
    pub spread_bps: f64,
    pub total_bid_depth_l5: f64,
    pub total_ask_depth_l5: f64,
    pub total_bid_depth_l10: f64,
    pub total_ask_depth_l10: f64,
}

#[derive(Debug, Clone)]
pub enum IngestedEvent {
    Polymarket {
        market: MarketInfo,
        recv_ts_ns: i64,
        payload: Value,
    },
    Binance {
        recv_ts_ns: i64,
        payload: Value,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TrackingPlan {
    pub markets: Vec<MarketInfo>,
}

impl TrackingPlan {
    pub fn normalized(mut self) -> Self {
        self.markets.sort_by_key(|market| market.market_start_ts.unwrap_or(i64::MAX));
        self
    }

    pub fn slugs(&self) -> Vec<String> {
        self.markets.iter().map(|market| market.market_slug.clone()).collect()
    }
}

#[derive(Debug, Clone)]
pub enum BookCommand {
    Snapshot {
        market: MarketInfo,
        recv_ts_ns: i64,
        payload: Value,
    },
    PriceChange {
        market: MarketInfo,
        recv_ts_ns: i64,
        source_ts_ms: Option<i64>,
        token_id: String,
        asset_side: String,
        price: f64,
        size: f64,
        side: String,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        event_hash: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum WriterCommand {
    MarketMetadata(MarketMetadataRow),
    PolymarketRawEvent(PolymarketRawEventRow),
    BinanceEvent(BinanceEventRow),
    PolymarketMarketEvent(PolymarketMarketEventRow),
    PolymarketTrade(PolymarketTradeRow),
    BookSnapshot(BookSnapshotRow),
    FlushAndShutdown,
}
