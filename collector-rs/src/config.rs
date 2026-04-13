use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Settings {
    pub polymarket_gamma_url: String,
    pub polymarket_clob_url: String,
    pub polymarket_ws_url: String,
    pub binance_ws_url: String,
    pub poly_market_query: String,
    pub poly_market_slug: Option<String>,
    pub poly_yes_token_id: Option<String>,
    pub poly_no_token_id: Option<String>,
    pub discovery_interval_secs: u64,
    pub snapshot_interval_secs: u64,
    pub pre_subscribe_seconds: i64,
    pub writer_flush_interval_secs: u64,
    pub writer_batch_rows: usize,
    pub data_dir: PathBuf,
}

impl Settings {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        Self {
            polymarket_gamma_url: env_var("POLYMARKET_GAMMA_URL", "https://gamma-api.polymarket.com"),
            polymarket_clob_url: env_var("POLYMARKET_CLOB_URL", "https://clob.polymarket.com"),
            polymarket_ws_url: env_var(
                "POLYMARKET_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ),
            binance_ws_url: env_var(
                "BINANCE_WS_URL",
                "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@kline_1s",
            ),
            poly_market_query: env_var("POLY_MARKET_QUERY", "btc-updown-5m"),
            poly_market_slug: opt_env_var("POLY_MARKET_SLUG"),
            poly_yes_token_id: opt_env_var("POLY_YES_TOKEN_ID"),
            poly_no_token_id: opt_env_var("POLY_NO_TOKEN_ID"),
            discovery_interval_secs: parse_env_var("COLLECTOR_DISCOVERY_INTERVAL_SECS", 20_u64),
            snapshot_interval_secs: parse_env_var("COLLECTOR_SNAPSHOT_INTERVAL_SECS", 1_u64),
            pre_subscribe_seconds: parse_env_var("COLLECTOR_PRE_SUBSCRIBE_SECONDS", 30_i64),
            writer_flush_interval_secs: parse_env_var("COLLECTOR_WRITER_FLUSH_INTERVAL_SECS", 5_u64),
            writer_batch_rows: parse_env_var("COLLECTOR_WRITER_BATCH_ROWS", 500_usize),
            data_dir: PathBuf::from(env_var("COLLECTOR_DATA_DIR", "./data/collector")),
        }
    }
}

fn env_var(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn opt_env_var(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_env_var<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env::var(key)
        .ok()
        .and_then(|raw| raw.parse::<T>().ok())
        .unwrap_or(default)
}
