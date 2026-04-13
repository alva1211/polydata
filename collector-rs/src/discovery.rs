use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use regex::Regex;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::config::Settings;
use crate::models::{BookCommand, MarketInfo, MarketMetadataRow, TrackingPlan, WriterCommand};
use crate::time::{now_ns, ts_to_utc_string};

pub struct MarketDiscovery {
    settings: Arc<Settings>,
    http: Client,
    slug_regex: Regex,
}

impl MarketDiscovery {
    pub fn new(settings: Arc<Settings>) -> Result<Self> {
        Ok(Self {
            settings,
            http: Client::builder().build()?,
            slug_regex: Regex::new(r"^btc-updown-5m-(\d{10})$")?,
        })
    }

    pub async fn run(
        self,
        plan_tx: watch::Sender<TrackingPlan>,
        book_tx: mpsc::Sender<BookCommand>,
        writer_tx: mpsc::Sender<WriterCommand>,
    ) -> Result<()> {
        let mut current_plan = TrackingPlan::default();
        let mut interval = tokio::time::interval(Duration::from_secs(self.settings.discovery_interval_secs));

        loop {
            interval.tick().await;

            match self.discover_tracking_plan(&current_plan).await {
                Ok(mut plan) => {
                    plan = plan.normalized();
                    let current_ns = now_ns();
                    for market in &mut plan.markets {
                        market.created_recv_ts_ns = current_plan
                            .markets
                            .iter()
                            .find(|existing| existing.market_slug == market.market_slug)
                            .and_then(|existing| existing.created_recv_ts_ns)
                            .or(Some(current_ns));
                    }

                    let changed = current_plan != plan;
                    if changed {
                        let added: Vec<MarketInfo> = plan
                            .markets
                            .iter()
                            .filter(|market| {
                                !current_plan
                                    .markets
                                    .iter()
                                    .any(|existing| existing.market_slug == market.market_slug)
                            })
                            .cloned()
                            .collect();
                        let updated: Vec<MarketInfo> = plan
                            .markets
                            .iter()
                            .filter(|market| {
                                current_plan
                                    .markets
                                    .iter()
                                    .find(|existing| existing.market_slug == market.market_slug)
                                    .map(|existing| existing != *market)
                                    .unwrap_or(true)
                            })
                            .cloned()
                            .collect();

                        info!(
                            tracked_markets = ?plan.slugs(),
                            tracking_windows = %format_tracking_plan(&plan),
                            "updated tracking plan"
                        );
                        plan_tx.send_replace(plan.clone());

                        for market in &updated {
                            writer_tx
                                .send(WriterCommand::MarketMetadata(MarketMetadataRow::from_market(
                                    market,
                                    current_ns,
                                )))
                                .await?;
                        }

                        for market in &added {
                            self.bootstrap_book(market, &book_tx).await?;
                        }
                    }

                    current_plan = plan;
                }
                Err(err) => warn!(error = %err, "market discovery failed"),
            }
        }
    }

    async fn discover_tracking_plan(&self, current_plan: &TrackingPlan) -> Result<TrackingPlan> {
        if let (Some(yes_token_id), Some(no_token_id)) = (
            self.settings.poly_yes_token_id.clone(),
            self.settings.poly_no_token_id.clone(),
        ) {
            return Ok(TrackingPlan {
                markets: vec![MarketInfo {
                    market_slug: self
                        .settings
                        .poly_market_slug
                        .clone()
                        .unwrap_or_else(|| "manual-token-config".to_string()),
                    question: "manual-token-config".to_string(),
                    description: None,
                    condition_id: None,
                    yes_token_id,
                    no_token_id,
                    yes_outcome_label: Some("Yes".to_string()),
                    no_outcome_label: Some("No".to_string()),
                    market_start_ts: None,
                    market_end_ts: None,
                    tick_size: None,
                    min_order_size: None,
                    active: true,
                    closed: false,
                    resolved: false,
                    winner: None,
                    winning_token_id: None,
                    resolution_ts: None,
                    neg_risk: None,
                    event_id: None,
                    series_id: None,
                    total_volume: None,
                    created_recv_ts_ns: None,
                }],
            });
        }

        if let Some(slug) = self.settings.poly_market_slug.clone() {
            let markets = self.fetch_markets(&[("slug", slug)]).await?;
            if let Some(market) = self.pick_market(markets).await? {
                return Ok(TrackingPlan { markets: vec![market] });
            }
        }

        if let Some(plan) = self.discover_time_bucket_plan(current_plan).await? {
            return Ok(plan);
        }
        if !current_plan.markets.is_empty() {
            return Ok(current_plan.clone());
        }

        Err(anyhow!(
            "no BTC 5m time-bucket market found yet; waiting for the next scheduled market to appear"
        ))
    }

    async fn discover_time_bucket_plan(&self, current_plan: &TrackingPlan) -> Result<Option<TrackingPlan>> {
        let now_ts = Utc::now().timestamp();
        let current_bucket_start = (now_ts / 300) * 300;

        let primary_start = if current_plan.markets.is_empty() {
            current_bucket_start + 300
        } else if let Some(unexpired) = current_plan
            .markets
            .iter()
            .filter(|market| market.market_end_ts.unwrap_or(i64::MIN) > now_ts)
            .min_by_key(|market| market.market_start_ts.unwrap_or(i64::MAX))
        {
            unexpired.market_start_ts.unwrap_or(current_bucket_start + 300)
        } else {
            current_bucket_start + 300
        };

        let Some(primary_market) = self.fetch_time_bucket_market(primary_start).await? else {
            return Ok(None);
        };

        let mut markets = vec![primary_market.clone()];
        let primary_end = primary_market.market_end_ts.unwrap_or(primary_start + 300);
        if primary_end - now_ts <= self.settings.pre_subscribe_seconds {
            if let Some(next_market) = self.fetch_time_bucket_market(primary_start + 300).await? {
                markets.push(next_market);
            }
        }

        Ok(Some(TrackingPlan { markets }.normalized()))
    }

    async fn fetch_time_bucket_market(&self, start_ts: i64) -> Result<Option<MarketInfo>> {
        let slug = format!("btc-updown-5m-{}", start_ts);
        let markets = self.fetch_markets(&[("slug", slug)]).await?;
        if let Some(market) = self.pick_market(markets).await? {
            return Ok(Some(market));
        }

        Ok(None)
    }

    async fn fetch_markets(&self, query: &[(&str, String)]) -> Result<Vec<Value>> {
        let url = format!("{}/markets", self.settings.polymarket_gamma_url);
        let body = self
            .http
            .get(url)
            .query(query)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        Ok(value_as_array(&body))
    }

    async fn pick_market(&self, markets: Vec<Value>) -> Result<Option<MarketInfo>> {
        let mut candidates = Vec::new();

        for market in markets {
            let slug = value_as_str(&market, "slug").unwrap_or_default();
            let question = value_as_str(&market, "question")
                .or_else(|| value_as_str(&market, "title"))
                .unwrap_or_else(|| slug.clone());
            let haystack = format!("{} {}", slug.to_lowercase(), question.to_lowercase());

            if !(self.slug_regex.is_match(&slug)
                || haystack.contains(&self.settings.poly_market_query.to_lowercase()))
            {
                continue;
            }

            if value_as_bool(&market, "active") == Some(false)
                || value_as_bool(&market, "closed") == Some(true)
                || value_as_bool(&market, "enableOrderBook") == Some(false)
            {
                continue;
            }

            let Some((yes_token_id, no_token_id)) = extract_yes_no_tokens(&market) else {
                continue;
            };

            let Ok(yes_book) = self.fetch_book(&yes_token_id).await else {
                continue;
            };
            let Ok(no_book) = self.fetch_book(&no_token_id).await else {
                continue;
            };
            if !book_has_liquidity(&yes_book) || !book_has_liquidity(&no_book) {
                continue;
            }

            let (first_outcome, second_outcome) = extract_outcome_labels(&market);

            candidates.push(MarketInfo {
                market_slug: slug.clone(),
                question,
                description: value_as_str(&market, "description"),
                condition_id: value_as_str(&market, "conditionId")
                    .or_else(|| value_as_str(&market, "condition_id")),
                yes_token_id,
                no_token_id,
                yes_outcome_label: first_outcome,
                no_outcome_label: second_outcome,
                market_start_ts: parse_market_ts(&market, &["startDate", "start_date"]),
                market_end_ts: parse_market_end_ts(&market, &slug),
                tick_size: value_as_f64(&yes_book, "tick_size")
                    .or_else(|| value_as_f64(&market, "tickSize"))
                    .or_else(|| value_as_f64(&market, "tick_size")),
                min_order_size: value_as_f64(&yes_book, "min_order_size")
                    .or_else(|| value_as_f64(&market, "minimumOrderSize"))
                    .or_else(|| value_as_f64(&market, "minOrderSize")),
                active: value_as_bool(&market, "active").unwrap_or(true),
                closed: value_as_bool(&market, "closed").unwrap_or(false),
                resolved: value_as_bool(&market, "resolved").unwrap_or(false),
                winner: value_as_str(&market, "winner")
                    .or_else(|| value_as_str(&market, "winningOutcome"))
                    .or_else(|| value_as_str(&market, "winning_outcome")),
                winning_token_id: value_as_str(&market, "winningTokenId")
                    .or_else(|| value_as_str(&market, "winning_token_id")),
                resolution_ts: parse_market_ts(&market, &["resolutionDate", "resolution_date", "resolvedAt"]),
                neg_risk: value_as_bool(&market, "negRisk").or_else(|| value_as_bool(&market, "neg_risk")),
                event_id: value_as_str(&market, "eventId").or_else(|| value_as_str(&market, "event_id")),
                series_id: value_as_str(&market, "seriesId").or_else(|| value_as_str(&market, "series_id")),
                total_volume: value_as_f64(&market, "volume").or_else(|| value_as_f64(&market, "volumeNum")),
                created_recv_ts_ns: None,
            });
        }

        candidates.sort_by_key(|market| market.market_end_ts.unwrap_or(i64::MAX));
        Ok(candidates.into_iter().next())
    }

    async fn bootstrap_book(&self, market: &MarketInfo, book_tx: &mpsc::Sender<BookCommand>) -> Result<()> {
        info!(
            market_slug = %market.market_slug,
            market_window = %format_market_window(market),
            yes_token_id = %market.yes_token_id,
            no_token_id = %market.no_token_id,
            "bootstrapping market books from CLOB REST"
        );
        for (token_id, asset_side) in [
            (market.yes_token_id.clone(), "YES"),
            (market.no_token_id.clone(), "NO"),
        ] {
            let mut book = self.fetch_book(&token_id).await?;
            book["event_type"] = Value::String("book".to_string());
            book["asset_id"] = Value::String(token_id);
            book["asset_side"] = Value::String(asset_side.to_string());

            book_tx
                .send(BookCommand::Snapshot {
                    market: market.clone(),
                    recv_ts_ns: now_ns(),
                    payload: book,
                })
                .await
                .context("failed to send bootstrap book")?;
        }
        Ok(())
    }

    async fn fetch_book(&self, token_id: &str) -> Result<Value> {
        let url = format!("{}/book", self.settings.polymarket_clob_url);
        self.http
            .get(url)
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await
            .context("failed to parse clob book response")
    }
}

fn value_as_array(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items.clone(),
        Value::Object(map) => map
            .get("data")
            .and_then(|nested| nested.as_array())
            .cloned()
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn value_as_str(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|item| match item {
        Value::String(text) => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
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

fn value_as_f64(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|item| match item {
        Value::String(text) => text.parse::<f64>().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn parse_market_end_ts(market: &Value, slug: &str) -> Option<i64> {
    if let Some(ts) = parse_market_ts(market, &["endDate", "end_date"]) {
        return Some(ts);
    }

    Regex::new(r"^btc-updown-5m-(\d{10})$")
        .ok()
        .and_then(|regex| regex.captures(slug))
        .and_then(|caps| caps.get(1).map(|matched| matched.as_str().to_string()))
        .and_then(|raw| raw.parse::<i64>().ok())
        .map(|start_ts| start_ts + 300)
}

fn parse_market_ts(market: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(raw) = value_as_str(market, key) {
            if let Ok(parsed) = DateTime::parse_from_rfc3339(&raw) {
                return Some(parsed.timestamp());
            }
            if let Ok(ts) = raw.parse::<i64>() {
                return Some(ts);
            }
        }
    }
    None
}

fn extract_yes_no_tokens(market: &Value) -> Option<(String, String)> {
    let raw_tokens = market.get("clobTokenIds").or_else(|| market.get("clob_token_ids"))?;
    let token_ids = match raw_tokens {
        Value::String(text) => serde_json::from_str::<Vec<String>>(text).ok()?,
        Value::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str().map(|text| text.to_string()))
            .collect::<Vec<_>>(),
        _ => return None,
    };
    if token_ids.len() < 2 {
        return None;
    }

    let outcomes = match market.get("outcomes") {
        Some(Value::String(text)) => serde_json::from_str::<Vec<String>>(text).unwrap_or_default(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(|text| text.to_string()))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };

    if outcomes.len() >= 2 {
        let mut yes_token = None;
        let mut no_token = None;
        for (outcome, token_id) in outcomes.iter().zip(token_ids.iter()) {
            match outcome.to_ascii_lowercase().as_str() {
                "yes" => yes_token = Some(token_id.clone()),
                "no" => no_token = Some(token_id.clone()),
                _ => {}
            }
        }
        if let (Some(yes_token), Some(no_token)) = (yes_token, no_token) {
            return Some((yes_token, no_token));
        }
    }

    Some((token_ids[0].clone(), token_ids[1].clone()))
}

fn extract_outcome_labels(market: &Value) -> (Option<String>, Option<String>) {
    let outcomes = match market.get("outcomes") {
        Some(Value::String(text)) => serde_json::from_str::<Vec<String>>(text).unwrap_or_default(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(|text| text.to_string()))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };

    let first = outcomes.first().cloned();
    let second = outcomes.get(1).cloned();
    (first, second)
}

fn book_has_liquidity(book: &Value) -> bool {
    let bids = book
        .get("bids")
        .and_then(|value| value.as_array())
        .map(|levels| !levels.is_empty())
        .unwrap_or(false);
    let asks = book
        .get("asks")
        .and_then(|value| value.as_array())
        .map(|levels| !levels.is_empty())
        .unwrap_or(false);
    bids && asks
}

fn format_tracking_plan(plan: &TrackingPlan) -> String {
    if plan.markets.is_empty() {
        return "[]".to_string();
    }

    plan.markets
        .iter()
        .map(|market| format!("{} [{}]", market.market_slug, format_market_window(market)))
        .collect::<Vec<_>>()
        .join(" | ")
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
