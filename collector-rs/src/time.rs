use chrono::{TimeZone, Utc};

pub fn now_ns() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
}

pub fn ns_to_dt_string(ts_ns: i64) -> String {
    let secs = ts_ns.div_euclid(1_000_000_000);
    let nanos = ts_ns.rem_euclid(1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d")
        .to_string()
}

pub fn ts_to_utc_string(ts: i64) -> String {
    Utc.timestamp_opt(ts, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string()
}
