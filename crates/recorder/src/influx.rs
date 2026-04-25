//! InfluxDB v2 HTTP client for writing operational metrics.
//!
//! Writes to the `/api/v2/write` endpoint using the InfluxDB Line Protocol.
//! Failures are logged and silently ignored — InfluxDB is non-critical for
//! the trading pipeline; QuestDB holds the authoritative audit trail.

use tracing::warn;

pub struct InfluxClient {
    http: reqwest::Client,
    write_url: String,
    auth: String,
}

impl InfluxClient {
    pub fn new(base_url: &str, org: &str, bucket: &str, token: &str) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("reqwest client build failed");
        let write_url = format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ns",
            base_url.trim_end_matches('/'),
            org,
            bucket,
        );
        Self {
            http,
            write_url,
            auth: format!("Token {}", token),
        }
    }

    /// POST one or more ILP lines to InfluxDB. Errors are logged, not propagated.
    pub async fn write(&self, lines: String) {
        match self
            .http
            .post(&self.write_url)
            .header("Authorization", &self.auth)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(lines)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => warn!(status = %r.status(), "InfluxDB write non-2xx response"),
            Err(e) => warn!(error = %e, "InfluxDB write failed"),
        }
    }
}

/// Build the `execution_latency` ILP measurement from sorted latency vecs.
///
/// Both slices must be sorted ascending before calling.
pub fn execution_latency_ilp(
    symbol: &str,
    e2e: &[u64],
    decision: &[u64],
    count: u64,
    ts: u64,
) -> String {
    if e2e.is_empty() {
        return String::new();
    }
    let pct = |v: &[u64], p: usize| -> u64 {
        v[(v.len() * p / 100).min(v.len() - 1)]
    };
    let p999 = |v: &[u64]| -> u64 {
        v[(v.len() * 999 / 1000).min(v.len() - 1)]
    };
    format!(
        "execution_latency,symbol={sym} e2e_p50={p50}i,e2e_p99={p99}i,e2e_p999={p999}i,decision_p50={d50}i,decision_p99={d99}i,count={count}i {ts}\n",
        sym    = symbol,
        p50    = pct(e2e, 50),
        p99    = pct(e2e, 99),
        p999   = p999(e2e),
        d50    = pct(decision, 50),
        d99    = pct(decision, 99),
        count  = count,
        ts     = ts,
    )
}

/// Build the `throughput` ILP measurement for order totals.
pub fn throughput_ilp(symbol: &str, orders_total: u64, ts: u64) -> String {
    format!(
        "throughput,symbol={sym} orders_total={n}i {ts}\n",
        sym = symbol,
        n   = orders_total,
        ts  = ts,
    )
}
