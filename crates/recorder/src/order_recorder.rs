//! Order recorder — persists every OrderSignal to QuestDB and logs latency stats.
//!
//! decision_ns   = signal_ns − tick_ns        (strategy think time)
//! submission_ns = submitted_ns − signal_ns   (gateway + routing overhead)
//! e2e_ns        = submitted_ns − tick_ns     (full tick-to-order path)
//!
//! Latency percentiles are logged every 5 s and available for ad-hoc query in
//! QuestDB:
//!   SELECT percentile_approx(e2e_ns, 0.99) FROM order_signals SAMPLE BY 1m;

use crate::ilp::IlpWriter;
use common::{now_ns, OrderSignal};
use std::time::Duration;
use tracing::{info, warn};

pub async fn run(
    questdb_addr: String,
    _symbol: String,
    zenoh_session: zenoh::Session,
    order_key: String,
) {
    let subscriber = match zenoh_session.declare_subscriber(&order_key).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, key = %order_key, "failed to declare zenoh subscriber");
            return;
        }
    };

    let mut writer = IlpWriter::new(&questdb_addr);
    let mut stats_ticker = tokio::time::interval(Duration::from_secs(5));
    let mut e2e_latencies: Vec<u64> = Vec::with_capacity(1024);
    let mut decision_latencies: Vec<u64> = Vec::with_capacity(1024);
    let mut orders_total: u64 = 0;

    stats_ticker.tick().await;

    info!(key = %order_key, "order recorder ready");

    loop {
        tokio::select! {
            result = subscriber.recv_async() => {
                match result {
                    Ok(sample) => {
                        let submitted_ns = now_ns();
                        let bytes = sample.payload().to_bytes();

                        if bytes.len() != std::mem::size_of::<OrderSignal>() {
                            warn!(len = bytes.len(), "unexpected order payload size, skipping");
                            continue;
                        }

                        let order: OrderSignal = bytemuck::pod_read_unaligned(&bytes);
                        let decision_ns   = order.signal_ns.saturating_sub(order.tick_ns);
                        let e2e_ns        = submitted_ns.saturating_sub(order.tick_ns);
                        let submission_ns = submitted_ns.saturating_sub(order.signal_ns);

                        e2e_latencies.push(e2e_ns);
                        decision_latencies.push(decision_ns);
                        orders_total += 1;

                        writer.push_order(&order, submitted_ns);
                        writer.flush().await;

                        info!(
                            symbol       = order.symbol_str(),
                            side         = order.side_str(),
                            seq          = order.seq,
                            decision_ns,
                            submission_ns,
                            e2e_ns,
                            "order recorded"
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "zenoh order subscriber error");
                        break;
                    }
                }
            }

            _ = stats_ticker.tick() => {
                if e2e_latencies.is_empty() {
                    continue;
                }

                e2e_latencies.sort_unstable();
                decision_latencies.sort_unstable();

                let n = e2e_latencies.len();
                let p50  = e2e_latencies[n / 2];
                let p99  = e2e_latencies[(n * 99 / 100).min(n - 1)];
                let p999 = e2e_latencies[(n * 999 / 1000).min(n - 1)];
                let dp50 = decision_latencies[n / 2];
                let dp99 = decision_latencies[(n * 99 / 100).min(n - 1)];

                info!(
                    orders_total,
                    e2e_p50_ns  = p50,
                    e2e_p99_ns  = p99,
                    e2e_p999_ns = p999,
                    decision_p50_ns = dp50,
                    decision_p99_ns = dp99,
                    "latency percentiles"
                );

                e2e_latencies.clear();
                decision_latencies.clear();
            }
        }
    }
}
