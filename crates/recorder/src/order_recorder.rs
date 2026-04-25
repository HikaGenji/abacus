//! Order recorder — persists OrderSignal to QuestDB, latency metrics to InfluxDB.
//!
//! Subscribes to the zenoh key `exchange/orders/BTCUSD` — the final hop in the
//! pipeline after the order-gateway has dispatched the order.  Adding `now_ns()`
//! at this point gives us the submission timestamp, enabling three latency measures:
//!
//!   decision_ns   = signal_ns − tick_ns        (strategy think time)
//!   submission_ns = submitted_ns − signal_ns   (gateway + routing overhead)
//!   e2e_ns        = submitted_ns − tick_ns     (full tick-to-order path)
//!
//! Every 5 s the latency distributions are sorted and percentile metrics are
//! posted to InfluxDB for Grafana dashboards.

use crate::ilp::IlpWriter;
use crate::influx::{execution_latency_ilp, throughput_ilp, InfluxClient};
use common::{now_ns, OrderSignal};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

pub async fn run(
    questdb_addr: String,
    influx: Arc<InfluxClient>,
    symbol: String,
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

    stats_ticker.tick().await; // discard first immediate tick

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

                        let order: OrderSignal = *bytemuck::from_bytes(&bytes);
                        let decision_ns   = order.signal_ns.saturating_sub(order.tick_ns);
                        let e2e_ns        = submitted_ns.saturating_sub(order.tick_ns);
                        let submission_ns = submitted_ns.saturating_sub(order.signal_ns);

                        e2e_latencies.push(e2e_ns);
                        decision_latencies.push(decision_ns);
                        orders_total += 1;

                        // Write order to QuestDB immediately (orders are rare)
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
                let ts = now_ns();

                let mut body = execution_latency_ilp(
                    &symbol,
                    &e2e_latencies,
                    &decision_latencies,
                    orders_total,
                    ts,
                );
                body.push_str(&throughput_ilp(&symbol, orders_total, ts));

                let e2e_p50 = e2e_latencies[e2e_latencies.len() / 2];
                let e2e_p99 = e2e_latencies[(e2e_latencies.len() * 99 / 100).min(e2e_latencies.len() - 1)];

                let influx = influx.clone();
                tokio::spawn(async move { influx.write(body).await });

                info!(
                    orders_total,
                    e2e_p50_ns = e2e_p50,
                    e2e_p99_ns = e2e_p99,
                    "execution metrics reported to InfluxDB"
                );

                e2e_latencies.clear();
                decision_latencies.clear();
            }
        }
    }
}
