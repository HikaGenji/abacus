//! Order gateway — iceoryx2 → zenoh bridge for order routing.
//!
//! Reads `OrderSignal` from shared memory (submitted by the strategy engine)
//! and forwards them to the exchange simulator via zenoh.
//!
//! Also computes end-to-end tick-to-order latency by comparing the order's
//! embedded `tick_ns` with the current wall clock.

use clap::Parser;
use common::{now_ns, OrderSignal};
use iceoryx2::prelude::*;
use std::time::{Duration, Instant};
use tracing::info;
use zenoh::bytes::ZBytes;

const IN_SERVICE: &str = "orders/BTCUSD";

#[derive(Parser)]
#[command(about = "Order gateway: iceoryx2 order subscriber → zenoh publisher")]
struct Args {
    #[arg(long, default_value = IN_SERVICE)]
    in_service: String,

    /// zenoh key to publish orders on
    #[arg(long, default_value = "exchange/orders/BTCUSD")]
    key: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    info!(service = %args.in_service, key = %args.key, "starting order-gateway");

    let iox_node = NodeBuilder::new()
        .name(&NodeName::new("order-gateway").unwrap())
        .create::<ipc::Service>()
        .unwrap();

    let in_service = iox_node
        .service_builder(&ServiceName::new(&args.in_service).unwrap())
        .publish_subscribe::<OrderSignal>()
        .open_or_create()
        .unwrap();
    let subscriber = in_service.subscriber_builder().create().unwrap();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let publisher = session.declare_publisher(&args.key).await.unwrap();

    let mut e2e_latencies: Vec<u64> = Vec::with_capacity(10_000);
    let mut last_stats = Instant::now();
    let mut routed: u64 = 0;

    info!("order-gateway ready — polling iceoryx2 shared memory");

    loop {
        while let Some(sample) = subscriber.receive().unwrap() {
            let gw_ns = now_ns();
            let order: &OrderSignal = &*sample;

            // End-to-end: exchange tick timestamp → order reaching gateway
            let e2e_ns = gw_ns.saturating_sub(order.tick_ns);
            e2e_latencies.push(e2e_ns);

            // Serialise and publish via zenoh to the exchange simulator
            let payload: &[u8] = bytemuck::bytes_of(order);
            publisher
                .put(ZBytes::from(payload.to_vec()))
                .await
                .unwrap();

            routed += 1;

            info!(
                seq = order.seq,
                symbol = order.symbol_str(),
                side = order.side_str(),
                qty = order.quantity,
                price = order.limit_price,
                e2e_ns,
                "order routed"
            );
        }

        if last_stats.elapsed() >= Duration::from_secs(5) && !e2e_latencies.is_empty() {
            e2e_latencies.sort_unstable();
            let n = e2e_latencies.len();
            let p50 = e2e_latencies[n * 50 / 100];
            let p99 = e2e_latencies[n * 99 / 100];
            let p999 = e2e_latencies[(n * 999 / 1000).min(n - 1)];
            let max = *e2e_latencies.last().unwrap();

            info!(
                routed,
                samples = n,
                p50_ns = p50,
                p99_ns = p99,
                p999_ns = p999,
                max_ns = max,
                "END-TO-END tick-to-order latency (ns)"
            );

            e2e_latencies.clear();
            last_stats = Instant::now();
        }

        std::hint::spin_loop();
    }
}
