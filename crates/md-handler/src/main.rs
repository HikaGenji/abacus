//! Market-data handler — the zenoh → iceoryx2 bridge.
//!
//! Receives ticks from the external feed (zenoh) and republishes them onto the
//! local shared-memory bus (iceoryx2) with zero heap allocation on the hot path.
//!
//! This is where network latency ends and nanosecond-class IPC latency begins.

use clap::Parser;
use common::{MarketTick};
use iceoryx2::prelude::*;
use std::time::Duration;
use tracing::info;

const SERVICE_NAME: &str = "market_data/BTCUSD";

#[derive(Parser)]
#[command(about = "Market-data handler: zenoh subscriber → iceoryx2 publisher")]
struct Args {
    /// zenoh key expression to subscribe to
    #[arg(long, default_value = "market/tick/BTCUSD")]
    key: String,

    /// iceoryx2 service name to publish on
    #[arg(long, default_value = SERVICE_NAME)]
    iox_service: String,
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

    info!(key = %args.key, service = %args.iox_service, "starting md-handler");

    // iceoryx2: create node and publisher
    let iox_node = NodeBuilder::new()
        .name(&NodeName::new("md-handler").unwrap())
        .create::<ipc::Service>()
        .unwrap();

    let service = iox_node
        .service_builder(&ServiceName::new(&args.iox_service).unwrap())
        .publish_subscribe::<MarketTick>()
        .open_or_create()
        .unwrap();

    let iox_publisher = service.publisher_builder().create().unwrap();

    // zenoh: subscribe to market data feed
    let mut config = zenoh::Config::default();
    config.insert_json5("listen/endpoints", r#"["tcp/0.0.0.0:0"]"#).unwrap();
    let session = zenoh::open(config).await.unwrap();
    let subscriber = session.declare_subscriber(&args.key).await.unwrap();

    info!("md-handler ready — forwarding ticks to iceoryx2 shared memory");

    let mut count: u64 = 0;
    let mut stats_t = std::time::Instant::now();

    loop {
        let sample = match subscriber.recv_async().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "zenoh receive error");
                continue;
            }
        };

        let bytes = sample.payload().to_bytes();
        if bytes.len() != std::mem::size_of::<MarketTick>() {
            tracing::warn!(len = bytes.len(), "unexpected payload size, skipping");
            continue;
        }

        // Deserialise — one copy from network buffer into stack
        let tick: MarketTick = bytemuck::pod_read_unaligned(&bytes);

        // Zero-copy publish onto shared memory: loan → write → send
        match iox_publisher.loan_uninit() {
            Ok(mut sample) => {
                sample.payload_mut().write(tick);
                // SAFETY: we just wrote a valid MarketTick above
                let sample = unsafe { sample.assume_init() };
                if let Err(e) = sample.send() {
                    tracing::warn!(error = ?e, "iceoryx2 send error");
                }
            }
            Err(e) => {
                tracing::warn!(error = ?e, "iceoryx2 loan error");
            }
        }

        count += 1;

        let elapsed = stats_t.elapsed();
        if elapsed >= Duration::from_secs(1) {
            let rate = count as f64 / elapsed.as_secs_f64();
            info!(
                forwarded = count,
                rate = format!("{:.0} ticks/s", rate),
                "md-handler"
            );
            count = 0;
            stats_t = std::time::Instant::now();
        }
    }
}
