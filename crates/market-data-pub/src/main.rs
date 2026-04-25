//! Simulates an exchange market-data feed.
//!
//! Publishes `MarketTick` payloads via zenoh at a configurable rate.
//! Key expression: `"market/tick/{symbol}"` (default: `"market/tick/BTCUSD"`).
//!
//! In a real system this component would receive a FIX/ITCH/FAST feed from an
//! exchange co-location and forward normalised ticks into the local pipeline.

use clap::Parser;
use common::{now_ns, symbol_bytes, MarketTick};
use std::time::Duration;
use tracing::info;
use zenoh::bytes::ZBytes;

#[derive(Parser)]
#[command(about = "Exchange market-data feed simulator (zenoh publisher)")]
struct Args {
    /// Symbol to publish
    #[arg(long, default_value = "BTCUSD")]
    symbol: String,

    /// Ticks per second
    #[arg(long, default_value_t = 10_000)]
    rate: u64,

    /// Initial mid price
    #[arg(long, default_value_t = 30_000.0)]
    price: f64,

    /// Half-spread in price units
    #[arg(long, default_value_t = 0.5)]
    half_spread: f64,

    /// Total ticks to publish (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    count: u64,
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
    let key = format!("market/tick/{}", args.symbol);
    let sym = symbol_bytes(&args.symbol);
    let interval_ns = 1_000_000_000u64 / args.rate;

    info!(
        symbol = %args.symbol,
        rate = args.rate,
        key = %key,
        "starting market-data publisher"
    );

    let mut config = zenoh::Config::default();
    config.insert_json5("listen/endpoints", r#"["tcp/0.0.0.0:0"]"#).unwrap();
    let session = zenoh::open(config).await.unwrap();
    let publisher = session.declare_publisher(&key).await.unwrap();

    // Simple random-walk price simulation
    let mut mid = args.price;
    let mut seq: u64 = 0;
    let mut published: u64 = 0;
    let mut stats_tick: u64 = 0;
    let mut stats_t = std::time::Instant::now();

    loop {
        // Gaussian-ish random walk: combine two LCG values
        let r = lcg_next(&mut seq.wrapping_add(1234567891));
        let step = (r as f64 / u64::MAX as f64 - 0.5) * 2.0; // [-1, +1]
        mid += step;
        mid = mid.max(1.0); // prevent negative prices

        let tick = MarketTick {
            symbol: sym,
            bid: mid - args.half_spread,
            ask: mid + args.half_spread,
            timestamp_ns: now_ns(),
            seq,
        };

        let payload: &[u8] = bytemuck::bytes_of(&tick);
        publisher.put(ZBytes::from(payload.to_vec())).await.unwrap();

        seq += 1;
        published += 1;
        stats_tick += 1;

        // Log throughput once per second
        let elapsed = stats_t.elapsed();
        if elapsed >= Duration::from_secs(1) {
            let rate = stats_tick as f64 / elapsed.as_secs_f64();
            info!(
                published = published,
                rate = format!("{:.0} ticks/s", rate),
                mid = format!("{:.2}", mid),
                "market-data-pub"
            );
            stats_tick = 0;
            stats_t = std::time::Instant::now();
        }

        if args.count > 0 && published >= args.count {
            info!(published, "reached target count, stopping");
            break;
        }

        // Busy-sleep for tight timing control (acceptable in HFT co-location)
        let deadline = std::time::Instant::now() + Duration::from_nanos(interval_ns);
        while std::time::Instant::now() < deadline {
            std::hint::spin_loop();
        }
    }
}

/// Minimal LCG for price simulation — not cryptographic, just fast.
fn lcg_next(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}
