//! Momentum strategy engine — pure iceoryx2 IPC on the hot path.
//!
//! Subscribes to market ticks via iceoryx2 shared memory (zero-copy, ~100 ns).
//! Computes an exponential moving average (EMA-20) and emits a BUY signal when
//! mid > EMA, SELL when mid < EMA.  Publishes `OrderSignal` back onto iceoryx2.
//!
//! Latency stats (p50/p99/p999) are printed every 5 seconds.

use clap::Parser;
use common::{now_ns, MarketTick, OrderSignal};
use iceoryx2::prelude::*;
use std::time::{Duration, Instant};
use tracing::info;

const IN_SERVICE: &str = "market_data/BTCUSD";
const OUT_SERVICE: &str = "orders/BTCUSD";

#[derive(Parser)]
#[command(about = "Momentum strategy: iceoryx2 tick subscriber → order publisher")]
struct Args {
    #[arg(long, default_value = IN_SERVICE)]
    in_service: String,

    #[arg(long, default_value = OUT_SERVICE)]
    out_service: String,

    /// EMA window (number of ticks)
    #[arg(long, default_value_t = 20)]
    ema_window: u64,

    /// Order quantity
    #[arg(long, default_value_t = 0.01)]
    qty: f64,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let alpha = 2.0 / (args.ema_window as f64 + 1.0);

    info!(
        in = %args.in_service,
        out = %args.out_service,
        ema_window = args.ema_window,
        "starting strategy engine"
    );

    let iox_node = NodeBuilder::new()
        .name(&NodeName::new("strategy").unwrap())
        .create::<ipc::Service>()
        .unwrap();

    // Subscriber for incoming ticks
    let in_service = iox_node
        .service_builder(&ServiceName::new(&args.in_service).unwrap())
        .publish_subscribe::<MarketTick>()
        .open_or_create()
        .unwrap();
    let subscriber = in_service.subscriber_builder().create().unwrap();

    // Publisher for outgoing order signals
    let out_service = iox_node
        .service_builder(&ServiceName::new(&args.out_service).unwrap())
        .publish_subscribe::<OrderSignal>()
        .open_or_create()
        .unwrap();
    let publisher = out_service.publisher_builder().create().unwrap();

    let mut ema: Option<f64> = None;
    let mut prev_side: Option<u8> = None;
    let mut latencies: Vec<u64> = Vec::with_capacity(100_000);
    let mut last_stats = Instant::now();
    let mut signals: u64 = 0;
    let mut seq: u64 = 0;

    info!("strategy ready — polling iceoryx2 shared memory");

    loop {
        // Spin-receive: no OS scheduling jitter on the hot path
        while let Some(sample) = subscriber.receive().unwrap() {
            let recv_ns = now_ns();
            let tick: &MarketTick = &*sample;
            let mid = tick.mid();

            // EMA update
            let ema_val = match ema {
                None => {
                    ema = Some(mid);
                    mid
                }
                Some(e) => {
                    let new_e = alpha * mid + (1.0 - alpha) * e;
                    ema = Some(new_e);
                    new_e
                }
            };

            // Signal: side flips when mid crosses EMA
            let side: u8 = if mid >= ema_val { 0 } else { 1 };

            // Only emit a new order when side changes (avoid order spam)
            if prev_side != Some(side) {
                prev_side = Some(side);
                signals += 1;
                seq += 1;

                let signal = OrderSignal {
                    symbol: tick.symbol,
                    side,
                    _pad: [0u8; 7],
                    quantity: args.qty,
                    limit_price: if side == 0 { tick.ask } else { tick.bid },
                    signal_ns: now_ns(),
                    tick_ns: tick.timestamp_ns,
                    seq,
                };

                if let Ok(mut s) = publisher.loan_uninit() {
                    s.payload_mut().write(signal);
                    let s = unsafe { s.assume_init() };
                    let _ = s.send();
                }

                // Record tick-to-signal latency
                let latency_ns = recv_ns.saturating_sub(tick.timestamp_ns);
                latencies.push(latency_ns);
            }
        }

        // Print stats every 5 seconds
        if last_stats.elapsed() >= Duration::from_secs(5) && !latencies.is_empty() {
            latencies.sort_unstable();
            let n = latencies.len();
            let p50 = latencies[n * 50 / 100];
            let p99 = latencies[n * 99 / 100];
            let p999 = latencies[(n * 999 / 1000).min(n - 1)];
            let max = *latencies.last().unwrap();

            info!(
                signals,
                samples = n,
                p50_ns = p50,
                p99_ns = p99,
                p999_ns = p999,
                max_ns = max,
                "tick-to-signal latency (ns)"
            );

            latencies.clear();
            last_stats = Instant::now();
        }

        // Yield briefly to avoid starving other threads; remove for max throughput
        std::hint::spin_loop();
    }
}
