//! Latency benchmark — iceoryx2 IPC vs zenoh loopback.
//!
//! Runs a ping-pong test: publisher sends a timestamped payload, subscriber
//! receives it and measures one-way latency.  This is the number that goes
//! into the Medium article.
//!
//! Usage:
//!   # Terminal 1 — subscriber (pong)
//!   latency-bench pong
//!
//!   # Terminal 2 — publisher (ping)
//!   latency-bench ping --count 1000000

use bytemuck::Zeroable;
use clap::{Parser, Subcommand};
use common::{now_ns, MarketTick};
use iceoryx2::prelude::*;
use std::time::Instant;
use tracing::info;

const PING_SERVICE: &str = "bench/ping";
const PONG_SERVICE: &str = "bench/pong";

#[derive(Parser)]
#[command(about = "iceoryx2 latency benchmark (ping-pong)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Publish pings and measure round-trip latency
    Ping {
        #[arg(long, default_value_t = 1_000_000)]
        count: u64,

        /// Warmup iterations (excluded from stats)
        #[arg(long, default_value_t = 10_000)]
        warmup: u64,
    },
    /// Receive pings and echo them back (run before ping)
    Pong,
}

fn print_stats(label: &str, mut latencies: Vec<u64>) {
    if latencies.is_empty() {
        return;
    }
    latencies.sort_unstable();
    let n = latencies.len();
    let mean = latencies.iter().sum::<u64>() as f64 / n as f64;
    let p50 = latencies[n * 50 / 100];
    let p95 = latencies[n * 95 / 100];
    let p99 = latencies[n * 99 / 100];
    let p999 = latencies[(n * 999 / 1000).min(n - 1)];
    let max = *latencies.last().unwrap();

    println!("\n=== {} latency ({} samples) ===", label, n);
    println!("  mean  : {:>8.1} ns", mean);
    println!("  p50   : {:>8} ns", p50);
    println!("  p95   : {:>8} ns", p95);
    println!("  p99   : {:>8} ns", p99);
    println!("  p99.9 : {:>8} ns", p999);
    println!("  max   : {:>8} ns", max);
}

fn run_pong() {
    info!("pong side ready — waiting for pings");

    let node = NodeBuilder::new()
        .name(&NodeName::new("bench-pong").unwrap())
        .create::<ipc::Service>()
        .unwrap();

    let ping_svc = node
        .service_builder(&ServiceName::new(PING_SERVICE).unwrap())
        .publish_subscribe::<MarketTick>()
        .open_or_create()
        .unwrap();
    let subscriber = ping_svc.subscriber_builder().create().unwrap();

    let pong_svc = node
        .service_builder(&ServiceName::new(PONG_SERVICE).unwrap())
        .publish_subscribe::<MarketTick>()
        .open_or_create()
        .unwrap();
    let publisher = pong_svc.publisher_builder().create().unwrap();

    loop {
        while let Some(sample) = subscriber.receive().unwrap() {
            let tick: MarketTick = *sample;
            if let Ok(mut s) = publisher.loan_uninit() {
                s.payload_mut().write(tick);
                let s = unsafe { s.assume_init() };
                let _ = s.send();
            }
        }
        std::hint::spin_loop();
    }
}

fn run_ping(count: u64, warmup: u64) {
    info!(count, warmup, "ping side starting");

    let node = NodeBuilder::new()
        .name(&NodeName::new("bench-ping").unwrap())
        .create::<ipc::Service>()
        .unwrap();

    let ping_svc = node
        .service_builder(&ServiceName::new(PING_SERVICE).unwrap())
        .publish_subscribe::<MarketTick>()
        .open_or_create()
        .unwrap();
    let publisher = ping_svc.publisher_builder().create().unwrap();

    let pong_svc = node
        .service_builder(&ServiceName::new(PONG_SERVICE).unwrap())
        .publish_subscribe::<MarketTick>()
        .open_or_create()
        .unwrap();
    let subscriber = pong_svc.subscriber_builder().create().unwrap();

    let mut latencies: Vec<u64> = Vec::with_capacity(count as usize);
    let total = count + warmup;
    let mut completed: u64 = 0;

    let mut tick = MarketTick::zeroed();
    tick.bid = 30_000.0;
    tick.ask = 30_001.0;

    let wall_start = Instant::now();

    while completed < total {
        tick.timestamp_ns = now_ns();
        tick.seq = completed;

        if let Ok(mut s) = publisher.loan_uninit() {
            s.payload_mut().write(tick);
            let s = unsafe { s.assume_init() };
            let _ = s.send();
        }

        // Wait for pong
        loop {
            if let Some(pong) = subscriber.receive().unwrap() {
                if pong.seq == completed {
                    let rtt_ns = now_ns().saturating_sub(pong.timestamp_ns);
                    // one-way ≈ rtt / 2
                    let one_way_ns = rtt_ns / 2;
                    if completed >= warmup {
                        latencies.push(one_way_ns);
                    }
                    break;
                }
            }
            std::hint::spin_loop();
        }

        completed += 1;

        if completed % 100_000 == 0 {
            info!(completed, "ping progress");
        }
    }

    let elapsed = wall_start.elapsed();
    let throughput = total as f64 / elapsed.as_secs_f64();
    println!("\ntotal time  : {:.3}s", elapsed.as_secs_f64());
    println!("throughput  : {:.0} round-trips/s", throughput);

    print_stats("iceoryx2 one-way IPC", latencies);
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Pong => run_pong(),
        Cmd::Ping { count, warmup } => run_ping(count, warmup),
    }
}
