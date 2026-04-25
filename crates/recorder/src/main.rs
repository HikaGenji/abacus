//! Recorder — persistent storage backend for best execution, TCA, and auditing.
//!
//! Two parallel data paths:
//!
//! 1. **Market ticks → QuestDB** (`market_ticks` table)
//!    - iceoryx2 subscriber on `market_data/BTCUSD` (zero-copy shared memory)
//!    - Batched ILP TCP writes every 100 ms or 32 KB
//!
//! 2. **Orders → QuestDB + InfluxDB** (`order_signals` table, `execution_latency` measurement)
//!    - zenoh subscriber on `exchange/orders/BTCUSD`
//!    - Immediate QuestDB write on every order
//!    - Latency percentiles (p50/p99/p999) posted to InfluxDB every 5 s
//!
//! Start infrastructure first:
//!   docker compose up -d
//!
//! Then run the recorder alongside the demo pipeline:
//!   RUST_LOG=info ./target/release/recorder

mod ilp;
mod influx;
mod order_recorder;
mod tick_recorder;

use clap::Parser;
use common::MarketTick;
use influx::InfluxClient;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Parser)]
#[command(about = "Recorder: market ticks → QuestDB, execution metrics → InfluxDB")]
struct Args {
    /// QuestDB ILP TCP address (port 9009)
    #[arg(long, default_value = "127.0.0.1:9009")]
    questdb_addr: String,

    /// InfluxDB HTTP base URL
    #[arg(long, default_value = "http://127.0.0.1:8086")]
    influxdb_url: String,

    /// InfluxDB bucket
    #[arg(long, default_value = "hft")]
    influxdb_bucket: String,

    /// InfluxDB organisation
    #[arg(long, default_value = "hft_org")]
    influxdb_org: String,

    /// InfluxDB API token (can also be set via INFLUXDB_TOKEN env var)
    #[arg(long, env = "INFLUXDB_TOKEN", default_value = "hft_token")]
    influxdb_token: String,

    /// iceoryx2 service name for market data ticks
    #[arg(long, default_value = "market_data/BTCUSD")]
    iox_tick_service: String,

    /// zenoh key expression for dispatched orders
    #[arg(long, default_value = "exchange/orders/BTCUSD")]
    order_key: String,

    /// Symbol tag written to QuestDB/InfluxDB
    #[arg(long, default_value = "BTCUSD")]
    symbol: String,

    /// Capacity of the tick mpsc channel (ticks buffered between IPC thread and async task)
    #[arg(long, default_value_t = 1024)]
    tick_channel_cap: usize,
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

    let influx = Arc::new(InfluxClient::new(
        &args.influxdb_url,
        &args.influxdb_org,
        &args.influxdb_bucket,
        &args.influxdb_token,
    ));

    // Bridge: iceoryx2 blocking thread → async tokio via mpsc
    let (tick_tx, tick_rx) = mpsc::channel::<MarketTick>(args.tick_channel_cap);
    tick_recorder::spawn_iox_tick_thread(args.iox_tick_service.clone(), tick_tx);

    // zenoh session (owned by order_recorder task)
    let zenoh_session = zenoh::open(zenoh::Config::default()).await.unwrap();

    info!(
        questdb  = %args.questdb_addr,
        influxdb = %args.influxdb_url,
        bucket   = %args.influxdb_bucket,
        symbol   = %args.symbol,
        "recorder started — writing to QuestDB and InfluxDB"
    );

    // Run both recording tasks concurrently
    tokio::join!(
        tick_recorder::run(
            tick_rx,
            args.questdb_addr.clone(),
            influx.clone(),
            args.symbol.clone(),
        ),
        order_recorder::run(
            args.questdb_addr,
            influx,
            args.symbol,
            zenoh_session,
            args.order_key,
        ),
    );
}
