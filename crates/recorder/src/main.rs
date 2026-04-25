mod ilp;
mod order_recorder;
mod tick_recorder;

use clap::Parser;
use common::MarketTick;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Parser)]
#[command(about = "Recorder: market ticks and orders → QuestDB")]
struct Args {
    /// QuestDB ILP TCP address (port 9009)
    #[arg(long, default_value = "127.0.0.1:9009")]
    questdb_addr: String,

    /// iceoryx2 service name for market data ticks
    #[arg(long, default_value = "market_data/BTCUSD")]
    iox_tick_service: String,

    /// zenoh key expression for dispatched orders
    #[arg(long, default_value = "exchange/orders/BTCUSD")]
    order_key: String,

    /// Symbol tag written to QuestDB
    #[arg(long, default_value = "BTCUSD")]
    symbol: String,

    /// Capacity of the tick mpsc channel
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

    let (tick_tx, tick_rx) = mpsc::channel::<MarketTick>(args.tick_channel_cap);
    tick_recorder::spawn_iox_tick_thread(args.iox_tick_service.clone(), tick_tx);

    let zenoh_session = zenoh::open(zenoh::Config::default()).await.unwrap();

    info!(
        questdb = %args.questdb_addr,
        symbol  = %args.symbol,
        "recorder started — writing to QuestDB"
    );

    tokio::join!(
        tick_recorder::run(tick_rx, args.questdb_addr.clone()),
        order_recorder::run(
            args.questdb_addr,
            args.symbol,
            zenoh_session,
            args.order_key,
        ),
    );
}
