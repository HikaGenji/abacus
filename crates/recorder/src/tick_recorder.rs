use crate::ilp::IlpWriter;
use common::MarketTick;
use iceoryx2::prelude::*;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub fn spawn_iox_tick_thread(service_name: String, tx: mpsc::Sender<MarketTick>) {
    std::thread::spawn(move || {
        let node = NodeBuilder::new()
            .name(&NodeName::new("recorder-ticks").unwrap())
            .create::<ipc::Service>()
            .unwrap();

        let service = node
            .service_builder(&ServiceName::new(&service_name).unwrap())
            .publish_subscribe::<MarketTick>()
            .open_or_create()
            .unwrap();

        let subscriber = service.subscriber_builder().create().unwrap();
        info!(service = %service_name, "iceoryx2 tick subscriber ready");

        loop {
            while let Some(sample) = subscriber.receive().unwrap() {
                let tick: MarketTick = *sample;
                if tx.try_send(tick).is_err() {
                    static DROP_COUNT: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let n = DROP_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if n % 10_000 == 0 {
                        warn!(dropped_total = n, "tick channel full — dropping ticks");
                    }
                }
            }
            std::hint::spin_loop();
        }
    });
}

pub async fn run(mut rx: mpsc::Receiver<MarketTick>, questdb_addr: String) {
    let mut writer = IlpWriter::new(&questdb_addr);
    let mut flush_ticker = tokio::time::interval(Duration::from_millis(100));
    let mut stats_ticker = tokio::time::interval(Duration::from_secs(5));
    let mut ticks_total: u64 = 0;

    flush_ticker.tick().await;
    stats_ticker.tick().await;

    info!("tick recorder ready");

    loop {
        tokio::select! {
            biased;

            result = rx.recv() => {
                match result {
                    Some(tick) => {
                        writer.push_tick(&tick);
                        ticks_total += 1;
                        if writer.needs_flush() {
                            writer.flush().await;
                        }
                    }
                    None => {
                        info!("tick channel closed");
                        writer.flush().await;
                        break;
                    }
                }
            }

            _ = flush_ticker.tick() => {
                writer.flush().await;
            }

            _ = stats_ticker.tick() => {
                info!(ticks_total, "tick throughput");
            }
        }
    }
}
