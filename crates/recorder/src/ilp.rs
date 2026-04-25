//! InfluxDB Line Protocol (ILP) writer over a persistent TCP connection.
//!
//! Used to stream data into QuestDB's ILP listener (default port 9009).
//! Internally buffers lines and flushes when the buffer exceeds 32 KB or
//! 100 ms have elapsed — whichever comes first.
//!
//! Reconnects transparently on write failure: buffered data is preserved
//! across reconnects and retried up to 3 times before being dropped.

use common::{MarketTick, OrderSignal};
use std::fmt::Write as FmtWrite;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{info, warn};

const BUF_LIMIT: usize = 32 * 1024;
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

pub struct IlpWriter {
    addr: String,
    stream: Option<TcpStream>,
    buf: String,
    last_flush: Instant,
}

impl IlpWriter {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            stream: None,
            buf: String::with_capacity(BUF_LIMIT + 4096),
            last_flush: Instant::now(),
        }
    }

    /// Append a market tick as an ILP line.
    /// Silently drops the tick if any price field is non-finite.
    pub fn push_tick(&mut self, tick: &MarketTick) {
        if !tick.bid.is_finite() || !tick.ask.is_finite() {
            return;
        }
        write!(
            self.buf,
            "market_ticks,symbol={sym} bid={bid:.8},ask={ask:.8},mid={mid:.8},spread={spread:.8},seq={seq}i {ts}\n",
            sym    = tick.symbol_str(),
            bid    = tick.bid,
            ask    = tick.ask,
            mid    = tick.mid(),
            spread = tick.spread(),
            seq    = tick.seq,
            ts     = tick.timestamp_ns,
        )
        .unwrap();
    }

    /// Append an order signal as an ILP line.
    /// Computes decision_ns, submission_ns, and e2e_ns from the embedded timestamps.
    pub fn push_order(&mut self, order: &OrderSignal, submitted_ns: u64) {
        if !order.quantity.is_finite() || !order.limit_price.is_finite() {
            return;
        }
        let decision_ns   = order.signal_ns.saturating_sub(order.tick_ns);
        let submission_ns = submitted_ns.saturating_sub(order.signal_ns);
        let e2e_ns        = submitted_ns.saturating_sub(order.tick_ns);
        write!(
            self.buf,
            "order_signals,symbol={sym},side={side} qty={qty:.8},price={price:.8},decision_ns={dec}i,submission_ns={sub}i,e2e_ns={e2e}i,seq={seq}i {ts}\n",
            sym   = order.symbol_str(),
            side  = order.side_str(),
            qty   = order.quantity,
            price = order.limit_price,
            dec   = decision_ns,
            sub   = submission_ns,
            e2e   = e2e_ns,
            seq   = order.seq,
            ts    = order.signal_ns,
        )
        .unwrap();
    }

    /// True when the buffer should be flushed (size or age threshold reached).
    pub fn needs_flush(&self) -> bool {
        self.buf.len() >= BUF_LIMIT || self.last_flush.elapsed() >= FLUSH_INTERVAL
    }

    /// Flush pending lines to QuestDB, reconnecting if necessary.
    /// Retries up to 3 times; drops buffered data after exhausting retries.
    pub async fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        for attempt in 0u8..3 {
            if self.stream.is_none() {
                self.connect().await;
            }
            if let Some(ref mut s) = self.stream {
                match s.write_all(self.buf.as_bytes()).await {
                    Ok(_) => {
                        self.buf.clear();
                        self.last_flush = Instant::now();
                        return;
                    }
                    Err(e) => {
                        warn!(error = %e, attempt, "QuestDB write failed, will reconnect");
                        self.stream = None;
                    }
                }
            }
        }
        warn!(bytes = self.buf.len(), "dropping ILP data after 3 failed flush attempts");
        self.buf.clear();
        self.last_flush = Instant::now();
    }

    async fn connect(&mut self) {
        match TcpStream::connect(&self.addr).await {
            Ok(s) => {
                info!(addr = %self.addr, "connected to QuestDB ILP");
                self.stream = Some(s);
            }
            Err(e) => {
                warn!(error = %e, addr = %self.addr, "QuestDB ILP connection failed");
            }
        }
    }
}
