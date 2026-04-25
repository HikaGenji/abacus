use bytemuck::{Pod, Zeroable};

/// A single market tick received from the exchange feed.
///
/// `#[repr(C)]` + `Pod` are required for iceoryx2 zero-copy shared-memory transfer.
/// All fields are fixed-size primitives — no heap allocations, no pointers.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct MarketTick {
    /// ASCII symbol padded with zeros, e.g. b"BTCUSD\0\0"
    pub symbol: [u8; 8],
    pub bid: f64,
    pub ask: f64,
    /// nanoseconds since UNIX epoch (from the exchange feed publisher)
    pub timestamp_ns: u64,
    /// monotonically increasing sequence number
    pub seq: u64,
}

impl MarketTick {
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) * 0.5
    }

    pub fn spread(&self) -> f64 {
        self.ask - self.bid
    }

    pub fn symbol_str(&self) -> &str {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(8);
        std::str::from_utf8(&self.symbol[..end]).unwrap_or("?")
    }
}

/// An order signal produced by the strategy engine.
///
/// Carries the original tick timestamp so downstream components can compute
/// end-to-end tick-to-order latency.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct OrderSignal {
    pub symbol: [u8; 8],
    /// 0 = buy, 1 = sell
    pub side: u8,
    pub _pad: [u8; 7],
    pub quantity: f64,
    pub limit_price: f64,
    /// nanoseconds since UNIX epoch — set by strategy when signal fires
    pub signal_ns: u64,
    /// original tick timestamp, forwarded for e2e latency measurement
    pub tick_ns: u64,
    pub seq: u64,
}

impl OrderSignal {
    pub fn symbol_str(&self) -> &str {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(8);
        std::str::from_utf8(&self.symbol[..end]).unwrap_or("?")
    }

    pub fn side_str(&self) -> &str {
        if self.side == 0 { "BUY" } else { "SELL" }
    }
}

pub fn symbol_bytes(s: &str) -> [u8; 8] {
    let mut buf = [0u8; 8];
    let bytes = s.as_bytes();
    let len = bytes.len().min(8);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

pub fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_mid_and_spread() {
        let mut tick = MarketTick::zeroed();
        tick.bid = 29_990.0;
        tick.ask = 30_010.0;
        assert!((tick.mid() - 30_000.0).abs() < 1e-9);
        assert!((tick.spread() - 20.0).abs() < 1e-9);
    }

    #[test]
    fn symbol_roundtrip() {
        let sym = symbol_bytes("BTCUSD");
        let mut tick = MarketTick::zeroed();
        tick.symbol = sym;
        assert_eq!(tick.symbol_str(), "BTCUSD");
    }

    #[test]
    fn pod_sizes() {
        // MarketTick: 8+8+8+8+8 = 40 bytes
        assert_eq!(std::mem::size_of::<MarketTick>(), 40);
        // OrderSignal: 8+1+7+8+8+8+8+8 = 56 bytes
        assert_eq!(std::mem::size_of::<OrderSignal>(), 56);
    }
}
