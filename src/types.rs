use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketKind {
    Spot,
    Perp,
}

#[derive(Debug, Clone)]
pub struct MarketTick {
    pub exchange: &'static str,
    pub market: MarketKind,
    pub symbol: Box<str>,
    pub bid: f64,
    pub ask: f64,
    pub mark: Option<f64>,
    pub funding_rate: Option<f64>,
    pub ts_ms: u64,
}

#[derive(Debug, Clone)]
pub enum DataEvent {
    Tick(MarketTick),
    Heartbeat {
        exchange: &'static str,
        ts_ms: u64,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum BackpressureMode {
    Block,
    DropNewest,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
