use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{RwLock, broadcast};

use crate::types::{DataEvent, MarketKind, now_ms};

pub const SCHEMA_VERSION: &str = "v1";

#[derive(Debug, Clone, Serialize)]
pub struct NormalizedTick {
    pub version: &'static str,
    pub exchange: &'static str,
    pub market: &'static str,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub mark: Option<f64>,
    pub funding: Option<f64>,
    pub ts: u64,
    pub source_latency_ms: u64,
    pub stale: bool,
}

#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<NormalizedTick>,
    snapshots: Arc<RwLock<HashMap<String, NormalizedTick>>>,
    stale_ttl_ms: u64,
}

impl EventBus {
    pub fn new(capacity: usize, stale_ttl_ms: u64) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            stale_ttl_ms,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<NormalizedTick> {
        self.tx.subscribe()
    }

    pub async fn publish_from_event(&self, event: &DataEvent) {
        if let DataEvent::Tick(t) = event {
            let now = now_ms();
            let latency = now.saturating_sub(t.ts_ms);
            let normalized = NormalizedTick {
                version: SCHEMA_VERSION,
                exchange: t.exchange,
                market: market_to_str(t.market),
                symbol: t.symbol.to_string(),
                bid: t.bid,
                ask: t.ask,
                mark: t.mark,
                funding: t.funding_rate,
                ts: t.ts_ms,
                source_latency_ms: latency,
                stale: latency > self.stale_ttl_ms,
            };

            let key = snapshot_key(&normalized.exchange, normalized.market, &normalized.symbol);
            {
                let mut guard = self.snapshots.write().await;
                guard.insert(key, normalized.clone());
            }
            let _ = self.tx.send(normalized);
        }
    }

    pub async fn snapshot_by_symbol(&self, symbol: &str) -> Vec<NormalizedTick> {
        let needle = symbol.to_ascii_uppercase();
        let guard = self.snapshots.read().await;
        guard
            .values()
            .filter(|t| t.symbol.eq_ignore_ascii_case(&needle))
            .cloned()
            .collect()
    }

    pub async fn snapshot_all(&self) -> Vec<NormalizedTick> {
        let guard = self.snapshots.read().await;
        guard.values().cloned().collect()
    }
}

fn market_to_str(m: MarketKind) -> &'static str {
    match m {
        MarketKind::Spot => "spot",
        MarketKind::Perp => "perp",
    }
}

fn snapshot_key(exchange: &str, market: &str, symbol: &str) -> String {
    format!("{exchange}:{market}:{symbol}")
}
