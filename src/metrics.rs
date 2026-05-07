use std::sync::Arc;

use prometheus::{Encoder, IntCounter, IntGauge, Registry, TextEncoder};

#[derive(Clone)]
pub struct AppMetrics {
    registry: Registry,
    pub ticks_ingested_total: IntCounter,
    pub bus_publish_total: IntCounter,
    pub ws_subscribers: IntGauge,
    pub redis_xadd_total: IntCounter,
}

impl AppMetrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let ticks_ingested_total = IntCounter::new("ticks_ingested_total", "Total ingested ticks").unwrap();
        let bus_publish_total = IntCounter::new("bus_publish_total", "Total normalized events published to bus").unwrap();
        let ws_subscribers = IntGauge::new("ws_subscribers", "Current websocket subscribers").unwrap();
        let redis_xadd_total = IntCounter::new("redis_xadd_total", "Total redis xadd writes").unwrap();

        registry.register(Box::new(ticks_ingested_total.clone())).unwrap();
        registry.register(Box::new(bus_publish_total.clone())).unwrap();
        registry.register(Box::new(ws_subscribers.clone())).unwrap();
        registry.register(Box::new(redis_xadd_total.clone())).unwrap();

        Arc::new(Self {
            registry,
            ticks_ingested_total,
            bus_publish_total,
            ws_subscribers,
            redis_xadd_total,
        })
    }

    pub fn render(&self) -> String {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let _ = encoder.encode(&metric_families, &mut buffer);
        String::from_utf8(buffer).unwrap_or_default()
    }
}
