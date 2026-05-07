use tokio::sync::mpsc;

use crate::event_bus::EventBus;
use crate::metrics::AppMetrics;
use crate::types::DataEvent;

pub struct EventRouter {
    source_rx: mpsc::Receiver<DataEvent>,
    agg_tx: mpsc::Sender<DataEvent>,
    bus: EventBus,
    metrics: std::sync::Arc<AppMetrics>,
}

impl EventRouter {
    pub fn new(
        source_rx: mpsc::Receiver<DataEvent>,
        agg_tx: mpsc::Sender<DataEvent>,
        bus: EventBus,
        metrics: std::sync::Arc<AppMetrics>,
    ) -> Self {
        Self {
            source_rx,
            agg_tx,
            bus,
            metrics,
        }
    }

    pub async fn run(mut self) {
        while let Some(event) = self.source_rx.recv().await {
            if matches!(&event, DataEvent::Tick(_)) {
                self.metrics.ticks_ingested_total.inc();
            }
            self.bus.publish_from_event(&event).await;
            if matches!(&event, DataEvent::Tick(_)) {
                self.metrics.bus_publish_total.inc();
            }
            if self.agg_tx.send(event).await.is_err() {
                break;
            }
        }
    }
}
