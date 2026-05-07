mod aggregator;
mod config;
mod event_bus;
mod exchanges;
mod runtime;
mod source;
mod types;

use aggregator::SpreadAggregator;
use config::AppConfig;
use event_bus::EventBus;
use exchanges::registry::build_sources;
use runtime::SourceRuntime;
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use types::DataEvent;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cfg = AppConfig::load()?;
    let runtime = SourceRuntime::new(cfg.runtime.queue_capacity, cfg.backpressure_mode());
    let sources = build_sources(&cfg);

    let handle = runtime.spawn_sources(sources);
    let shutdown = handle.shutdown.clone();
    let mut tasks = handle.tasks;

    let bus = EventBus::new(8192, cfg.runtime.stale_ttl_ms);

    let (agg_tx, agg_rx) = mpsc::channel::<DataEvent>(cfg.runtime.queue_capacity);
    let bus_for_router = bus.clone();
    let mut source_rx = handle.rx;
    let mut router_task = tokio::spawn(async move {
        while let Some(event) = source_rx.recv().await {
            bus_for_router.publish_from_event(&event).await;
            if agg_tx.send(event).await.is_err() {
                break;
            }
        }
    });

    let mut agg_task = tokio::spawn(SpreadAggregator::from_config(&cfg).run(agg_rx));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c received, shutting down");
            shutdown.cancel();
        }
        res = &mut agg_task => {
            match res {
                Ok(()) => info!("aggregator exited"),
                Err(e) => error!(error = %e, "aggregator task failed"),
            }
            shutdown.cancel();
        }
    }

    router_task.abort();
    let _ = router_task.await;

    for t in tasks.drain(..) {
        let _ = t.await;
    }

    Ok(())
}
