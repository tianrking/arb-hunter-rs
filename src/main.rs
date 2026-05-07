mod aggregator;
mod config;
mod exchanges;
mod runtime;
mod source;
mod types;

use aggregator::SpreadAggregator;
use config::AppConfig;
use exchanges::registry::build_sources;
use runtime::SourceRuntime;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

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
    let mut agg_task = tokio::spawn(SpreadAggregator::from_config(&cfg).run(handle.rx));

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

    for t in tasks.drain(..) {
        let _ = t.await;
    }

    Ok(())
}
