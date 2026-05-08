use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::metrics::AppMetrics;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{BackpressureMode, DataEvent};

pub struct RuntimeHandle {
    pub rx: mpsc::Receiver<DataEvent>,
    pub shutdown: CancellationToken,
    pub tasks: Vec<JoinHandle<()>>,
}

pub struct SourceRuntime {
    pub queue_capacity: usize,
    pub backpressure: BackpressureMode,
    pub max_backoff: Duration,
    pub metrics: Arc<AppMetrics>,
}

impl SourceRuntime {
    pub fn new(queue_capacity: usize, backpressure: BackpressureMode, metrics: Arc<AppMetrics>) -> Self {
        Self {
            queue_capacity,
            backpressure,
            max_backoff: Duration::from_secs(30),
            metrics,
        }
    }

    pub fn spawn_sources(&self, sources: Vec<Arc<dyn ExchangeSource>>) -> RuntimeHandle {
        let (tx, rx) = mpsc::channel(self.queue_capacity);
        let shutdown = CancellationToken::new();
        let mut tasks = Vec::with_capacity(sources.len());

        for source in sources {
            let ctx = SourceContext {
                tx: tx.clone(),
                backpressure: self.backpressure,
                metrics: self.metrics.clone(),
            };
            let max_backoff = self.max_backoff;
            let stop = shutdown.clone();
            let source = source.clone();
            let task = tokio::spawn(async move {
                let mut delay = Duration::from_secs(1);
                loop {
                    if stop.is_cancelled() {
                        break;
                    }

                    info!(exchange = source.name(), "source start");
                    let run_ctx = ctx.clone();
                    let source_run = source.clone();
                    let mut run_task = tokio::spawn(async move { source_run.run(run_ctx).await });

                    let ran_ok = tokio::select! {
                        _ = stop.cancelled() => {
                            run_task.abort();
                            let _ = run_task.await;
                            break;
                        }
                        join = &mut run_task => {
                            match join {
                                Ok(Ok(())) => {
                                    warn!(exchange = source.name(), "source exited normally, reconnecting");
                                    true
                                }
                                Ok(Err(e)) => {
                                    error!(exchange = source.name(), error = %e, "source error");
                                    false
                                }
                                Err(e) => {
                                    error!(exchange = source.name(), error = %e, "source task panicked");
                                    false
                                }
                            }
                        }
                    };

                    if ran_ok {
                        delay = Duration::from_secs(1);
                    }

                    tokio::select! {
                        _ = stop.cancelled() => break,
                        _ = sleep(delay) => {}
                    }

                    delay = (delay * 2).min(max_backoff);
                }
            });
            tasks.push(task);
        }

        RuntimeHandle {
            rx,
            shutdown,
            tasks,
        }
    }
}
