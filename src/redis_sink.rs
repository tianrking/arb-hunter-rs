use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::event_bus::EventBus;
use crate::metrics::AppMetrics;

pub fn spawn_redis_sink(
    bus: EventBus,
    redis_url: String,
    stream_prefix: String,
    metrics: Arc<AppMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = bus.subscribe();

        let client = match redis::Client::open(redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "redis client open failed");
                return;
            }
        };

        let mut conn = loop {
            match client.get_multiplexed_tokio_connection().await {
                Ok(c) => {
                    info!("redis sink connected");
                    break c;
                }
                Err(e) => {
                    warn!(error = %e, "redis connect failed, retrying");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        };

        loop {
            match rx.recv().await {
                Ok(t) => {
                    let stream = format!("{}:{}", stream_prefix, t.symbol.to_ascii_uppercase());
                    let payload = serde_json::to_string(&t).unwrap_or_else(|_| "{}".to_string());
                    let mut wrote = false;
                    for _ in 0..2 {
                        let res: redis::RedisResult<String> = redis::cmd("XADD")
                            .arg(&stream)
                            .arg("*")
                            .arg("exchange").arg(t.exchange)
                            .arg("market").arg(t.market)
                            .arg("symbol").arg(&t.symbol)
                            .arg("ts").arg(t.ts as i64)
                            .arg("payload").arg(&payload)
                            .query_async(&mut conn)
                            .await;
                        match res {
                            Ok(_) => {
                                metrics.redis_xadd_total.inc();
                                wrote = true;
                                break;
                            }
                            Err(e) => {
                                warn!(error=%e, stream=%stream, "redis xadd failed, reconnecting");
                                match client.get_multiplexed_tokio_connection().await {
                                    Ok(c) => conn = c,
                                    Err(e2) => {
                                        warn!(error=%e2, "redis reconnect failed");
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                    }
                                }
                            }
                        }
                    }
                    if !wrote {
                        error!(stream=%stream, "redis xadd dropped event after retries");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped=n, "redis sink lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}
