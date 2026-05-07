use async_trait::async_trait;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick_ext;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

pub struct GatePerpBookTicker {
    pub symbols: Vec<String>,
}
impl GatePerpBookTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[derive(Deserialize)]
struct GateFuturesMsg {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    result: Option<GateFuturesResult>,
}

#[derive(Deserialize)]
struct GateFuturesResult {
    #[serde(default)]
    contract: Option<String>,
    #[serde(default)]
    highest_bid: Option<String>,
    #[serde(default)]
    lowest_ask: Option<String>,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    time_ms: Option<u64>,
}

#[async_trait]
impl ExchangeSource for GatePerpBookTicker {
    fn name(&self) -> &'static str {
        "gate"
    }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("gate perp symbols empty");
        }

        let (ws, _) = connect_async("wss://fx-ws.gateio.ws/v4/ws/usdt").await?;
        let (mut sink, mut stream) = ws.split();
        sink.send(Message::Text(
            json!({
                "time": now_ms()/1000,
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": self.symbols
            })
            .to_string()
            .into(),
        ))
        .await?;

        let mut ping_tick = interval(Duration::from_secs(20));
        let mut last_seen = Instant::now();
        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_seen.elapsed() > Duration::from_secs(90) { anyhow::bail!("gate perp heartbeat timeout"); }
                    sink.send(Message::Text(json!({"time": now_ms()/1000, "channel":"futures.ping"}).to_string().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("gate perp stream ended")??;
                    match msg {
                        Message::Text(t) => {
                            last_seen = Instant::now();
                            if let Ok(v) = serde_json::from_str::<GateFuturesMsg>(&t)
                                && v.channel.as_deref() == Some("futures.book_ticker")
                                && let Some(r) = v.result
                                && let (Some(symbol), Some(bid), Some(ask)) =
                                    (r.contract.as_deref(), r.highest_bid.as_deref(), r.lowest_ask.as_deref()) {
                                emit_tick_ext(
                                    &ctx,
                                    self.name(),
                                    MarketKind::Perp,
                                    symbol,
                                    bid,
                                    ask,
                                    r.mark_price.as_deref(),
                                    r.funding_rate.as_deref(),
                                    r.time_ms,
                                ).await?;
                            }
                        }
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Pong(_) => last_seen = Instant::now(),
                        Message::Binary(_) | Message::Frame(_) => {}
                        Message::Close(_) => anyhow::bail!("gate perp closed"),
                    }
                }
            }
        }
    }
}
