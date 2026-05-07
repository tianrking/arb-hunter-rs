use async_trait::async_trait;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

pub struct GateSpotBookTicker {
    pub symbols: Vec<String>,
}
impl GateSpotBookTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[derive(Deserialize)]
struct GateMsg {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    result: Option<GateResult>,
}

#[derive(Deserialize)]
struct GateResult {
    #[serde(default)]
    s: Option<String>,
    #[serde(default)]
    b: Option<String>,
    #[serde(default)]
    a: Option<String>,
}

#[async_trait]
impl ExchangeSource for GateSpotBookTicker {
    fn name(&self) -> &'static str {
        "gate"
    }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("gate symbols empty");
        }

        let (ws, _) = connect_async("wss://api.gateio.ws/ws/v4/").await?;
        let (mut sink, mut stream) = ws.split();
        sink.send(Message::Text(
            json!({
                "time": now_ms()/1000,
                "channel": "spot.book_ticker",
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
                    if last_seen.elapsed() > Duration::from_secs(90) { anyhow::bail!("gate heartbeat timeout"); }
                    sink.send(Message::Text(json!({"time": now_ms()/1000, "channel":"spot.ping"}).to_string().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("gate stream ended")??;
                    match msg {
                        Message::Text(t) => {
                            last_seen = Instant::now();
                            if let Ok(v) = serde_json::from_str::<GateMsg>(&t)
                                && v.channel.as_deref() == Some("spot.book_ticker")
                                && let Some(r) = v.result
                                && let (Some(symbol), Some(bid), Some(ask)) = (r.s.as_deref(), r.b.as_deref(), r.a.as_deref()) {
                                emit_tick(&ctx, self.name(), MarketKind::Spot, symbol, bid, ask).await?;
                            }
                        }
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Pong(_) => last_seen = Instant::now(),
                        Message::Binary(_) | Message::Frame(_) => {}
                        Message::Close(_) => anyhow::bail!("gate closed"),
                    }
                }
            }
        }
    }
}
