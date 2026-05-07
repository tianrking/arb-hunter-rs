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

pub struct KrakenTicker {
    pub symbols: Vec<String>,
}
impl KrakenTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[derive(Deserialize)]
struct KMsg {
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    data: Vec<KData>,
}

#[derive(Deserialize)]
struct KData {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    bid: Option<String>,
    #[serde(default)]
    ask: Option<String>,
}

#[async_trait]
impl ExchangeSource for KrakenTicker {
    fn name(&self) -> &'static str {
        "kraken"
    }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("kraken symbols empty");
        }

        let (ws, _) = connect_async("wss://ws.kraken.com/v2").await?;
        let (mut sink, mut stream) = ws.split();
        sink.send(Message::Text(
            json!({
                "method":"subscribe",
                "params": {"channel":"ticker", "symbol": self.symbols}
            })
            .to_string()
            .into(),
        ))
        .await?;

        let mut ping_tick = interval(Duration::from_secs(20));
        let mut last_pong = Instant::now();
        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_pong.elapsed() > Duration::from_secs(90) { anyhow::bail!("kraken heartbeat timeout"); }
                    sink.send(Message::Text(json!({"method":"ping"}).to_string().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("kraken stream ended")??;
                    match msg {
                        Message::Text(t) => {
                            if let Ok(v) = serde_json::from_str::<KMsg>(&t) {
                                if v.method.as_deref() == Some("pong") {
                                    last_pong = Instant::now();
                                    continue;
                                }
                                if v.channel.as_deref() == Some("ticker") {
                                    for d in v.data {
                                        if let (Some(symbol), Some(bid), Some(ask)) = (d.symbol.as_deref(), d.bid.as_deref(), d.ask.as_deref()) {
                                            emit_tick(&ctx, self.name(), MarketKind::Spot, symbol, bid, ask).await?;
                                        }
                                    }
                                }
                            }
                        }
                        Message::Pong(_) => last_pong = Instant::now(),
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Close(_) => anyhow::bail!("kraken closed"),
                        Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
            }
        }
    }
}
