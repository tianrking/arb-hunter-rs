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

pub struct BybitSpotTicker {
    pub symbols: Vec<String>,
}
impl BybitSpotTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[derive(Deserialize)]
struct BybitMsg {
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    ret_msg: Option<String>,
    #[serde(default)]
    data: Option<BybitTickData>,
}

#[derive(Deserialize)]
struct BybitTickData {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "bid1Price")]
    bid: String,
    #[serde(rename = "ask1Price")]
    ask: String,
}

#[async_trait]
impl ExchangeSource for BybitSpotTicker {
    fn name(&self) -> &'static str {
        "bybit"
    }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("bybit spot symbols empty");
        }

        let (ws, _) = connect_async("wss://stream.bybit.com/v5/public/spot").await?;
        let (mut sink, mut stream) = ws.split();

        let topics = self
            .symbols
            .iter()
            .map(|s| format!("tickers.{}", s))
            .collect::<Vec<_>>();
        sink.send(Message::Text(
            json!({"op":"subscribe","args":topics}).to_string().into(),
        ))
        .await?;

        let mut ping_tick = interval(Duration::from_secs(20));
        let mut last_pong = Instant::now();
        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_pong.elapsed() > Duration::from_secs(60) { anyhow::bail!("bybit pong timeout"); }
                    sink.send(Message::Text(json!({"op":"ping"}).to_string().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("bybit stream ended")??;
                    match msg {
                        Message::Text(t) => {
                            let parsed = serde_json::from_str::<BybitMsg>(&t);
                            if let Ok(m) = parsed {
                                if m.op.as_deref() == Some("pong") || m.ret_msg.as_deref() == Some("pong") {
                                    last_pong = Instant::now();
                                    continue;
                                }
                                if let Some(d) = m.data {
                                    emit_tick(&ctx, self.name(), MarketKind::Spot, &d.symbol, &d.bid, &d.ask).await?;
                                }
                            }
                        }
                        Message::Pong(_) => last_pong = Instant::now(),
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Close(_) => anyhow::bail!("bybit closed"),
                        Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
            }
        }
    }
}
