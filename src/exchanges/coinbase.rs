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

pub struct CoinbaseTicker {
    pub product_ids: Vec<String>,
}
impl CoinbaseTicker {
    pub fn new(product_ids: Vec<String>) -> Self {
        Self { product_ids }
    }
}

#[derive(Deserialize)]
struct CbMsg {
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    product_id: Option<String>,
    #[serde(default)]
    best_bid: Option<String>,
    #[serde(default)]
    best_ask: Option<String>,
    #[serde(default)]
    events: Vec<CbEvent>,
}

#[derive(Deserialize)]
struct CbEvent {
    #[serde(default)]
    tickers: Vec<CbTicker>,
}

#[derive(Deserialize)]
struct CbTicker {
    #[serde(default)]
    product_id: Option<String>,
    #[serde(default)]
    best_bid: Option<String>,
    #[serde(default)]
    best_ask: Option<String>,
}

#[async_trait]
impl ExchangeSource for CoinbaseTicker {
    fn name(&self) -> &'static str {
        "coinbase"
    }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.product_ids.is_empty() {
            anyhow::bail!("coinbase product_ids empty");
        }

        let (ws, _) = connect_async("wss://advanced-trade-ws.coinbase.com").await?;
        let (mut sink, mut stream) = ws.split();

        sink.send(Message::Text(
            json!({
                "type":"subscribe",
                "channel":"ticker",
                "product_ids": self.product_ids
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
                    if last_pong.elapsed() > Duration::from_secs(90) { anyhow::bail!("coinbase heartbeat timeout"); }
                    sink.send(Message::Text(json!({"type":"ping"}).to_string().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("coinbase stream ended")??;
                    match msg {
                        Message::Text(t) => {
                            if let Ok(v) = serde_json::from_str::<CbMsg>(&t) {
                                if v.r#type.as_deref() == Some("pong") {
                                    last_pong = Instant::now();
                                    continue;
                                }

                                if let (Some(symbol), Some(bid), Some(ask)) = (v.product_id.as_deref(), v.best_bid.as_deref(), v.best_ask.as_deref()) {
                                    emit_tick(&ctx, self.name(), MarketKind::Spot, symbol, bid, ask).await?;
                                }

                                for e in v.events {
                                    for t in e.tickers {
                                        if let (Some(symbol), Some(bid), Some(ask)) = (t.product_id.as_deref(), t.best_bid.as_deref(), t.best_ask.as_deref()) {
                                            emit_tick(&ctx, self.name(), MarketKind::Spot, symbol, bid, ask).await?;
                                        }
                                    }
                                }
                            }
                        }
                        Message::Pong(_) => last_pong = Instant::now(),
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Close(_) => anyhow::bail!("coinbase closed"),
                        Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
            }
        }
    }
}
