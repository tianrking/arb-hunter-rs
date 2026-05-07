use async_trait::async_trait;
use std::io::{Cursor, Read};
use std::time::Duration;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

pub struct HtxBbo {
    pub symbols: Vec<String>,
}
impl HtxBbo {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[async_trait]
impl ExchangeSource for HtxBbo {
    fn name(&self) -> &'static str {
        "htx"
    }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("htx symbols empty");
        }

        let (ws, _) = connect_async("wss://api.huobi.pro/ws").await?;
        let (mut sink, mut stream) = ws.split();

        for s in &self.symbols {
            let ch = format!("market.{}.bbo", s.to_ascii_lowercase());
            sink.send(Message::Text(
                json!({"sub": ch, "id": s}).to_string().into(),
            ))
            .await?;
        }

        let mut ping_tick = interval(Duration::from_secs(20));
        let mut last_seen = Instant::now();
        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_seen.elapsed() > Duration::from_secs(90) { anyhow::bail!("htx heartbeat timeout"); }
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("htx stream ended")??;
                    match msg {
                        Message::Binary(bin) => {
                            let mut d = GzDecoder::new(Cursor::new(bin));
                            let mut s = String::new();
                            d.read_to_string(&mut s)?;
                            last_seen = Instant::now();
                            if let Ok(v) = serde_json::from_str::<Value>(&s) {
                                if let Some(ping) = v.get("ping").and_then(|x| x.as_i64()) {
                                    sink.send(Message::Text(json!({"pong": ping}).to_string().into())).await?;
                                    continue;
                                }
                                let ch = v.get("ch").and_then(|x| x.as_str()).unwrap_or("");
                                let symbol = ch.split('.').nth(1).unwrap_or("UNKNOWN");
                                let bid = v.pointer("/tick/bid/0").and_then(|x| x.as_f64()).map(|x| x.to_string());
                                let ask = v.pointer("/tick/ask/0").and_then(|x| x.as_f64()).map(|x| x.to_string());
                                if let (Some(bid), Some(ask)) = (bid.as_deref(), ask.as_deref()) {
                                    emit_tick(&ctx, self.name(), MarketKind::Spot, symbol, bid, ask).await?;
                                }
                            }
                        }
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Pong(_) => last_seen = Instant::now(),
                        Message::Text(_) | Message::Frame(_) => {}
                        Message::Close(_) => anyhow::bail!("htx closed"),
                    }
                }
            }
        }
    }
}
