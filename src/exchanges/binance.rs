use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

pub struct BinanceBookTicker {
    symbols: Vec<String>,
}

impl BinanceBookTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }

    fn ws_url(&self) -> String {
        let streams = self
            .symbols
            .iter()
            .map(|s| format!("{}@bookTicker", s.to_ascii_lowercase()))
            .collect::<Vec<_>>()
            .join("/");
        format!("wss://stream.binance.com:9443/stream?streams={streams}")
    }
}

#[derive(Debug, Deserialize)]
struct BinanceCombined<'a> {
    #[serde(borrow)]
    data: BinanceBookTickerMsg<'a>,
}

#[derive(Debug, Deserialize)]
struct BinanceBookTickerMsg<'a> {
    #[serde(borrow, rename = "s")]
    symbol: &'a str,
    #[serde(borrow, rename = "b")]
    bid: &'a str,
    #[serde(borrow, rename = "a")]
    ask: &'a str,
}

#[async_trait]
impl ExchangeSource for BinanceBookTicker {
    fn name(&self) -> &'static str {
        "binance"
    }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("binance symbols empty");
        }

        let (ws, _) = connect_async(self.ws_url())
            .await
            .context("binance connect failed")?;
        let (mut sink, mut stream) = ws.split();
        let mut ping_tick = interval(Duration::from_secs(15));
        let mut last_pong = Instant::now();

        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_pong.elapsed() > Duration::from_secs(60) { anyhow::bail!("binance pong timeout"); }
                    sink.send(Message::Ping(Vec::new().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("binance stream ended")??;
                    match msg {
                        Message::Text(text) => {
                            if let Ok(parsed) = serde_json::from_str::<BinanceCombined<'_>>(&text) {
                                emit_tick(&ctx, self.name(), MarketKind::Spot, parsed.data.symbol, parsed.data.bid, parsed.data.ask).await?;
                            }
                        }
                        Message::Pong(_) => last_pong = Instant::now(),
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Close(_) => anyhow::bail!("binance closed"),
                        Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
            }
        }
    }
}
