use async_trait::async_trait;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick_ext;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

pub struct BinancePerpBookTicker {
    symbols: Vec<String>,
}

impl BinancePerpBookTicker {
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
        format!("wss://fstream.binance.com/stream?streams={streams}")
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
    #[serde(rename = "E")]
    event_ts: Option<u64>,
}

#[async_trait]
impl ExchangeSource for BinancePerpBookTicker {
    fn name(&self) -> &'static str {
        "binance"
    }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("binance perp symbols empty");
        }

        let (ws, _) = connect_async(self.ws_url())
            .await
            .context("binance perp connect failed")?;
        let (mut sink, mut stream) = ws.split();
        let mut ping_tick = interval(Duration::from_secs(15));
        let mut last_pong = Instant::now();

        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_pong.elapsed() > Duration::from_secs(60) { anyhow::bail!("binance perp pong timeout"); }
                    sink.send(Message::Ping(Vec::new().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("binance perp stream ended")??;
                    match msg {
                        Message::Text(text) => {
                            if let Ok(parsed) = serde_json::from_str::<BinanceCombined<'_>>(&text) {
                                emit_tick_ext(
                                    &ctx,
                                    self.name(),
                                    MarketKind::Perp,
                                    parsed.data.symbol,
                                    parsed.data.bid,
                                    parsed.data.ask,
                                    None,
                                    None,
                                    parsed.data.event_ts,
                                ).await?;
                            }
                        }
                        Message::Pong(_) => last_pong = Instant::now(),
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Close(_) => anyhow::bail!("binance perp closed"),
                        Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
            }
        }
    }
}
