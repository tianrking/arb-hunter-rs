use async_trait::async_trait;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::{emit_tick, emit_tick_ext};
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

// ── Spot ──────────────────────────────────────────────────────────────

pub struct BinanceBookTicker {
    symbols: Vec<String>,
}

impl BinanceBookTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
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

pub async fn run_binance(
    url: &str,
    exchange: &'static str,
    market: MarketKind,
    symbols: &[String],
    ctx: SourceContext,
) -> Result<()> {
    if symbols.is_empty() {
        anyhow::bail!(
            "binance {} symbols empty",
            if market == MarketKind::Spot {
                "spot"
            } else {
                "perp"
            }
        );
    }

    let streams = symbols
        .iter()
        .map(|s| format!("{}@bookTicker", s.to_ascii_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let ws_url = format!("{url}streams={streams}");

    let (ws, _) = connect_async(&ws_url)
        .await
        .with_context(|| format!("binance {} connect failed", market_label(market)))?;
    let (mut sink, mut stream) = ws.split();
    let mut ping_tick = interval(Duration::from_secs(15));
    let mut last_pong = Instant::now();

    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                if last_pong.elapsed() > Duration::from_secs(60) {
                    anyhow::bail!("binance {} pong timeout", market_label(market));
                }
                sink.send(Message::Ping(Vec::new().into())).await?;
                ctx.emit(DataEvent::Heartbeat { exchange, ts_ms: now_ms() }).await?;
            }
            msg = stream.next() => {
                let msg = msg.context(format!("binance {} stream ended", market_label(market)))??;
                match msg {
                    Message::Text(text) => {
                        if let Ok(parsed) = serde_json::from_str::<BinanceCombined<'_>>(&text) {
                            match market {
                                MarketKind::Spot => {
                                    emit_tick(&ctx, exchange, market, parsed.data.symbol, parsed.data.bid, parsed.data.ask).await?;
                                }
                                MarketKind::Perp => {
                                    // Perp stream includes event timestamp; spot does not
                                    emit_tick_ext(&ctx, exchange, market, parsed.data.symbol, parsed.data.bid, parsed.data.ask, None, None, None).await?;
                                }
                            }
                        }
                    }
                    Message::Pong(_) => last_pong = Instant::now(),
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Close(_) => anyhow::bail!("binance {} closed", market_label(market)),
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

fn market_label(m: MarketKind) -> &'static str {
    match m {
        MarketKind::Spot => "spot",
        MarketKind::Perp => "perp",
    }
}

#[async_trait]
impl ExchangeSource for BinanceBookTicker {
    fn name(&self) -> &'static str {
        "binance"
    }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_binance(
            "wss://stream.binance.com:9443/stream?",
            self.name(),
            MarketKind::Spot,
            &self.symbols,
            ctx,
        )
        .await
    }
}
