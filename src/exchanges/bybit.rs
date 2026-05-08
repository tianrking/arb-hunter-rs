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

// ── Shared run loop ───────────────────────────────────────────────────

#[derive(Deserialize)]
struct BybitMsg {
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    ret_msg: Option<String>,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

pub async fn run_bybit(
    url: &str,
    exchange: &'static str,
    market: MarketKind,
    symbols: &[String],
    ctx: SourceContext,
) -> Result<()> {
    let label = if market == MarketKind::Spot {
        "spot"
    } else {
        "perp"
    };
    if symbols.is_empty() {
        anyhow::bail!("bybit {label} symbols empty");
    }

    let (ws, _) = connect_async(url).await?;
    let (mut sink, mut stream) = ws.split();

    let topics = symbols
        .iter()
        .map(|s| format!("tickers.{s}"))
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
                if last_pong.elapsed() > Duration::from_secs(60) {
                    anyhow::bail!("bybit {label} pong timeout");
                }
                sink.send(Message::Text(json!({"op":"ping"}).to_string().into())).await?;
                ctx.emit(DataEvent::Heartbeat { exchange, ts_ms: now_ms() }).await?;
            }
            msg = stream.next() => {
                let msg = msg.context(format!("bybit {label} stream ended"))??;
                match msg {
                    Message::Text(t) => {
                        if let Ok(m) = serde_json::from_str::<BybitMsg>(&t) {
                            if m.op.as_deref() == Some("pong") || m.ret_msg.as_deref() == Some("pong") {
                                last_pong = Instant::now();
                                continue;
                            }
                            if let Some(d) = m.data {
                                let symbol = d.get("symbol").and_then(|x| x.as_str()).unwrap_or("UNKNOWN");
                                let bid = d.get("bid1Price").and_then(|x| x.as_str()).unwrap_or("0");
                                let ask = d.get("ask1Price").and_then(|x| x.as_str()).unwrap_or("0");
                                let ts = d.get("ts").and_then(|x| x.as_u64());
                                if bid != "0" && ask != "0" {
                                    let mark = if market == MarketKind::Perp {
                                        d.get("markPrice").and_then(|x| x.as_str())
                                    } else { None };
                                    let funding = if market == MarketKind::Perp {
                                        d.get("fundingRate").and_then(|x| x.as_str())
                                    } else { None };
                                    emit_tick_ext(&ctx, exchange, market, symbol, bid, ask, mark, funding, ts).await?;
                                }
                            }
                        }
                    }
                    Message::Pong(_) => last_pong = Instant::now(),
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Close(_) => anyhow::bail!("bybit {label} closed"),
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

// ── Spot ──────────────────────────────────────────────────────────────

pub struct BybitSpotTicker {
    pub symbols: Vec<String>,
}
impl BybitSpotTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[async_trait]
impl ExchangeSource for BybitSpotTicker {
    fn name(&self) -> &'static str {
        "bybit"
    }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_bybit(
            "wss://stream.bybit.com/v5/public/spot",
            self.name(),
            MarketKind::Spot,
            &self.symbols,
            ctx,
        )
        .await
    }
}
