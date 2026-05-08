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

// ── Shared parsing ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BitgetMsg {
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    arg: Option<BitgetArg>,
    #[serde(default)]
    data: Vec<BitgetTick>,
}

#[derive(Deserialize)]
struct BitgetArg {
    #[serde(rename = "instId")]
    inst_id: Option<String>,
}

#[derive(Deserialize)]
struct BitgetTick {
    #[serde(rename = "bidPr")]
    bid: String,
    #[serde(rename = "askPr")]
    ask: String,
    #[serde(rename = "instId")]
    inst_id: Option<String>,
    #[serde(rename = "markPrice")]
    mark: Option<String>,
    #[serde(rename = "fundingRate")]
    funding: Option<String>,
    #[serde(rename = "ts")]
    ts: Option<String>,
}

// ── Shared run loop ───────────────────────────────────────────────────

pub async fn run_bitget(
    inst_type: &str,
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
        anyhow::bail!("bitget {label} symbols empty");
    }

    let (ws, _) = connect_async("wss://ws.bitget.com/v2/ws/public").await?;
    let (mut sink, mut stream) = ws.split();

    let args = symbols
        .iter()
        .map(|s| json!({"instType": inst_type, "channel": "ticker", "instId": s}))
        .collect::<Vec<_>>();
    sink.send(Message::Text(
        json!({"op":"subscribe","args":args}).to_string().into(),
    ))
    .await?;

    let mut ping_tick = interval(Duration::from_secs(25));
    let mut last_seen = Instant::now();

    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                if last_seen.elapsed() > Duration::from_secs(90) {
                    anyhow::bail!("bitget {label} heartbeat timeout");
                }
                sink.send(Message::Text("ping".into())).await?;
                ctx.emit(DataEvent::Heartbeat { exchange, ts_ms: now_ms() }).await?;
            }
            msg = stream.next() => {
                let msg = msg.context(format!("bitget {label} stream ended"))??;
                match msg {
                    Message::Text(t) => {
                        if t == "pong" { last_seen = Instant::now(); continue; }
                        if let Ok(m) = serde_json::from_str::<BitgetMsg>(&t) {
                            if m.op.as_deref() == Some("pong") || m.action.as_deref() == Some("pong") {
                                last_seen = Instant::now();
                                continue;
                            }
                            let arg_inst = m.arg.and_then(|a| a.inst_id);
                            for d in m.data {
                                let symbol = d.inst_id.as_deref().or(arg_inst.as_deref()).unwrap_or("UNKNOWN");
                                let mark = if market == MarketKind::Perp { d.mark.as_deref() } else { None };
                                let funding = if market == MarketKind::Perp { d.funding.as_deref() } else { None };
                                emit_tick_ext(
                                    &ctx, exchange, market, symbol, &d.bid, &d.ask,
                                    mark, funding,
                                    d.ts.as_deref().and_then(|x| x.parse::<u64>().ok()),
                                ).await?;
                            }
                        }
                        last_seen = Instant::now();
                    }
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Pong(_) => last_seen = Instant::now(),
                    Message::Binary(_) | Message::Frame(_) => {}
                    Message::Close(_) => anyhow::bail!("bitget {label} closed"),
                }
            }
        }
    }
}

// ── Spot ──────────────────────────────────────────────────────────────

pub struct BitgetSpotTicker {
    pub symbols: Vec<String>,
}
impl BitgetSpotTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[async_trait]
impl ExchangeSource for BitgetSpotTicker {
    fn name(&self) -> &'static str {
        "bitget"
    }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_bitget("SPOT", self.name(), MarketKind::Spot, &self.symbols, ctx).await
    }
}
