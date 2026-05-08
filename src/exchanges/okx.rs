use async_trait::async_trait;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick_ext;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

// ── Shared types ──────────────────────────────────────────────────────

#[derive(Serialize)]
struct SubReq<'a> {
    op: &'a str,
    args: Vec<SubArg<'a>>,
}
#[derive(Serialize)]
struct SubArg<'a> {
    channel: &'a str,
    #[serde(rename = "instId")]
    inst_id: &'a str,
}

#[derive(Deserialize)]
struct Msg<'a> {
    #[serde(default)]
    arg: Option<Arg<'a>>,
    #[serde(default, borrow)]
    data: Vec<Tick<'a>>,
}
#[derive(Deserialize)]
struct Arg<'a> {
    #[serde(borrow, rename = "instId")]
    inst_id: &'a str,
}
#[derive(Deserialize)]
struct Tick<'a> {
    #[serde(borrow, rename = "bidPx")]
    bid: &'a str,
    #[serde(borrow, rename = "askPx")]
    ask: &'a str,
    #[serde(borrow, default, rename = "markPx")]
    mark: Option<&'a str>,
    #[serde(borrow, default, rename = "fundingRate")]
    funding: Option<&'a str>,
    #[serde(borrow, rename = "ts")]
    ts: Option<&'a str>,
}

// ── Shared run loop ───────────────────────────────────────────────────

pub async fn run_okx(exchange: &'static str, market: MarketKind, inst_ids: &[String], ctx: SourceContext) -> Result<()> {
    let label = if market == MarketKind::Spot { "spot" } else { "perp" };
    if inst_ids.is_empty() {
        anyhow::bail!("okx {label} symbols empty");
    }

    let (ws, _) = connect_async("wss://ws.okx.com:8443/ws/v5/public").await?;
    let (mut sink, mut stream) = ws.split();

    let args = inst_ids.iter()
        .map(|id| SubArg { channel: "tickers", inst_id: id.as_str() })
        .collect::<Vec<_>>();
    let sub = serde_json::to_string(&SubReq { op: "subscribe", args })?;
    sink.send(Message::Text(sub.into())).await?;

    let mut ping_tick = interval(Duration::from_secs(20));
    let mut last_pong = Instant::now();

    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                if last_pong.elapsed() > Duration::from_secs(60) {
                    anyhow::bail!("okx {label} pong timeout");
                }
                sink.send(Message::Text("ping".into())).await?;
                ctx.emit(DataEvent::Heartbeat { exchange, ts_ms: now_ms() }).await?;
            }
            msg = stream.next() => {
                let msg = msg.context(format!("okx {label} stream ended"))??;
                match msg {
                    Message::Text(t) => {
                        if t == "pong" { last_pong = Instant::now(); continue; }
                        if let Ok(parsed) = serde_json::from_str::<Msg<'_>>(&t)
                            && let Some(first) = parsed.data.first()
                            && let Some(arg) = parsed.arg
                        {
                            let (mark, funding) = if market == MarketKind::Perp {
                                (first.mark, first.funding)
                            } else {
                                (None, None)
                            };
                            emit_tick_ext(
                                &ctx, exchange, market, arg.inst_id,
                                first.bid, first.ask, mark, funding,
                                first.ts.and_then(|x| x.parse::<u64>().ok()),
                            ).await?;
                        }
                    }
                    Message::Pong(_) => last_pong = Instant::now(),
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Close(_) => anyhow::bail!("okx {label} closed"),
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

// ── Spot ──────────────────────────────────────────────────────────────

pub struct OkxTicker {
    pub inst_ids: Vec<String>,
}
impl OkxTicker {
    pub fn new(inst_ids: Vec<String>) -> Self { Self { inst_ids } }
}

#[async_trait]
impl ExchangeSource for OkxTicker {
    fn name(&self) -> &'static str { "okx" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_okx(self.name(), MarketKind::Spot, &self.inst_ids, ctx).await
    }
}
