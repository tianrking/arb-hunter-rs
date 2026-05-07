use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

pub struct OkxTicker {
    pub inst_ids: Vec<String>,
}
impl OkxTicker {
    pub fn new(inst_ids: Vec<String>) -> Self { Self { inst_ids } }
}

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
}

#[async_trait]
impl ExchangeSource for OkxTicker {
    fn name(&self) -> &'static str { "okx" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.inst_ids.is_empty() {
            anyhow::bail!("okx spot symbols empty");
        }

        let (ws, _) = connect_async("wss://ws.okx.com:8443/ws/v5/public").await?;
        let (mut sink, mut stream) = ws.split();

        let args = self
            .inst_ids
            .iter()
            .map(|id| SubArg {
                channel: "tickers",
                inst_id: id.as_str(),
            })
            .collect::<Vec<_>>();
        let sub = serde_json::to_string(&SubReq {
            op: "subscribe",
            args,
        })?;
        sink.send(Message::Text(sub.into())).await?;

        let mut ping_tick = interval(Duration::from_secs(20));
        let mut last_pong = Instant::now();
        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_pong.elapsed() > Duration::from_secs(60) { anyhow::bail!("okx pong timeout"); }
                    sink.send(Message::Text("ping".into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("okx stream ended")??;
                    match msg {
                        Message::Text(t) => {
                            if t == "pong" { last_pong = Instant::now(); continue; }
                            if let Ok(parsed) = serde_json::from_str::<Msg<'_>>(&t)
                                && let Some(first) = parsed.data.first()
                                && let Some(arg) = parsed.arg {
                                emit_tick(&ctx, self.name(), MarketKind::Spot, arg.inst_id, first.bid, first.ask).await?;
                            }
                        }
                        Message::Pong(_) => last_pong = Instant::now(),
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Close(_) => anyhow::bail!("okx closed"),
                        Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
            }
        }
    }
}
