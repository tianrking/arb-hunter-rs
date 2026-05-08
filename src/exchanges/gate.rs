use async_trait::async_trait;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::{emit_tick, emit_tick_ext};
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

// ── Shared types ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct GateMsg {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    result: Option<GateResult>,
}

#[derive(Deserialize)]
struct GateResult {
    #[serde(default)]
    s: Option<String>,
    #[serde(default)]
    b: Option<String>,
    #[serde(default)]
    a: Option<String>,
    #[serde(default)]
    contract: Option<String>,
    #[serde(default)]
    highest_bid: Option<String>,
    #[serde(default)]
    lowest_ask: Option<String>,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    time_ms: Option<u64>,
}

// ── Shared run loop ───────────────────────────────────────────────────

pub async fn run_gate(
    url: &str,
    channel: &str,
    ping_channel: &str,
    exchange: &'static str,
    market: MarketKind,
    symbols: &[String],
    ctx: SourceContext,
) -> Result<()> {
    let label = if market == MarketKind::Spot { "spot" } else { "perp" };
    if symbols.is_empty() {
        anyhow::bail!("gate {label} symbols empty");
    }

    let (ws, _) = connect_async(url).await?;
    let (mut sink, mut stream) = ws.split();
    sink.send(Message::Text(
        json!({"time":now_ms()/1000,"channel":channel,"event":"subscribe","payload":symbols})
            .to_string().into(),
    )).await?;

    let mut ping_tick = interval(Duration::from_secs(20));
    let mut last_seen = Instant::now();

    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                if last_seen.elapsed() > Duration::from_secs(90) {
                    anyhow::bail!("gate {label} heartbeat timeout");
                }
                sink.send(Message::Text(json!({"time":now_ms()/1000,"channel":ping_channel}).to_string().into())).await?;
                ctx.emit(DataEvent::Heartbeat { exchange, ts_ms: now_ms() }).await?;
            }
            msg = stream.next() => {
                let msg = msg.context(format!("gate {label} stream ended"))??;
                match msg {
                    Message::Text(t) => {
                        last_seen = Instant::now();
                        if let Ok(v) = serde_json::from_str::<GateMsg>(&t)
                            && v.channel.as_deref() == Some(channel)
                            && let Some(r) = v.result
                        {
                            match market {
                                MarketKind::Spot => {
                                    if let (Some(symbol), Some(bid), Some(ask)) =
                                        (r.s.as_deref(), r.b.as_deref(), r.a.as_deref())
                                    {
                                        emit_tick(&ctx, exchange, market, symbol, bid, ask).await?;
                                    }
                                }
                                MarketKind::Perp => {
                                    if let (Some(symbol), Some(bid), Some(ask)) =
                                        (r.contract.as_deref(), r.highest_bid.as_deref(), r.lowest_ask.as_deref())
                                    {
                                        emit_tick_ext(
                                            &ctx, exchange, market, symbol, bid, ask,
                                            r.mark_price.as_deref(), r.funding_rate.as_deref(), r.time_ms,
                                        ).await?;
                                    }
                                }
                            }
                        }
                    }
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Pong(_) => last_seen = Instant::now(),
                    Message::Binary(_) | Message::Frame(_) => {}
                    Message::Close(_) => anyhow::bail!("gate {label} closed"),
                }
            }
        }
    }
}

// ── Spot ──────────────────────────────────────────────────────────────

pub struct GateSpotBookTicker {
    pub symbols: Vec<String>,
}
impl GateSpotBookTicker {
    pub fn new(symbols: Vec<String>) -> Self { Self { symbols } }
}

#[async_trait]
impl ExchangeSource for GateSpotBookTicker {
    fn name(&self) -> &'static str { "gate" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_gate(
            "wss://api.gateio.ws/ws/v4/",
            "spot.book_ticker", "spot.ping",
            self.name(), MarketKind::Spot, &self.symbols, ctx,
        ).await
    }
}
