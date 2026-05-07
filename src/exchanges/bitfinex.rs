use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

pub struct BitfinexTicker {
    pub symbols: Vec<String>,
}
impl BitfinexTicker {
    pub fn new(symbols: Vec<String>) -> Self { Self { symbols } }
}

#[async_trait]
impl ExchangeSource for BitfinexTicker {
    fn name(&self) -> &'static str { "bitfinex" }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("bitfinex symbols empty");
        }

        let (ws, _) = connect_async("wss://api-pub.bitfinex.com/ws/2").await?;
        let (mut sink, mut stream) = ws.split();

        for sym in &self.symbols {
            sink.send(Message::Text(json!({"event":"subscribe","channel":"ticker","symbol":sym}).to_string().into())).await?;
        }

        let mut chan_map: HashMap<i64, String> = HashMap::new();
        let mut ping_tick = interval(Duration::from_secs(20));
        let mut last_seen = Instant::now();
        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_seen.elapsed() > Duration::from_secs(90) { anyhow::bail!("bitfinex heartbeat timeout"); }
                    sink.send(Message::Text(json!({"event":"ping","cid": now_ms()}).to_string().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("bitfinex stream ended")??;
                    match msg {
                        Message::Text(t) => {
                            last_seen = Instant::now();
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                                if v.is_object() {
                                    let chan_id = v.get("chanId").and_then(|x| x.as_i64());
                                    let event = v.get("event").and_then(|x| x.as_str());
                                    let sym = v.get("symbol").and_then(|x| x.as_str());
                                    if event == Some("subscribed")
                                        && let (Some(cid), Some(sym)) = (chan_id, sym) {
                                        chan_map.insert(cid, sym.to_string());
                                    }
                                    continue;
                                }
                                if let Some(arr) = v.as_array()
                                    && arr.len() >= 2
                                    && let Some(chan_id) = arr[0].as_i64()
                                    && let Some(data) = arr[1].as_array()
                                    && data.len() >= 4
                                {
                                    let bid = data[0].as_f64().map(|x| x.to_string());
                                    let ask = data[2].as_f64().map(|x| x.to_string());
                                    if let (Some(bid), Some(ask), Some(sym)) = (bid.as_deref(), ask.as_deref(), chan_map.get(&chan_id)) {
                                        emit_tick(&ctx, self.name(), MarketKind::Spot, sym, bid, ask).await?;
                                    }
                                }
                            }
                        }
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Pong(_) => last_seen = Instant::now(),
                        Message::Binary(_) | Message::Frame(_) => {}
                        Message::Close(_) => anyhow::bail!("bitfinex closed"),
                    }
                }
            }
        }
    }
}
