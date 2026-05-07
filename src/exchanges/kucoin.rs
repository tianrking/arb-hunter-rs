use async_trait::async_trait;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::exchanges::common::emit_tick;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::{DataEvent, MarketKind, now_ms};

pub struct KucoinTicker {
    pub symbols: Vec<String>,
}
impl KucoinTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[derive(Deserialize)]
struct BulletResp {
    data: BulletData,
}
#[derive(Deserialize)]
struct BulletData {
    token: String,
    #[serde(rename = "instanceServers")]
    instance_servers: Vec<ServerInfo>,
}
#[derive(Deserialize)]
struct ServerInfo {
    endpoint: String,
}

#[derive(Deserialize)]
struct KuMsg {
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    data: Option<KuTickData>,
}

#[derive(Deserialize)]
struct KuTickData {
    #[serde(rename = "bestBid")]
    bid: Option<String>,
    #[serde(rename = "bestAsk")]
    ask: Option<String>,
}

#[async_trait]
impl ExchangeSource for KucoinTicker {
    fn name(&self) -> &'static str {
        "kucoin"
    }

    async fn run(&self, ctx: SourceContext) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("kucoin symbols empty");
        }

        let client = reqwest::Client::new();
        let bullet = client
            .post("https://api.kucoin.com/api/v1/bullet-public")
            .send()
            .await?
            .error_for_status()?
            .json::<BulletResp>()
            .await?;
        let endpoint = bullet
            .data
            .instance_servers
            .first()
            .context("kucoin no instance server")?
            .endpoint
            .clone();
        let ws_url = format!("{}?token={}", endpoint, bullet.data.token);

        let (ws, _) = connect_async(ws_url).await?;
        let (mut sink, mut stream) = ws.split();

        for (i, s) in self.symbols.iter().enumerate() {
            let topic = format!("/market/ticker:{}", s);
            sink.send(Message::Text(
                json!({
                    "id": format!("sub-{}", i),
                    "type":"subscribe",
                    "topic": topic,
                    "privateChannel": false,
                    "response": true
                })
                .to_string()
                .into(),
            ))
            .await?;
        }

        let mut ping_tick = interval(Duration::from_secs(20));
        let mut last_seen = Instant::now();
        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if last_seen.elapsed() > Duration::from_secs(90) { anyhow::bail!("kucoin heartbeat timeout"); }
                    sink.send(Message::Text(json!({"id":"ping","type":"ping"}).to_string().into())).await?;
                    ctx.emit(DataEvent::Heartbeat { exchange: self.name(), ts_ms: now_ms() }).await?;
                }
                msg = stream.next() => {
                    let msg = msg.context("kucoin stream ended")??;
                    match msg {
                        Message::Text(t) => {
                            last_seen = Instant::now();
                            if let Ok(v) = serde_json::from_str::<KuMsg>(&t) {
                                if v.r#type.as_deref() == Some("pong") {
                                    continue;
                                }
                                if let (Some(topic), Some(data)) = (v.topic.as_deref(), v.data)
                                    && let (Some(bid), Some(ask)) = (data.bid.as_deref(), data.ask.as_deref()) {
                                    let symbol = topic.split(':').nth(1).unwrap_or("UNKNOWN");
                                    emit_tick(&ctx, self.name(), MarketKind::Spot, symbol, bid, ask).await?;
                                }
                            }
                        }
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Pong(_) => last_seen = Instant::now(),
                        Message::Binary(_) | Message::Frame(_) => {}
                        Message::Close(_) => anyhow::bail!("kucoin closed"),
                    }
                }
            }
        }
    }
}
