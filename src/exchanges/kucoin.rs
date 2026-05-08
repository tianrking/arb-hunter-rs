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

// ── Shared types ──────────────────────────────────────────────────────

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
    #[serde(rename = "bestBidPrice")]
    bid_price: Option<String>,
    #[serde(rename = "bestAskPrice")]
    ask_price: Option<String>,
    #[serde(rename = "markPrice")]
    mark: Option<String>,
    #[serde(rename = "fundingRate")]
    funding: Option<String>,
    #[serde(rename = "time")]
    ts_time: Option<u64>,
    #[serde(rename = "ts")]
    ts: Option<u64>,
}

// ── Shared run loop ───────────────────────────────────────────────────

pub struct KucoinConf {
    pub bullet_url: &'static str,
    pub topic_prefix: &'static str,
    pub sub_id_prefix: &'static str,
}

pub async fn run_kucoin(
    conf: &KucoinConf,
    exchange: &'static str,
    market: MarketKind,
    symbols: &[String],
    ctx: SourceContext,
    client: &reqwest::Client,
) -> Result<()> {
    let label = if market == MarketKind::Spot { "spot" } else { "perp" };
    if symbols.is_empty() {
        anyhow::bail!("kucoin {label} symbols empty");
    }

    let bullet = client
        .post(conf.bullet_url)
        .send().await?
        .error_for_status()?
        .json::<BulletResp>().await?;
    let endpoint = bullet.data.instance_servers.first()
        .context(format!("kucoin {label} no instance server"))?
        .endpoint.clone();
    let ws_url = format!("{}?token={}", endpoint, bullet.data.token);

    let (ws, _) = connect_async(ws_url).await?;
    let (mut sink, mut stream) = ws.split();

    for (i, s) in symbols.iter().enumerate() {
        let topic = format!("{}{}", conf.topic_prefix, s);
        sink.send(Message::Text(
            json!({"id":format!("{}-{}",conf.sub_id_prefix, i),"type":"subscribe","topic":topic,"privateChannel":false,"response":true})
                .to_string().into(),
        )).await?;
    }

    let mut ping_tick = interval(Duration::from_secs(20));
    let mut last_seen = Instant::now();

    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                if last_seen.elapsed() > Duration::from_secs(90) {
                    anyhow::bail!("kucoin {label} heartbeat timeout");
                }
                sink.send(Message::Text(json!({"id":"ping","type":"ping"}).to_string().into())).await?;
                ctx.emit(DataEvent::Heartbeat { exchange, ts_ms: now_ms() }).await?;
            }
            msg = stream.next() => {
                let msg = msg.context(format!("kucoin {label} stream ended"))??;
                match msg {
                    Message::Text(t) => {
                        last_seen = Instant::now();
                        if let Ok(v) = serde_json::from_str::<KuMsg>(&t) {
                            if v.r#type.as_deref() == Some("pong") { continue; }
                            if let (Some(topic), Some(data)) = (v.topic.as_deref(), v.data) {
                                let bid = data.bid.as_deref().or(data.bid_price.as_deref());
                                let ask = data.ask.as_deref().or(data.ask_price.as_deref());
                                if let (Some(bid), Some(ask)) = (bid, ask) {
                                    let symbol = topic.split(':').nth(1).unwrap_or("UNKNOWN");
                                    let ts = data.ts_time
                                        .map(|x| if x > 10_000_000_000_000 { x / 1_000_000 } else { x })
                                        .or(data.ts);
                                    let (mark, funding) = if market == MarketKind::Perp {
                                        (data.mark.as_deref(), data.funding.as_deref())
                                    } else {
                                        (None, None)
                                    };
                                    emit_tick_ext(&ctx, exchange, market, symbol, bid, ask, mark, funding, ts).await?;
                                }
                            }
                        }
                    }
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Pong(_) => last_seen = Instant::now(),
                    Message::Binary(_) | Message::Frame(_) => {}
                    Message::Close(_) => anyhow::bail!("kucoin {label} closed"),
                }
            }
        }
    }
}

// ── Spot ──────────────────────────────────────────────────────────────

pub struct KucoinTicker {
    pub symbols: Vec<String>,
    client: reqwest::Client,
}
impl KucoinTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols, client: reqwest::Client::new() }
    }
}

#[async_trait]
impl ExchangeSource for KucoinTicker {
    fn name(&self) -> &'static str { "kucoin" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        let conf = KucoinConf {
            bullet_url: "https://api.kucoin.com/api/v1/bullet-public",
            topic_prefix: "/market/ticker:",
            sub_id_prefix: "sub",
        };
        run_kucoin(&conf, self.name(), MarketKind::Spot, &self.symbols, ctx, &self.client).await
    }
}
