use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::kucoin::{KucoinConf, run_kucoin};
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct KucoinPerpTicker {
    pub symbols: Vec<String>,
    client: reqwest::Client,
}
impl KucoinPerpTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            symbols,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl ExchangeSource for KucoinPerpTicker {
    fn name(&self) -> &'static str {
        "kucoin"
    }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        let conf = KucoinConf {
            bullet_url: "https://api-futures.kucoin.com/api/v1/bullet-public",
            topic_prefix: "/contractMarket/ticker:",
            sub_id_prefix: "sub-perp",
        };
        run_kucoin(
            &conf,
            self.name(),
            MarketKind::Perp,
            &self.symbols,
            ctx,
            &self.client,
        )
        .await
    }
}
