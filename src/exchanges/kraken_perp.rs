use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::kraken::run_kraken;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct KrakenPerpTicker {
    pub symbols: Vec<String>,
}
impl KrakenPerpTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[async_trait]
impl ExchangeSource for KrakenPerpTicker {
    fn name(&self) -> &'static str {
        "kraken"
    }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_kraken(
            "wss://ws.kraken.com/v2",
            self.name(),
            MarketKind::Perp,
            &self.symbols,
            ctx,
        )
        .await
    }
}
