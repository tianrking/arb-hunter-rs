use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::bybit::run_bybit;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct BybitPerpTicker {
    pub symbols: Vec<String>,
}
impl BybitPerpTicker {
    pub fn new(symbols: Vec<String>) -> Self { Self { symbols } }
}

#[async_trait]
impl ExchangeSource for BybitPerpTicker {
    fn name(&self) -> &'static str { "bybit" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_bybit("wss://stream.bybit.com/v5/public/linear", self.name(), MarketKind::Perp, &self.symbols, ctx).await
    }
}
