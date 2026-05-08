use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::bitfinex::run_bitfinex;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct BitfinexPerpTicker {
    pub symbols: Vec<String>,
}
impl BitfinexPerpTicker {
    pub fn new(symbols: Vec<String>) -> Self { Self { symbols } }
}

#[async_trait]
impl ExchangeSource for BitfinexPerpTicker {
    fn name(&self) -> &'static str { "bitfinex" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_bitfinex(self.name(), MarketKind::Perp, &self.symbols, ctx).await
    }
}
