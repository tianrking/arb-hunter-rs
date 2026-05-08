use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::binance::run_binance;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct BinancePerpBookTicker {
    symbols: Vec<String>,
}

impl BinancePerpBookTicker {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[async_trait]
impl ExchangeSource for BinancePerpBookTicker {
    fn name(&self) -> &'static str { "binance" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_binance("wss://fstream.binance.com/stream?", self.name(), MarketKind::Perp, &self.symbols, ctx).await
    }
}
