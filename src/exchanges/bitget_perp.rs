use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::bitget::run_bitget;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct BitgetPerpTicker {
    pub symbols: Vec<String>,
}
impl BitgetPerpTicker {
    pub fn new(symbols: Vec<String>) -> Self { Self { symbols } }
}

#[async_trait]
impl ExchangeSource for BitgetPerpTicker {
    fn name(&self) -> &'static str { "bitget" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_bitget("USDT-FUTURES", self.name(), MarketKind::Perp, &self.symbols, ctx).await
    }
}
