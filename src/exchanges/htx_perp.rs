use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::htx::run_htx;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct HtxPerpBbo {
    pub symbols: Vec<String>,
}
impl HtxPerpBbo {
    pub fn new(symbols: Vec<String>) -> Self { Self { symbols } }
}

#[async_trait]
impl ExchangeSource for HtxPerpBbo {
    fn name(&self) -> &'static str { "htx" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_htx("wss://api.hbdm.com/linear-swap-ws", self.name(), MarketKind::Perp, &self.symbols, ctx).await
    }
}
