use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::okx::run_okx;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct OkxPerpTicker {
    pub inst_ids: Vec<String>,
}
impl OkxPerpTicker {
    pub fn new(inst_ids: Vec<String>) -> Self { Self { inst_ids } }
}

#[async_trait]
impl ExchangeSource for OkxPerpTicker {
    fn name(&self) -> &'static str { "okx" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_okx(self.name(), MarketKind::Perp, &self.inst_ids, ctx).await
    }
}
