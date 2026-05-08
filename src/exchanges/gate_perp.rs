use async_trait::async_trait;

use anyhow::Result;

use crate::exchanges::gate::run_gate;
use crate::source::{ExchangeSource, SourceContext};
use crate::types::MarketKind;

pub struct GatePerpBookTicker {
    pub symbols: Vec<String>,
}
impl GatePerpBookTicker {
    pub fn new(symbols: Vec<String>) -> Self { Self { symbols } }
}

#[async_trait]
impl ExchangeSource for GatePerpBookTicker {
    fn name(&self) -> &'static str { "gate" }
    async fn run(&self, ctx: SourceContext) -> Result<()> {
        run_gate(
            "wss://fx-ws.gateio.ws/v4/ws/usdt",
            "futures.book_ticker", "futures.ping",
            self.name(), MarketKind::Perp, &self.symbols, ctx,
        ).await
    }
}
