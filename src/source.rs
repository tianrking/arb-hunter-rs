use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::types::{BackpressureMode, DataEvent};

#[derive(Clone)]
pub struct SourceContext {
    pub tx: mpsc::Sender<DataEvent>,
    pub backpressure: BackpressureMode,
}

impl SourceContext {
    pub async fn emit(&self, ev: DataEvent) -> Result<()> {
        match self.backpressure {
            BackpressureMode::Block => {
                self.tx.send(ev).await?;
            }
            BackpressureMode::DropNewest => {
                let _ = self.tx.try_send(ev);
            }
        }
        Ok(())
    }
}

#[async_trait]
pub trait ExchangeSource: Send + Sync {
    fn name(&self) -> &'static str;
    async fn run(&self, ctx: SourceContext) -> Result<()>;
}
