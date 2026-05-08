use anyhow::Result;
use tracing::warn;

use crate::source::SourceContext;
use crate::types::{DataEvent, MarketKind, MarketTick, now_ms};

pub async fn emit_tick(
    ctx: &SourceContext,
    exchange: &'static str,
    market: MarketKind,
    symbol: &str,
    bid: &str,
    ask: &str,
) -> Result<()> {
    emit_tick_ext(ctx, exchange, market, symbol, bid, ask, None, None, None).await
}

pub async fn emit_tick_ext(
    ctx: &SourceContext,
    exchange: &'static str,
    market: MarketKind,
    symbol: &str,
    bid: &str,
    ask: &str,
    mark: Option<&str>,
    funding_rate: Option<&str>,
    source_ts_ms: Option<u64>,
) -> Result<()> {
    let parsed_bid = bid.parse::<f64>();
    let parsed_ask = ask.parse::<f64>();

    if let (Ok(bid), Ok(ask)) = (parsed_bid, parsed_ask) {
        let mark = mark.and_then(|x| x.parse::<f64>().ok());
        let funding_rate = funding_rate.and_then(|x| x.parse::<f64>().ok());
        let tick = MarketTick {
            exchange,
            market,
            symbol: symbol.to_string().into_boxed_str(),
            bid,
            ask,
            mark,
            funding_rate,
            ts_ms: source_ts_ms.unwrap_or_else(now_ms),
        };
        ctx.emit(DataEvent::Tick(tick)).await?;
    } else {
        warn!(exchange, symbol, bid_raw=%bid, ask_raw=%ask, "invalid tick parse");
    }
    Ok(())
}

pub async fn emit_tick_f64(
    ctx: &SourceContext,
    exchange: &'static str,
    market: MarketKind,
    symbol: &str,
    bid: f64,
    ask: f64,
    mark: Option<f64>,
    funding_rate: Option<f64>,
    source_ts_ms: Option<u64>,
) -> Result<()> {
    if bid > 0.0 && ask > 0.0 && bid.is_finite() && ask.is_finite() {
        let tick = MarketTick {
            exchange,
            market,
            symbol: symbol.to_string().into_boxed_str(),
            bid,
            ask,
            mark,
            funding_rate,
            ts_ms: source_ts_ms.unwrap_or_else(now_ms),
        };
        ctx.emit(DataEvent::Tick(tick)).await?;
    }
    Ok(())
}
