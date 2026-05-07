use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use tokio::time::interval;
use tracing::warn;

use crate::event_bus::{EventBus, NormalizedTick};

#[derive(Clone)]
pub struct ApiState {
    pub bus: EventBus,
}

pub fn build_router(state: ApiState) -> Router {
    Router::new()
        .route("/ws/ticks", get(ws_ticks))
        .route("/", get(|| async { Json(serde_json::json!({"service":"arb-hunter-rs"})) }))
        .with_state(Arc::new(state))
}

#[derive(Debug, Deserialize, Default)]
pub struct TickFilterQuery {
    pub symbols: Option<String>,
    pub exchanges: Option<String>,
    pub market: Option<String>,
}

async fn ws_ticks(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
    Query(q): Query<TickFilterQuery>,
) -> impl IntoResponse {
    { let bus = state.bus.clone(); ws.on_upgrade(move |socket| ws_loop(socket, bus, q)) }
}

async fn ws_loop(mut socket: WebSocket, bus: EventBus, q: TickFilterQuery) {
    let mut rx = bus.subscribe();
    let filter = TickFilter::from_query(q);
    let mut hb = interval(Duration::from_secs(15));

    loop {
        tokio::select! {
            _ = hb.tick() => {
                if socket.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
            }
            msg = rx.recv() => {
                match msg {
                    Ok(tick) => {
                        if !filter.matches(&tick) {
                            continue;
                        }
                        match serde_json::to_string(&tick) {
                            Ok(line) => {
                                if socket.send(Message::Text(line)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!(error=%e, "ws serialize failed");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "ws consumer lagged behind event bus");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }
}

#[derive(Default)]
struct TickFilter {
    symbols: Option<HashSet<String>>,
    exchanges: Option<HashSet<String>>,
    market: Option<String>,
}

impl TickFilter {
    fn from_query(q: TickFilterQuery) -> Self {
        Self {
            symbols: q.symbols.map(parse_csv_set_upper),
            exchanges: q.exchanges.map(parse_csv_set_lower),
            market: q.market.map(|x| x.trim().to_ascii_lowercase()),
        }
    }

    fn matches(&self, t: &NormalizedTick) -> bool {
        if let Some(symbols) = &self.symbols
            && !symbols.contains(&t.symbol.to_ascii_uppercase()) {
            return false;
        }
        if let Some(exchanges) = &self.exchanges
            && !exchanges.contains(&t.exchange.to_ascii_lowercase()) {
            return false;
        }
        if let Some(market) = &self.market
            && t.market != market {
            return false;
        }
        true
    }
}

fn parse_csv_set_upper(s: String) -> HashSet<String> {
    s.split(',')
        .map(|x| x.trim().to_ascii_uppercase())
        .filter(|x| !x.is_empty())
        .collect()
}

fn parse_csv_set_lower(s: String) -> HashSet<String> {
    s.split(',')
        .map(|x| x.trim().to_ascii_lowercase())
        .filter(|x| !x.is_empty())
        .collect()
}
