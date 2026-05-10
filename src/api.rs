use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tracing::warn;

use crate::event_bus::{EventBus, NormalizedTick};
use crate::metrics::AppMetrics;

#[derive(Clone)]
pub struct ApiState {
    pub bus: EventBus,
    pub metrics: Arc<AppMetrics>,
}

pub fn build_router(state: ApiState) -> Router {
    Router::new()
        .route("/ws/ticks", get(ws_ticks))
        .route("/health", get(health))
        .route("/snapshot", get(snapshot))
        .route("/funding", get(funding))
        .route("/coverage", get(coverage))
        .route("/metrics", get(metrics))
        .route(
            "/",
            get(|| async { Json(serde_json::json!({"service":"arb-hunter-rs"})) }),
        )
        .with_state(Arc::new(state))
}

#[derive(Debug, Deserialize, Default)]
pub struct TickFilterQuery {
    pub symbols: Option<String>,
    pub exchanges: Option<String>,
    pub market: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct SnapshotQuery {
    symbol: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct FundingQuery {
    symbols: Option<String>,
    exchanges: Option<String>,
    only_with_funding: Option<bool>,
    include_stale: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct CoverageQuery {
    symbols: Option<String>,
    exchanges: Option<String>,
    market: Option<String>,
    include_stale: Option<bool>,
    only_with_funding: Option<bool>,
}

#[derive(Debug, Serialize)]
struct FundingPoint {
    exchange: String,
    raw_symbol: String,
    funding: Option<f64>,
    mark: Option<f64>,
    ts: u64,
    source_latency_ms: u64,
    stale: bool,
}

#[derive(Debug, Serialize)]
struct FundingView {
    symbol: String,
    exchanges_total: usize,
    exchanges_with_funding: usize,
    min_funding: Option<f64>,
    max_funding: Option<f64>,
    funding_spread: Option<f64>,
    updated_at: u64,
    points: Vec<FundingPoint>,
}

#[derive(Debug, Serialize)]
struct CoveragePoint {
    exchange: String,
    raw_symbol: String,
    funding: Option<f64>,
    funding_available: bool,
    mark: Option<f64>,
    source_latency_ms: u64,
    stale: bool,
    ts: u64,
}

#[derive(Debug, Serialize)]
struct CoverageView {
    symbol: String,
    market: String,
    health_status: String,
    alerts: Vec<String>,
    exchanges_total: usize,
    exchanges_with_funding: usize,
    funding_coverage_ratio: f64,
    exchanges_stale: usize,
    stale_ratio: f64,
    latency_ms_min: u64,
    latency_ms_p50: u64,
    latency_ms_avg: f64,
    latency_ms_p95: u64,
    latency_ms_max: u64,
    updated_at: u64,
    points: Vec<CoveragePoint>,
}

#[derive(Debug, Serialize)]
struct CoverageResponse {
    generated_at: u64,
    query: CoverageQueryEcho,
    summary: CoverageSummary,
    exchange_summaries: Vec<CoverageExchangeSummary>,
    alerts: Vec<CoverageAlert>,
    symbols: Vec<CoverageView>,
}

#[derive(Debug, Serialize)]
struct CoverageQueryEcho {
    symbols: Vec<String>,
    exchanges: Vec<String>,
    market: Option<String>,
    include_stale: bool,
    only_with_funding: bool,
}

#[derive(Debug, Serialize)]
struct CoverageSummary {
    total_symbols: usize,
    total_points: usize,
    healthy_symbols: usize,
    warning_symbols: usize,
    critical_symbols: usize,
    stale_points: usize,
    stale_ratio: f64,
    perp_points: usize,
    funding_points: usize,
    funding_coverage_ratio: f64,
    exchange_count_seen: usize,
    expected_exchange_count: usize,
    online_exchange_count: usize,
    exchange_online_ratio: f64,
    latency_ms_p50: u64,
    latency_ms_p95: u64,
    markets: Vec<CoverageMarketSummary>,
}

#[derive(Debug, Serialize)]
struct CoverageMarketSummary {
    market: String,
    symbols: usize,
    points: usize,
    healthy_symbols: usize,
    warning_symbols: usize,
    critical_symbols: usize,
    stale_ratio: f64,
    funding_coverage_ratio: f64,
    avg_exchanges_per_symbol: f64,
}

#[derive(Debug, Serialize)]
struct CoverageExchangeSummary {
    exchange: String,
    online: bool,
    health_status: String,
    symbols_total: usize,
    spot_symbols: usize,
    perp_symbols: usize,
    points_total: usize,
    stale_points: usize,
    stale_ratio: f64,
    perp_points: usize,
    funding_points: usize,
    funding_coverage_ratio: f64,
    latency_ms_p50: u64,
    latency_ms_avg: f64,
    latency_ms_p95: u64,
    latency_ms_max: u64,
    updated_at: u64,
}

#[derive(Debug, Serialize)]
struct CoverageAlert {
    level: String,
    scope: String,
    target: String,
    message: String,
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"ok": true}))
}

async fn snapshot(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<SnapshotQuery>,
) -> impl IntoResponse {
    let data = if let Some(sym) = q.symbol {
        state.bus.snapshot_by_symbol(&sym).await
    } else {
        state.bus.snapshot_all().await
    };
    Json(data)
}

async fn metrics(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    state.metrics.render()
}

async fn funding(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<FundingQuery>,
) -> impl IntoResponse {
    let symbols = q.symbols.map(parse_csv_set_upper);
    let exchanges = q.exchanges.map(parse_csv_set_lower);
    let only_with_funding = q.only_with_funding.unwrap_or(true);
    let include_stale = q.include_stale.unwrap_or(false);

    let ticks = state.bus.snapshot_all().await;
    let mut groups: HashMap<String, Vec<FundingPoint>> = HashMap::new();

    for t in ticks {
        if t.market != "perp" {
            continue;
        }
        if !include_stale && t.stale {
            continue;
        }
        if let Some(ex_set) = &exchanges
            && !ex_set.contains(&t.exchange.to_ascii_lowercase())
        {
            continue;
        }
        if only_with_funding && t.funding.is_none() {
            continue;
        }

        let unified = canonical_perp_symbol(&t.symbol);
        if let Some(sym_set) = &symbols {
            let raw = t.symbol.to_ascii_uppercase();
            if !sym_set.contains(&unified) && !sym_set.contains(&raw) {
                continue;
            }
        }

        groups.entry(unified).or_default().push(FundingPoint {
            exchange: t.exchange.to_string(),
            raw_symbol: t.symbol,
            funding: t.funding,
            mark: t.mark,
            ts: t.ts,
            source_latency_ms: t.source_latency_ms,
            stale: t.stale,
        });
    }

    let mut out = groups
        .into_iter()
        .map(|(symbol, mut points)| {
            points.sort_by(|a, b| a.exchange.cmp(&b.exchange));
            let funding_values = points.iter().filter_map(|p| p.funding).collect::<Vec<_>>();
            let min_funding = funding_values.iter().copied().reduce(f64::min);
            let max_funding = funding_values.iter().copied().reduce(f64::max);
            let updated_at = points.iter().map(|p| p.ts).max().unwrap_or(0);
            FundingView {
                symbol,
                exchanges_total: points.len(),
                exchanges_with_funding: funding_values.len(),
                min_funding,
                max_funding,
                funding_spread: min_funding.zip(max_funding).map(|(min, max)| max - min),
                updated_at,
                points,
            }
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    Json(out)
}

async fn coverage(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<CoverageQuery>,
) -> impl IntoResponse {
    let symbols = q.symbols.map(parse_csv_set_upper);
    let exchanges = q.exchanges.map(parse_csv_set_lower);
    let market_filter = q.market.map(|x| x.trim().to_ascii_lowercase());
    let include_stale = q.include_stale.unwrap_or(true);
    let only_with_funding = q.only_with_funding.unwrap_or(false);
    let query_echo = CoverageQueryEcho {
        symbols: set_to_sorted_vec_upper(symbols.as_ref()),
        exchanges: set_to_sorted_vec_lower(exchanges.as_ref()),
        market: market_filter.clone(),
        include_stale,
        only_with_funding,
    };

    let ticks = state.bus.snapshot_all().await;
    let mut groups: HashMap<(String, String), Vec<CoveragePoint>> = HashMap::new();

    for t in ticks {
        let market = t.market.to_ascii_lowercase();
        if let Some(m) = &market_filter
            && &market != m
        {
            continue;
        }
        if !include_stale && t.stale {
            continue;
        }
        if only_with_funding && t.funding.is_none() {
            continue;
        }
        if let Some(ex_set) = &exchanges
            && !ex_set.contains(&t.exchange.to_ascii_lowercase())
        {
            continue;
        }

        let unified_symbol = if market == "perp" {
            canonical_perp_symbol(&t.symbol)
        } else {
            canonical_spot_symbol(&t.symbol)
        };
        if let Some(sym_set) = &symbols {
            let raw = t.symbol.to_ascii_uppercase();
            if !sym_set.contains(&unified_symbol) && !sym_set.contains(&raw) {
                continue;
            }
        }

        groups
            .entry((market, unified_symbol))
            .or_default()
            .push(CoveragePoint {
                exchange: t.exchange.to_string(),
                raw_symbol: t.symbol,
                funding: t.funding,
                funding_available: t.funding.is_some(),
                mark: t.mark,
                source_latency_ms: t.source_latency_ms,
                stale: t.stale,
                ts: t.ts,
            });
    }

    let mut symbols_out = groups
        .into_iter()
        .map(|((market, symbol), mut points)| {
            points.sort_by(|a, b| a.exchange.cmp(&b.exchange));
            let exchanges_total = points.len();
            let exchanges_with_funding = points.iter().filter(|p| p.funding_available).count();
            let funding_coverage_ratio = ratio(exchanges_with_funding, exchanges_total);
            let exchanges_stale = points.iter().filter(|p| p.stale).count();
            let stale_ratio = ratio(exchanges_stale, exchanges_total);
            let latencies = points
                .iter()
                .map(|p| p.source_latency_ms)
                .collect::<Vec<_>>();
            let latency_ms_min = latencies.iter().copied().min().unwrap_or(0);
            let latency_ms_max = latencies.iter().copied().max().unwrap_or(0);
            let latency_ms_p50 = percentile_u64(&latencies, 0.50);
            let latency_ms_p95 = percentile_u64(&latencies, 0.95);
            let latency_ms_avg = avg_u64(&latencies);
            let updated_at = points.iter().map(|p| p.ts).max().unwrap_or(0);
            let (health_status, alerts) = assess_symbol_health(
                &market,
                exchanges_total,
                exchanges_stale,
                exchanges_with_funding,
                latency_ms_p95,
            );

            CoverageView {
                symbol,
                market,
                health_status,
                alerts,
                exchanges_total,
                exchanges_with_funding,
                funding_coverage_ratio,
                exchanges_stale,
                stale_ratio,
                latency_ms_min,
                latency_ms_p50,
                latency_ms_avg,
                latency_ms_p95,
                latency_ms_max,
                updated_at,
                points,
            }
        })
        .collect::<Vec<_>>();

    symbols_out.sort_by(|a, b| {
        a.market
            .cmp(&b.market)
            .then_with(|| a.symbol.cmp(&b.symbol))
    });

    let exchange_summaries = build_exchange_summaries(&symbols_out, exchanges.as_ref());
    let summary = build_coverage_summary(&symbols_out, &exchange_summaries, exchanges.as_ref());
    let alerts = build_coverage_alerts(&symbols_out, &exchange_summaries, &summary);

    Json(CoverageResponse {
        generated_at: crate::types::now_ms(),
        query: query_echo,
        summary,
        exchange_summaries,
        alerts,
        symbols: symbols_out,
    })
}

async fn ws_ticks(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
    Query(q): Query<TickFilterQuery>,
) -> impl IntoResponse {
    let bus = state.bus.clone();
    let metrics = state.metrics.clone();
    ws.on_upgrade(move |socket| ws_loop(socket, bus, q, metrics))
}

async fn ws_loop(
    mut socket: WebSocket,
    bus: EventBus,
    q: TickFilterQuery,
    metrics: Arc<AppMetrics>,
) {
    metrics.ws_subscribers.inc();

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

    metrics.ws_subscribers.dec();
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
            && !symbols.contains(&t.symbol.to_ascii_uppercase())
        {
            return false;
        }
        if let Some(exchanges) = &self.exchanges
            && !exchanges.contains(&t.exchange.to_ascii_lowercase())
        {
            return false;
        }
        if let Some(market) = &self.market
            && t.market != market
        {
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

fn canonical_perp_symbol(raw: &str) -> String {
    let mut s = raw.to_ascii_uppercase();
    s.retain(|c| c.is_ascii_alphanumeric());

    if s.starts_with('T') && s.contains("F0") {
        s = s.trim_start_matches('T').replace("F0", "");
    }
    for suffix in ["SWAP", "PERP"] {
        if let Some(trimmed) = s.strip_suffix(suffix) {
            s = trimmed.to_string();
        }
    }
    if let Some(trimmed) = s.strip_suffix('M') {
        s = trimmed.to_string();
    }
    s
}

fn canonical_spot_symbol(raw: &str) -> String {
    let mut s = raw.to_ascii_uppercase();
    s.retain(|c| c.is_ascii_alphanumeric());
    s
}

const WARN_STALE_RATIO: f64 = 0.25;
const CRIT_STALE_RATIO: f64 = 0.60;
const WARN_LATENCY_MS: u64 = 1500;
const CRIT_LATENCY_MS: u64 = 5000;
const WARN_PERP_FUNDING_EXCHANGES: usize = 2;

fn assess_symbol_health(
    market: &str,
    exchanges_total: usize,
    exchanges_stale: usize,
    exchanges_with_funding: usize,
    latency_ms_p95: u64,
) -> (String, Vec<String>) {
    let stale_ratio = ratio(exchanges_stale, exchanges_total);
    let mut alerts = Vec::new();

    if exchanges_total == 0 {
        alerts.push("no exchange points available".to_string());
        return ("critical".to_string(), alerts);
    }

    if market == "perp" && exchanges_with_funding == 0 {
        alerts.push("no funding available across exchanges".to_string());
        return ("critical".to_string(), alerts);
    }
    if stale_ratio >= CRIT_STALE_RATIO {
        alerts.push(format!("critical stale ratio {:.1}%", stale_ratio * 100.0));
        return ("critical".to_string(), alerts);
    }
    if latency_ms_p95 >= CRIT_LATENCY_MS {
        alerts.push(format!("critical p95 latency {}ms", latency_ms_p95));
        return ("critical".to_string(), alerts);
    }

    let mut warning = false;
    if exchanges_total < 2 {
        warning = true;
        alerts.push("single-exchange coverage".to_string());
    }
    if stale_ratio >= WARN_STALE_RATIO {
        warning = true;
        alerts.push(format!("elevated stale ratio {:.1}%", stale_ratio * 100.0));
    }
    if latency_ms_p95 >= WARN_LATENCY_MS {
        warning = true;
        alerts.push(format!("elevated p95 latency {}ms", latency_ms_p95));
    }
    if market == "perp" && exchanges_with_funding < WARN_PERP_FUNDING_EXCHANGES {
        warning = true;
        alerts.push(format!(
            "low funding coverage ({}/{})",
            exchanges_with_funding, exchanges_total
        ));
    }

    if warning {
        ("warning".to_string(), alerts)
    } else {
        ("healthy".to_string(), alerts)
    }
}

#[derive(Default)]
struct ExchangeAccumulator {
    online: bool,
    symbol_keys: HashSet<String>,
    spot_symbols: HashSet<String>,
    perp_symbols: HashSet<String>,
    points_total: usize,
    stale_points: usize,
    perp_points: usize,
    funding_points: usize,
    latencies: Vec<u64>,
    updated_at: u64,
}

fn build_exchange_summaries(
    symbols: &[CoverageView],
    requested_exchanges: Option<&HashSet<String>>,
) -> Vec<CoverageExchangeSummary> {
    let mut acc: HashMap<String, ExchangeAccumulator> = HashMap::new();

    for s in symbols {
        for p in &s.points {
            let e = acc.entry(p.exchange.clone()).or_default();
            e.symbol_keys.insert(format!("{}:{}", s.market, s.symbol));
            if s.market == "spot" {
                e.spot_symbols.insert(s.symbol.clone());
            } else if s.market == "perp" {
                e.perp_symbols.insert(s.symbol.clone());
                e.perp_points += 1;
            }
            e.points_total += 1;
            if p.stale {
                e.stale_points += 1;
            } else {
                e.online = true;
            }
            if p.funding_available {
                e.funding_points += 1;
            }
            e.latencies.push(p.source_latency_ms);
            e.updated_at = e.updated_at.max(p.ts);
        }
    }

    if let Some(req) = requested_exchanges {
        for ex in req {
            acc.entry(ex.clone()).or_default();
        }
    }

    let mut out = acc
        .into_iter()
        .map(|(exchange, e)| {
            let stale_ratio = ratio(e.stale_points, e.points_total);
            let funding_coverage_ratio = if e.perp_points > 0 {
                ratio(e.funding_points, e.perp_points)
            } else {
                1.0
            };
            let latency_ms_p50 = percentile_u64(&e.latencies, 0.50);
            let latency_ms_p95 = percentile_u64(&e.latencies, 0.95);
            let latency_ms_max = e.latencies.iter().copied().max().unwrap_or(0);
            let latency_ms_avg = avg_u64(&e.latencies);
            let health_status = if !e.online {
                "critical".to_string()
            } else if stale_ratio >= CRIT_STALE_RATIO || latency_ms_p95 >= CRIT_LATENCY_MS {
                "critical".to_string()
            } else if stale_ratio >= WARN_STALE_RATIO
                || latency_ms_p95 >= WARN_LATENCY_MS
                || (e.perp_points > 0 && funding_coverage_ratio < 0.5)
            {
                "warning".to_string()
            } else {
                "healthy".to_string()
            };

            CoverageExchangeSummary {
                exchange,
                online: e.online,
                health_status,
                symbols_total: e.symbol_keys.len(),
                spot_symbols: e.spot_symbols.len(),
                perp_symbols: e.perp_symbols.len(),
                points_total: e.points_total,
                stale_points: e.stale_points,
                stale_ratio,
                perp_points: e.perp_points,
                funding_points: e.funding_points,
                funding_coverage_ratio,
                latency_ms_p50,
                latency_ms_avg,
                latency_ms_p95,
                latency_ms_max,
                updated_at: e.updated_at,
            }
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| a.exchange.cmp(&b.exchange));
    out
}

#[derive(Default)]
struct MarketAccumulator {
    symbols: usize,
    points: usize,
    healthy_symbols: usize,
    warning_symbols: usize,
    critical_symbols: usize,
    stale_points: usize,
    perp_points: usize,
    funding_points: usize,
    exchanges_total: usize,
}

fn build_coverage_summary(
    symbols: &[CoverageView],
    exchange_summaries: &[CoverageExchangeSummary],
    requested_exchanges: Option<&HashSet<String>>,
) -> CoverageSummary {
    let total_symbols = symbols.len();
    let total_points = symbols.iter().map(|s| s.exchanges_total).sum::<usize>();
    let stale_points = symbols.iter().map(|s| s.exchanges_stale).sum::<usize>();
    let stale_ratio = ratio(stale_points, total_points);
    let perp_points = symbols
        .iter()
        .filter(|s| s.market == "perp")
        .map(|s| s.exchanges_total)
        .sum::<usize>();
    let funding_points = symbols
        .iter()
        .filter(|s| s.market == "perp")
        .map(|s| s.exchanges_with_funding)
        .sum::<usize>();
    let funding_coverage_ratio = if perp_points > 0 {
        ratio(funding_points, perp_points)
    } else {
        1.0
    };

    let healthy_symbols = symbols
        .iter()
        .filter(|s| s.health_status == "healthy")
        .count();
    let warning_symbols = symbols
        .iter()
        .filter(|s| s.health_status == "warning")
        .count();
    let critical_symbols = symbols
        .iter()
        .filter(|s| s.health_status == "critical")
        .count();

    let all_latencies = symbols
        .iter()
        .flat_map(|s| s.points.iter().map(|p| p.source_latency_ms))
        .collect::<Vec<_>>();
    let latency_ms_p50 = percentile_u64(&all_latencies, 0.50);
    let latency_ms_p95 = percentile_u64(&all_latencies, 0.95);

    let mut market_map: HashMap<String, MarketAccumulator> = HashMap::new();
    for s in symbols {
        let m = market_map.entry(s.market.clone()).or_default();
        m.symbols += 1;
        m.points += s.exchanges_total;
        m.stale_points += s.exchanges_stale;
        m.exchanges_total += s.exchanges_total;
        if s.market == "perp" {
            m.perp_points += s.exchanges_total;
            m.funding_points += s.exchanges_with_funding;
        }
        match s.health_status.as_str() {
            "healthy" => m.healthy_symbols += 1,
            "warning" => m.warning_symbols += 1,
            "critical" => m.critical_symbols += 1,
            _ => {}
        }
    }
    let mut markets = market_map
        .into_iter()
        .map(|(market, m)| CoverageMarketSummary {
            market,
            symbols: m.symbols,
            points: m.points,
            healthy_symbols: m.healthy_symbols,
            warning_symbols: m.warning_symbols,
            critical_symbols: m.critical_symbols,
            stale_ratio: ratio(m.stale_points, m.points),
            funding_coverage_ratio: if m.perp_points > 0 {
                ratio(m.funding_points, m.perp_points)
            } else {
                1.0
            },
            avg_exchanges_per_symbol: if m.symbols > 0 {
                m.exchanges_total as f64 / m.symbols as f64
            } else {
                0.0
            },
        })
        .collect::<Vec<_>>();
    markets.sort_by(|a, b| a.market.cmp(&b.market));

    let exchange_count_seen = exchange_summaries
        .iter()
        .filter(|e| e.points_total > 0)
        .count();
    let expected_exchange_count = requested_exchanges
        .map(|s| s.len())
        .unwrap_or(exchange_summaries.len());
    let online_exchange_count = exchange_summaries.iter().filter(|e| e.online).count();
    let exchange_online_ratio = ratio(online_exchange_count, expected_exchange_count);

    CoverageSummary {
        total_symbols,
        total_points,
        healthy_symbols,
        warning_symbols,
        critical_symbols,
        stale_points,
        stale_ratio,
        perp_points,
        funding_points,
        funding_coverage_ratio,
        exchange_count_seen,
        expected_exchange_count,
        online_exchange_count,
        exchange_online_ratio,
        latency_ms_p50,
        latency_ms_p95,
        markets,
    }
}

fn build_coverage_alerts(
    symbols: &[CoverageView],
    exchange_summaries: &[CoverageExchangeSummary],
    summary: &CoverageSummary,
) -> Vec<CoverageAlert> {
    let mut out = Vec::new();

    for s in symbols {
        if s.health_status == "healthy" {
            continue;
        }
        out.push(CoverageAlert {
            level: s.health_status.clone(),
            scope: "symbol".to_string(),
            target: format!("{}:{}", s.market, s.symbol),
            message: if s.alerts.is_empty() {
                "symbol health degraded".to_string()
            } else {
                s.alerts.join("; ")
            },
        });
    }

    for e in exchange_summaries {
        if e.health_status == "healthy" {
            continue;
        }
        out.push(CoverageAlert {
            level: e.health_status.clone(),
            scope: "exchange".to_string(),
            target: e.exchange.clone(),
            message: if !e.online {
                "exchange currently offline (no fresh points)".to_string()
            } else if e.stale_ratio >= CRIT_STALE_RATIO {
                format!("high stale ratio {:.1}%", e.stale_ratio * 100.0)
            } else if e.latency_ms_p95 >= WARN_LATENCY_MS {
                format!("elevated p95 latency {}ms", e.latency_ms_p95)
            } else {
                "exchange quality degraded".to_string()
            },
        });
    }

    if summary.exchange_online_ratio < 1.0 {
        out.push(CoverageAlert {
            level: "warning".to_string(),
            scope: "global".to_string(),
            target: "coverage".to_string(),
            message: format!(
                "online exchange ratio {:.1}% ({}/{})",
                summary.exchange_online_ratio * 100.0,
                summary.online_exchange_count,
                summary.expected_exchange_count
            ),
        });
    }
    if summary.stale_ratio >= WARN_STALE_RATIO {
        out.push(CoverageAlert {
            level: if summary.stale_ratio >= CRIT_STALE_RATIO {
                "critical".to_string()
            } else {
                "warning".to_string()
            },
            scope: "global".to_string(),
            target: "coverage".to_string(),
            message: format!("global stale ratio {:.1}%", summary.stale_ratio * 100.0),
        });
    }

    out.sort_by(|a, b| {
        alert_level_rank(&a.level)
            .cmp(&alert_level_rank(&b.level))
            .then_with(|| a.scope.cmp(&b.scope))
            .then_with(|| a.target.cmp(&b.target))
    });
    out
}

fn alert_level_rank(level: &str) -> u8 {
    match level {
        "critical" => 0,
        "warning" => 1,
        _ => 2,
    }
}

fn ratio(num: usize, den: usize) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

fn avg_u64(values: &[u64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().map(|&v| v as f64).sum::<f64>() / values.len() as f64
    }
}

fn percentile_u64(values: &[u64], p: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let p = p.clamp(0.0, 1.0);
    let rank = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn set_to_sorted_vec_upper(set: Option<&HashSet<String>>) -> Vec<String> {
    let mut v = set
        .map(|s| s.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    v.sort();
    v
}

fn set_to_sorted_vec_lower(set: Option<&HashSet<String>>) -> Vec<String> {
    let mut v = set
        .map(|s| s.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tick() -> NormalizedTick {
        NormalizedTick {
            version: "v1",
            exchange: "okx",
            market: "spot",
            symbol: "BTCUSDT".to_string(),
            bid: 1.0,
            ask: 2.0,
            mark: None,
            funding: None,
            ts: 1,
            source_latency_ms: 0,
            stale: false,
        }
    }

    #[test]
    fn filter_matches_symbol_exchange_market() {
        let q = TickFilterQuery {
            symbols: Some("BTCUSDT".to_string()),
            exchanges: Some("okx".to_string()),
            market: Some("spot".to_string()),
        };
        let f = TickFilter::from_query(q);
        assert!(f.matches(&sample_tick()));
    }

    #[test]
    fn filter_rejects_other_symbol() {
        let q = TickFilterQuery {
            symbols: Some("ETHUSDT".to_string()),
            exchanges: None,
            market: None,
        };
        let f = TickFilter::from_query(q);
        assert!(!f.matches(&sample_tick()));
    }

    #[test]
    fn canonical_perp_symbol_normalizes_common_formats() {
        assert_eq!(canonical_perp_symbol("BTC-USDT-SWAP"), "BTCUSDT");
        assert_eq!(canonical_perp_symbol("BTCUSDTM"), "BTCUSDT");
        assert_eq!(canonical_perp_symbol("tBTCF0:USDTF0"), "BTCUSDT");
        assert_eq!(canonical_perp_symbol("BTC_USDT"), "BTCUSDT");
    }

    #[test]
    fn canonical_spot_symbol_normalizes_delimiters() {
        assert_eq!(canonical_spot_symbol("BTC-USDT"), "BTCUSDT");
        assert_eq!(canonical_spot_symbol("BTC/USDT"), "BTCUSDT");
        assert_eq!(canonical_spot_symbol("btc_usdt"), "BTCUSDT");
    }

    #[test]
    fn percentile_u64_basic() {
        let v = vec![10, 20, 30, 40, 50];
        assert_eq!(percentile_u64(&v, 0.50), 30);
        assert_eq!(percentile_u64(&v, 0.95), 50);
    }

    #[test]
    fn assess_symbol_health_perp_no_funding_is_critical() {
        let (status, alerts) = assess_symbol_health("perp", 3, 0, 0, 10);
        assert_eq!(status, "critical");
        assert!(alerts.iter().any(|x| x.contains("no funding")));
    }

    #[test]
    fn assess_symbol_health_warning_for_single_exchange() {
        let (status, alerts) = assess_symbol_health("spot", 1, 0, 0, 10);
        assert_eq!(status, "warning");
        assert!(alerts.iter().any(|x| x.contains("single-exchange")));
    }
}
