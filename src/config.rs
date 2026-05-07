use std::collections::HashMap;
use std::fs;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::types::BackpressureMode;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub strategy: StrategyConfig,
    pub symbols: Vec<String>,
    pub perp_symbols: Option<Vec<String>>,
    pub exchanges: HashMap<String, ExchangeConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    pub queue_capacity: usize,
    pub backpressure: BackpressureConfig,
    pub report_interval_ms: u64,
    pub stale_ttl_ms: u64,
    #[serde(default = "default_api_addr")]
    pub api_addr: String,
    #[serde(default)]
    pub redis_url: Option<String>,
    #[serde(default = "default_redis_stream_prefix")]
    pub redis_stream_prefix: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureConfig {
    Block,
    DropNewest,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    pub min_profit_usdt: f64,
    pub min_profit_bps: f64,
    pub min_signal_hold_ms: u64,
    pub slippage_bps: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConfig {
    pub enabled: bool,
    pub symbols: Option<Vec<String>>,      // spot symbols override
    pub perp_symbols: Option<Vec<String>>, // perp symbols override
    pub fee: FeeModel,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FeeModel {
    Fixed {
        #[allow(dead_code)]
        maker_bps: f64,
        taker_bps: f64,
    },
    Tiered {
        volume_30d_usdt: f64,
        tiers: Vec<FeeTier>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeeTier {
    pub min_volume_usdt: f64,
    #[allow(dead_code)]
    pub maker_bps: f64,
    pub taker_bps: f64,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let path = std::env::var("ARB_CONFIG").unwrap_or_else(|_| "config.yaml".to_string());
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file: {path}"))?;
        let mut cfg: AppConfig =
            serde_yaml::from_str(&content).with_context(|| format!("invalid yaml: {path}"))?;

        cfg.symbols = normalize_symbols(&cfg.symbols);
        cfg.perp_symbols = cfg.perp_symbols.take().map(|v| normalize_symbols(&v));

        for ex in cfg.exchanges.values_mut() {
            if let Some(symbols) = &mut ex.symbols {
                *symbols = normalize_symbols(symbols);
            }
            if let Some(perp) = &mut ex.perp_symbols {
                *perp = normalize_symbols(perp);
            }
        }

        Ok(cfg)
    }

    pub fn backpressure_mode(&self) -> BackpressureMode {
        match self.runtime.backpressure {
            BackpressureConfig::Block => BackpressureMode::Block,
            BackpressureConfig::DropNewest => BackpressureMode::DropNewest,
        }
    }

    pub fn symbols_for_exchange(&self, ex: &str) -> Vec<String> {
        let Some(cfg) = self.exchanges.get(ex) else {
            return Vec::new();
        };
        cfg.symbols.clone().unwrap_or_else(|| self.symbols.clone())
    }

    pub fn perp_symbols_for_exchange(&self, ex: &str) -> Vec<String> {
        let Some(cfg) = self.exchanges.get(ex) else {
            return Vec::new();
        };
        if let Some(v) = &cfg.perp_symbols {
            return v.clone();
        }
        self.perp_symbols.clone().unwrap_or_default()
    }

    pub fn enabled_exchanges(&self) -> Vec<String> {
        self.exchanges
            .iter()
            .filter_map(|(k, v)| if v.enabled { Some(k.clone()) } else { None })
            .collect()
    }

    pub fn taker_bps(&self, exchange: &str) -> Option<f64> {
        let ex = self.exchanges.get(exchange)?;
        Some(ex.fee.taker_bps())
    }
}

impl FeeModel {
    pub fn taker_bps(&self) -> f64 {
        match self {
            FeeModel::Fixed { taker_bps, .. } => *taker_bps,
            FeeModel::Tiered {
                volume_30d_usdt,
                tiers,
            } => {
                let mut best: Option<&FeeTier> = None;
                for t in tiers {
                    if *volume_30d_usdt >= t.min_volume_usdt {
                        if best.is_none_or(|x| t.min_volume_usdt > x.min_volume_usdt) {
                            best = Some(t);
                        }
                    }
                }
                best.map(|x| x.taker_bps)
                    .unwrap_or_else(|| tiers.first().map(|x| x.taker_bps).unwrap_or(0.0))
            }
        }
    }
}

fn normalize_symbols(input: &[String]) -> Vec<String> {
    input
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect()
}

fn default_api_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_redis_stream_prefix() -> String {
    "ticks".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiered_fee_selects_highest_matching_tier() {
        let f = FeeModel::Tiered {
            volume_30d_usdt: 5_500_000.0,
            tiers: vec![
                FeeTier {
                    min_volume_usdt: 0.0,
                    maker_bps: 10.0,
                    taker_bps: 12.0,
                },
                FeeTier {
                    min_volume_usdt: 1_000_000.0,
                    maker_bps: 8.0,
                    taker_bps: 9.0,
                },
                FeeTier {
                    min_volume_usdt: 5_000_000.0,
                    maker_bps: 6.0,
                    taker_bps: 7.0,
                },
            ],
        };
        assert!((f.taker_bps() - 7.0).abs() < 1e-9);
    }
}
