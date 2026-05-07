# arb-hunter-rs

Rust + Tokio multi-exchange WebSocket aggregator with spot/perp unified market model, fee-aware signal filtering, and production-style runtime controls.

![Rust](https://img.shields.io/badge/Rust-2024-000000?logo=rust)
![Tokio](https://img.shields.io/badge/Runtime-Tokio-333333?logo=rust)
![Architecture](https://img.shields.io/badge/Architecture-EventBus%20%2B%20Aggregator-1f6feb)
![Markets](https://img.shields.io/badge/Markets-Spot%20%2B%20Perp-0a7f3f)
![Interfaces](https://img.shields.io/badge/Output-WebSocket%20%7C%20HTTP%20%7C%20Redis-d97706)
![Status](https://img.shields.io/badge/Status-Production--style-blue)

## Architecture

```mermaid
flowchart LR
  A["Exchange WS Adapters"] --> B["SourceRuntime (supervisor)"]
  B --> C["mpsc queue"]
  C --> D["SpreadAggregator"]
  D --> E["Signal Output"]

  F["config.yaml"] --> B
  F --> D
```

## Runtime Pipeline

1. Exchange adapters ingest public WS market data.
2. `SourceRuntime` supervises each source with reconnect backoff.
3. Events are pushed into a bounded `tokio::sync::mpsc` queue.
4. `SpreadAggregator` computes best cross-exchange pairs per symbol.
5. Signals are filtered by fee/slippage/hold-time policy.

## Connection Model Matrix

| Exchange | Spot model | Perp model (this project) | Official perp support | Notes |
|---|---|---|---|---|
| Binance | Single WS, multi-symbol combined stream | Single WS, multi-symbol combined stream | Yes | Spot `stream.binance.com`, Perp `fstream.binance.com` |
| OKX | Single WS, multi-symbol subscribe | Single WS, multi-symbol subscribe | Yes | `tickers` + `-SWAP` mapping |
| Bybit | Single WS, multi-symbol subscribe | Single WS, multi-symbol subscribe | Yes | v5 `spot` / `linear` |
| Bitget | Single WS, multi-symbol subscribe | Single WS, multi-symbol subscribe | Yes | v2 public WS |
| KuCoin | Single WS, multi-topic subscribe | Single WS, multi-topic subscribe | Yes | tokenized endpoint; perp uses `api-futures` bullet token |
| Gate | Single WS, multi-symbol subscribe | Single WS, multi-symbol subscribe | Yes | spot/perp use different ws domains |
| Coinbase | Single WS, multi-product subscribe | Not implemented | Limited/varies by product line | advanced trade ticker |
| Kraken | Single WS, multi-symbol subscribe | Single WS, multi-symbol subscribe | Yes (derivatives endpoints differ) | ws v2 ticker; perp symbol naming must match venue |
| HTX | Single WS, multi-channel subscribe | Single WS, multi-channel subscribe | Yes | gzip binary payload |
| Bitfinex | Single WS, multi-subscribe channels | Single WS, multi-subscribe channels | Yes | `chanId -> symbol` map |

Perp adapters currently enabled in code: `okx_perp`, `bybit_perp`, `bitget_perp`, `binance_perp`, `kucoin_perp`, `gate_perp`, `kraken_perp`, `htx_perp`, `bitfinex_perp`.

Perp symbol conversion defaults in registry:

- Binance / Bybit / Bitget: `BTCUSDT`
- OKX / HTX: `BTC-USDT-SWAP` (OKX) / `BTC-USDT` (HTX)
- KuCoin Perp: `BTCUSDTM`
- Gate Perp: `BTC_USDT`
- Bitfinex Perp: `tBTCF0:USDTF0`
- Kraken Perp: pass-through (`perp_symbols` should be configured as exact venue symbols)

## Signal Fields

- `gross`: raw spread (`sell_bid - buy_ask`)
- `gross_bps`: gross spread in bps
- `buy_fee`, `sell_fee`, `slip`: explicit cost components
- `fee_bps_total`, `slippage_bps_total`: total modeled cost in bps
- `net`, `net_bps`: post-cost edge
- `state`: `FILTERED` / `HOLDING` / `TRIGGER`

## Configuration

Default config file is `config.yaml`.

```bash
ARB_CONFIG=./config.yaml cargo run
```

## Testing

Run all tests:

```bash
cargo test
```

Current test coverage focuses on:

- fee tier selection behavior
- symbol mapping helpers
- cross-exchange pair selection (same-exchange exclusion)
- profit computation direction and cost dominance

## Extend New Exchange

1. Add `src/exchanges/<name>.rs`
2. Implement `ExchangeSource`
3. Map payloads to `MarketTick` (`Spot` or `Perp`)
4. Register in `src/exchanges/registry.rs`
