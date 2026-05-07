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
|---|---|---|---|
| Binance | Single WS, multi-symbol combined stream | Not implemented | Yes | `.../stream?streams=` |
| OKX | Single WS, multi-symbol subscribe | Single WS, multi-symbol subscribe | Yes | `tickers` + `-SWAP` mapping |
| Bybit | Single WS, multi-symbol subscribe | Single WS, multi-symbol subscribe | Yes | v5 `spot` / `linear` |
| Bitget | Single WS, multi-symbol subscribe | Single WS, multi-symbol subscribe | Yes | v2 public WS |
| KuCoin | Single WS, multi-topic subscribe | Not implemented | Yes | tokenized endpoint |
| Gate | Single WS, multi-symbol subscribe | Not implemented | Yes | v4 `spot.book_ticker` |
| Coinbase | Single WS, multi-product subscribe | Not implemented | Limited/varies by product line | advanced trade ticker |
| Kraken | Single WS, multi-symbol subscribe | Not implemented | Yes (derivatives endpoints differ) | ws v2 ticker |
| HTX | Single WS, multi-channel subscribe | Not implemented | Yes | gzip binary payload |
| Bitfinex | Single WS, multi-subscribe channels | Not implemented | Yes | `chanId -> symbol` map |

Perp adapters currently enabled in code: `okx_perp`, `bybit_perp`, `bitget_perp`.

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
