# Data Sources

This document describes the data sources used by the reconciliation engine as it is currently implemented. It distinguishes externally supplied market data from synthetic, static, and derived inputs so that reconciliation results are interpreted correctly.

## Source summary

| Source | Instruments and data | Access | Implementation |
| --- | --- | --- | --- |
| Yahoo Finance, through `yfinance` | Equity OHLCV bars, equity option chains, continuous futures bars, FX spot bars, and Treasury-yield index proxies | Public Yahoo Finance data accessed through the third-party `yfinance` client | [`yahoo_equities.py`](reconciliation_engine/ingestion/yahoo_equities.py), [`yahoo_options.py`](reconciliation_engine/ingestion/yahoo_options.py), [`futures.py`](reconciliation_engine/ingestion/futures.py), [`swaps.py`](reconciliation_engine/ingestion/swaps.py), [`forwards.py`](reconciliation_engine/ingestion/forwards.py) |
| Alpaca Market Data API | Historical US equity bars and US equity option-chain snapshots | Authenticated API; coverage depends on the account's market-data entitlement | [`alpaca_equities.py`](reconciliation_engine/ingestion/alpaca_equities.py), [`alpaca_options.py`](reconciliation_engine/ingestion/alpaca_options.py) |

## Yahoo Finance via `yfinance`

The project uses [`yfinance`](https://ranaroussi.github.io/yfinance/) to access market data published by Yahoo Finance. The relevant client operations are [`Ticker.history`](https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.history.html), [`Ticker.option_chain`](https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.option_chain.html), and [`yfinance.download`](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html).

The following data is currently requested:

- **Equities:** daily open, high, low, close, adjusted close, and volume data for the configured reconciliation symbols.
- **Equity options:** call and put chains by expiration, including strike, contract symbol, last price, bid, ask, volume, and open interest.
- **Continuous futures:** Yahoo symbols `ES=F`, `NQ=F`, `CL=F`, `GC=F`, and `ZN=F` by default.
- **FX spot:** `EURUSD=X`, `GBPUSD=X`, and `USDJPY=X` for forward valuation inputs.
- **Rate proxies:** `^IRX` and `^TNX`. The engine uses these Yahoo yield-index series as simplified rate proxies; they are not direct SOFR observations, swap curves, or executable rates.

`yfinance` is an independent, unofficial client and is not affiliated with or endorsed by Yahoo. Its documentation states that the downloaded data is intended for research and educational use and directs users to Yahoo's terms for usage rights. Production deployments should validate licensing, redistribution rights, availability, and rate limits before relying on this source.

## Alpaca Market Data API

The project accesses Alpaca through the official [`alpaca-py`](https://alpaca.markets/sdks/python/) SDK. Requests require `ALPACA_API_KEY` and `ALPACA_SECRET_KEY`.

### Equity bars

Historical equity bars are requested with `StockHistoricalDataClient` and `StockBarsRequest`, explicitly using Alpaca's **IEX** feed. Alpaca documents IEX as the feed available without a market-data subscription; it represents a single exchange rather than the consolidated US market. See Alpaca's [historical stock data documentation](https://docs.alpaca.markets/us/v1.1/docs/historical-stock-data-1) and [`StockBarsRequest` reference](https://alpaca.markets/sdks/python/api_reference/data/stock/requests.html).

### Option chains

Option-chain snapshots are requested with `OptionHistoricalDataClient` and `OptionChainRequest`. The response can include the latest trade, latest quote, daily bar, and open interest for each contract. Alpaca's [option-chain documentation](https://docs.alpaca.markets/us/v1.1/reference/optionchain) identifies two feeds:

- **Indicative:** available with basic access; trades are delayed and quotes are derived.
- **OPRA:** official consolidated options data, subject to the appropriate subscription and entitlement.

The code does not force a feed for option-chain requests, so the effective source is determined by the Alpaca account's entitlement and API behavior. Consumers should record the active entitlement when evaluating cross-provider discrepancies.

Alpaca's broader coverage and subscription model is documented in [About Market Data API](https://docs.alpaca.markets/us/docs/about-market-data-api).

## Synthetic, static, and derived inputs

The following inputs are part of the engine but are **not external market-data sources**:

| Input | How it is produced | Limitation |
| --- | --- | --- |
| Swap blotter | Hard-coded synthetic trades in `SWAP_BLOTTER` | Test/demo positions, not records from a trading or accounting system |
| FX-forward blotter | Hard-coded synthetic trades in `FORWARD_BLOTTER` | Test/demo positions, not externally sourced trades |
| Foreign interest rates | Static constants for EUR, GBP, and JPY in `FOREIGN_RATES` | Do not represent a live curve or a dated fixing |
| Calculated FX forwards | Derived from Yahoo FX spot, a Yahoo USD-rate proxy, static foreign rates, and covered-interest-parity logic | Model output, not an executable or vendor-supplied forward quote |
| Real-time prices | Deterministic simulated ticks based on `SIMULATED_PRICES` | No external streaming market-data connection is currently active |
| Reconciliation metrics | Calculated from normalized source records | Differences, percentages, latency, and anomaly flags are derived results rather than source data |

## How the sources are compared

| Reconciliation area | Inputs being compared or evaluated |
| --- | --- |
| Equities | Yahoo Finance bars against Alpaca IEX bars for the configured symbols |
| Equity options | Yahoo Finance option chains against Alpaca option-chain snapshots |
| Futures | Yahoo Finance continuous-futures bars; no second external futures provider is currently configured |
| Swaps | Synthetic swap positions valued using simplified Yahoo yield-index proxies |
| FX forwards | Synthetic forward positions compared with model-derived values based on Yahoo spot/rate inputs and static foreign rates |
| Real-time monitoring | Simulated ticks only; this is not currently a vendor-to-vendor live reconciliation |

## Operational considerations

- Source timestamps, exchange calendars, corporate-action adjustments, quote conditions, and symbol conventions can differ between providers.
- IEX equity data should not be assumed to match consolidated SIP data, and indicative options data should not be assumed to match OPRA.
- Yahoo rate tickers are valuation proxies, not authoritative benchmark fixings or complete curves.
- API credentials must remain in environment variables or an untracked environment file; never commit them to the repository.
- External APIs can change schemas, entitlements, limits, or availability. Provider documentation and account terms should be reviewed before production use.

