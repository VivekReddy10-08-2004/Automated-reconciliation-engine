# Data Sources

This document describes the data sources used by the reconciliation engine as it is currently implemented. It distinguishes externally supplied market data from synthetic, static, and derived inputs so that reconciliation results are interpreted correctly.

Operational scope: equities use Yahoo, Alpaca, and Massive. The supported
persisted options command uses Yahoo and Alpaca and is offline-verified only.
Massive options and Yahoo/Massive futures are experimental library adapters,
outside persisted workflows. See [Feature Support](docs/FEATURE_SUPPORT.md).

## Source summary

| Source | Instruments and data | Access | Implementation |
| --- | --- | --- | --- |
| Yahoo Finance, through `yfinance` | Equity OHLCV bars, equity option chains, and continuous futures bars | Public Yahoo Finance data accessed through the third-party `yfinance` client | [`yahoo_equities.py`](reconciliation_engine/ingestion/yahoo_equities.py), [`yahoo_options.py`](reconciliation_engine/ingestion/yahoo_options.py), [`futures.py`](reconciliation_engine/ingestion/futures.py) |
| Alpaca Market Data API | Historical US equity bars and US equity option-chain snapshots | Authenticated API; coverage depends on the account's market-data entitlement | [`alpaca_equities.py`](reconciliation_engine/ingestion/alpaca_equities.py), [`alpaca_options.py`](reconciliation_engine/ingestion/alpaca_options.py) |
| Massive REST API | US equity daily/grouped OHLCV, US option contract reference and aggregate bars, and US futures contract reference and aggregate bars | Authenticated API; the configured free plans are limited to 5 calls/minute and plan-specific history/recency | [`massive.py`](reconciliation_engine/ingestion/massive.py), [`massive_equities.py`](reconciliation_engine/ingestion/massive_equities.py), [`massive_options.py`](reconciliation_engine/ingestion/massive_options.py), [`massive_futures.py`](reconciliation_engine/ingestion/massive_futures.py) |

## Yahoo Finance via `yfinance`

The project uses [`yfinance`](https://ranaroussi.github.io/yfinance/) to access market data published by Yahoo Finance. The relevant client operations are [`Ticker.history`](https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.history.html), [`Ticker.option_chain`](https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.option_chain.html), and [`yfinance.download`](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html).

The following data is currently requested:

- **Equities:** daily unadjusted open, high, low, close, and volume data for the configured reconciliation symbols. Automatic Yahoo adjustment is disabled so its price basis is closer to Alpaca's raw IEX bars.
- **Equity options:** call and put chains by expiration, including strike, contract symbol, last price, bid, ask, volume, and open interest.
- **Continuous futures:** Yahoo symbols `ES=F`, `NQ=F`, `CL=F`, `GC=F`, and `ZN=F` by default.

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

## Massive REST API

The project uses Massive through a small local REST client rather than a vendor
SDK. `MASSIVE_API_KEY` is sent in an authorization header and never placed in a
logged URL. One process-wide limiter is shared by equities, options, and
futures; it defaults to `MASSIVE_CALLS_PER_MINUTE=5`. Cursor pagination,
bounded retries, and `Retry-After` responses are handled centrally.

- **Equities:** per-symbol unadjusted daily aggregates are available for direct
  reconciliation. The grouped daily endpoint can acquire the entire US equity
  market for one date in one request and is the preferred broad-coverage path.
- **Options (experimental):** standalone functions normalize contract reference
  data and daily aggregate close/volume. The current adapter leaves bid, ask,
  and open interest null and does not fetch IV or Greeks. These are adapter
  limitations, not a claim about every Massive product or subscription.
  Massive is not connected to the persisted Yahoo/Alpaca options command.
- **Futures (experimental):** expiring contract reference and aggregate bars are normalized.
  The adapter preserves `session_end_date`, overnight `window_start`, and
  settlement price. It does not treat Yahoo continuous symbols such as `GC=F`
  as identical to an expiring Massive contract such as `GCZ6`.

See Massive's [stock API overview](https://massive.com/docs/rest/stocks/overview),
[options API overview](https://massive.com/docs/rest/options/overview), and
[futures API overview](https://massive.com/docs/rest/futures/overview). Plan
activation and market-data licensing remain account responsibilities.

## Synthetic, static, and derived inputs

The following inputs are part of the engine but are **not external market-data sources**:

| Input | How it is produced | Limitation |
| --- | --- | --- |
| Real-time prices | Deterministic simulated ticks based on `SIMULATED_PRICES` | No external streaming market-data connection is currently active |
| Reconciliation metrics | Calculated from normalized source records | Differences, percentages, latency, and anomaly flags are derived results rather than source data |

## How the sources are compared

| Reconciliation area | Inputs being compared or evaluated |
| --- | --- |
| Equities | Persisted pairwise control across Yahoo Finance, Alpaca IEX, and Massive. |
| Equity options | Supported persisted Yahoo/Alpaca control, offline verified. Massive ingestion and three-provider helpers are experimental library capabilities; live snapshot/bar comparability is unverified. |
| Futures | Experimental Yahoo/Massive library comparison with an explicit contract mapping. No persisted futures workflow or live verification is included. |
| Real-time monitoring | Simulated ticks only; this is not currently a vendor-to-vendor live reconciliation |

Contract identity alone does not make options observations comparable: a daily
aggregate close and a chain's latest trade or quote need matching observation
times and price definitions. Futures additionally require dated contract-roll
mapping and matching exchange sessions. The futures helper compares closes;
Massive settlement is retained separately as reference. Establish these
comparison rules and live evidence before extending derivative ingestion or
promoting the adapters into supported persisted workflows.

## Operational considerations

- Source timestamps, exchange calendars, corporate-action adjustments, quote conditions, and symbol conventions can differ between providers.
- Provider inputs are normalized to uppercase symbols and UTC-derived market dates before comparison. This reduces key mismatches but cannot make Yahoo and IEX coverage identical.
- IEX equity data should not be assumed to match consolidated SIP data, and indicative options data should not be assumed to match OPRA.
- API credentials must remain in environment variables or an untracked environment file; never commit them to the repository.
- External APIs can change schemas, entitlements, limits, or availability. Provider documentation and account terms should be reviewed before production use.
- The Massive free-plan request budget is a shared scarce resource in this application. Broad equity EOD data is fetched with the grouped endpoint; option and futures contract requests must be prioritized and checkpointed rather than fanned out without bounds.

