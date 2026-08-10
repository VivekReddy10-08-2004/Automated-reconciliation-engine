# Automated Financial Data Ingestion & Reconciliation Engine

A Python MVP that fetches equity data from Yahoo Finance and Alpaca, normalizes both sources into one schema, and flags cross-source closing-price differences.

## Current MVP

- Normalized Yahoo Finance equity bars: `symbol, trade_date, open, high, low, close, volume`
- Normalized Yahoo Finance option chains for calls and puts
- Normalized Alpaca daily equity bars using the same equity schema
- Equity reconciliation report with Yahoo/Alpaca close and volume differences
- `MATCH` / `BREAK` status using a configurable close-price tolerance

## Run

1. Create `project.env` with `ALPACA_API_KEY` and `ALPACA_SECRET_KEY`.
2. Install dependencies with `pip install -r requirements.txt`.
3. Run the reconciliation demo from the project root:

```powershell
python -m Ingestion.equity_reconciliation
```

The script compares recent daily AAPL bars and prints the reconciliation result plus the number of price breaks.

## Project Structure

```text
Ingestion/Equities_yf.py            Yahoo equity ingestion and normalization
Ingestion/options_yf.py             Yahoo options ingestion and normalization
Ingestion/Equities.py               Alpaca equity ingestion and normalization
Ingestion/equity_reconciliation.py  Yahoo vs. Alpaca equity comparison
```

## Planned Next Steps

- Persist normalized data to DuckDB or Parquet
- Reconcile option chains
- Add scheduled runs and alerts
