# Read-Only Export API

The FastAPI service exposes curated reconciliation evidence to dashboards and
other services without allowing them to modify DuckDB. Every data endpoint
requires a bearer token; `/health` is public so a local supervisor or load
balancer can probe the service.

## Start the service

Set a long random token in the owner-only `project.env` file:

```text
RECONCILIATION_EXPORT_API_KEY=<long-random-token>
```

Then start one API process:

```sh
python -m reconciliation_engine.api --db reconciliation.duckdb \
  --host 127.0.0.1 --port 8000 --max-concurrent-queries 4
```

The default loopback binding prevents remote access. Put TLS and a production
identity-aware proxy in front of the service before binding it to a shared
network. Do not pass the token on the command line or in a URL.

Call an authenticated endpoint:

```sh
curl -H "Authorization: Bearer $RECONCILIATION_EXPORT_API_KEY" \
  "http://127.0.0.1:8000/api/v1/reconciliations?symbol=AAPL&status=BREAK"
```

Interactive OpenAPI documentation is available at `/docs`. It can authorize
requests with the same bearer token.

## Endpoint contract

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/runs` | Paginated run history |
| `GET /api/v1/runs/{run_id}` | One run plus partition status metadata |
| `GET /api/v1/reconciliations` | Filter by symbol, date range, exact date, status, or run |
| `GET /api/v1/exceptions` | Filter discrepancies by symbol, severity, status, or run |
| `GET /api/v1/exports` | Download filtered CSV, JSON, or Parquet |
| `GET /health` | Public database availability and reader-capacity configuration |

List endpoints return:

```json
{
  "items": [],
  "page": {"limit": 50, "offset": 0, "total": 0, "next_offset": null}
}
```

Pagination is capped at 500 rows per request. Exports default to a 50,000-row
maximum and return `413` when filters are not selective enough. Tune this bound
with `--export-max-rows`; increasing it also increases memory and temporary-file
usage.

Example downloads:

```sh
curl -OJ -H "Authorization: Bearer $RECONCILIATION_EXPORT_API_KEY" \
  "http://127.0.0.1:8000/api/v1/exports?format=csv&run_id=<run-id>"

curl -OJ -H "Authorization: Bearer $RECONCILIATION_EXPORT_API_KEY" \
  "http://127.0.0.1:8000/api/v1/exports?format=parquet&symbol=AAPL"
```

## DuckDB concurrency model

The API deliberately uses:

- a bounded semaphore around database queries;
- short-lived connections opened with `read_only=True`;
- deterministic ordering and bounded page/export sizes; and
- one Uvicorn worker for the local DuckDB deployment.

DuckDB is not a shared multi-machine transactional service. The API holds no
idle database connection, allowing the batch writer to acquire the file between
requests. A read that overlaps a conflicting writer lock returns `503` rather
than retrying indefinitely. Schedule heavy exports after batch completion.

For continuous ingestion and high API concurrency, publish a completed database
snapshot or curated Parquet files for the API, or move workflow state to a
transactional database and analytics to a warehouse. Do not point multiple API
workers on different machines at the same writable DuckDB file.

The included snapshot publisher makes that recommendation executable. Run it
only after a completed batch, then point the API at the published file:

```sh
python -m reconciliation_engine.storage.snapshot \
  reconciliation.duckdb snapshots/reconciliation-read.duckdb
python -m reconciliation_engine.api \
  --db snapshots/reconciliation-read.duckdb --host 127.0.0.1 --port 8000
```

Publication builds a complete temporary DuckDB copy and atomically replaces the
destination. Existing API readers keep their current file handle while new
processes receive the new consistent snapshot.

## Security and operational behavior

- Tokens are compared with a constant-time comparison and never logged.
- SQL values use parameters; filter and format choices are allow-listed.
- Exports contain curated reconciliation fields, not credentials or raw vendor
  payloads.
- Export events log format, row count, and whether filters were used, but not
  token values or exported records.
- Rotate `RECONCILIATION_EXPORT_API_KEY` if it may have been exposed.
- A production deployment should replace the shared token with service
  identities, scoped authorization, TLS, rate limiting, and an external audit
  sink.
