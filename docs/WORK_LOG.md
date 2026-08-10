# Work Log

A running record of changes made to the project, newest first.

---

## 2026-06-19 — Broaden "derivatives" to all four types + add ingestion + dashboard spec

### Context / Decisions
- **Definition locked in:** in this project, **"derivatives" = options, futures, swaps, and forwards** (not just options chains).
- **Direction:** *keep and expand* derivatives as a core feature, rather than removing or softening them. The previously equities-focused docs were extended to cover all instrument types.
- **Data sourcing constraint:** 100% free. Alpaca (equities/options), yfinance (futures + reference rates). Swaps/forwards are OTC with **no free market feed**, so they follow the trade-ops pattern of *internal blotter vs. computed market mark*.

### New ingestion code (`Ingestion/`)
All three are **function-first** (not import-time execution) per the guidance in `PYTHON_KNOWLEDGE_FOR_PROJECT.md`, and all were run successfully against live data.

| File | Source | Output schema (key cols) |
| --- | --- | --- |
| `futures.py` | **yfinance** continuous contracts (`ES=F`, `NQ=F`, `CL=F`, `GC=F`, `ZN=F`) | `symbol, trade_date, open, high, low, close, volume` |
| `swaps.py` | Synthetic IRS blotter + **yfinance** yield proxies (`^IRX`, `^TNX`) | `trade_id, …, fixed_rate_pct, floating_index, reference_rate_pct, fixed_minus_float_pct, trade_date` |
| `forwards.py` | Synthetic FX-forward blotter + **yfinance** FX spot, marked via covered interest parity | `trade_id, pair, booked_forward_rate, spot_rate, market_forward_rate, diff_pips, …` |

- **Why Alpaca isn't used for these:** Alpaca has no free feed for futures/swaps/forwards.
- **Blotters are synthetic placeholders** with static booked rates — swap into a real booking export by passing your own list to `ingest_swaps()` / `ingest_forwards()` or editing `SWAP_BLOTTER` / `FORWARD_BLOTTER`. (Static forward rates currently show large `diff_pips` vs. live spot — this is expected and demonstrates a "break".)

### Documentation updated
- **`README.md`** — project scope broadened to "Options, Futures, Swaps & Forwards".
- **`system_design.md`** —
  - Section 2: per-instrument free-data-source table + OTC keying rules.
  - Section 3 (Phase 3/4): split the reconciliation into equity vs. derivative auditor models; added a derivatives alert example.
  - **Section 5 (new): Live CLI Dashboard (Module G)** — full spec (see below).
  - Section 1: added Module G to the microservices list.
- **`WORKFLOW_DESIGN.md`** — added `options/futures/swaps/forwards_data_staging` schemas; made `recon_results` / `exceptions` instrument-type aware; broadened the universe, ingestion, standardization, and reconciliation phases; updated the 2-week implementation sequence.
- **`learning.md`** — expanded the financial-domain section to cover all four derivative types and the listed-vs-OTC data distinction.
- **`requirements.txt`** — added `rich` (for the dashboard).

### New planned feature: Live CLI Dashboard (Module G)
Specced in `system_design.md` §5. A `rich`-based terminal dashboard (read-only consumer of the ZeroMQ bus + DuckDB) showing live ingestion feed/latency, ML anomalies, reconciliation PASS/WARN/BREAK tallies + top breaks, and pipeline health. ~1 day of work. Turns the demo from "I built this" into "watch me run it." **Not yet implemented** — design only.

### Known follow-ups / open items
- **Env-var inconsistency:** `Equities.py` uses `ALPACA_API_KEY` / `ALPACA_SECRET_KEY`; `options.py` uses `APCA_API_KEY` / `APCA_API_SECRET_KEY`. Should be unified.
- Existing `Equities.py` / `options.py` still use the flat import-time-execution pattern (the new modules don't); could be refactored to function-first for consistency.
- Build the Live CLI Dashboard (Module G).
