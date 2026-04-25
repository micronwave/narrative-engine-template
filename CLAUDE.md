# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# One-time setup (required before first pipeline run)
python build_asset_library.py

# Run the pipeline (single cycle)
python pipeline.py

# Run the FastAPI backend (port 8000)
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload

# Run the Next.js frontend (port 3000, proxies /api/* to 8000)
cd frontend && npm run dev

# Run the Flask ops dashboard (port 5000)
python dashboard/app.py

# Backend tests (from project root, always use -X utf8 on Windows)
python -X utf8 tests/test_c2_api.py    # or any test_*.py in tests/
python -X utf8 tests/test_f1_api.py

# Frontend tests
cd frontend && npx jest --watchAll=false

# Frontend type check
cd frontend && npx tsc --noEmit

# Pipeline scheduler (Windows Task Scheduler, every 4 hours with wake-to-run)
run_pipeline.bat
```

No lint step. Backend is pure Python, frontend is Next.js with Tailwind.

## Architecture

Three services run concurrently:

| Service | Port | Entry Point |
|---------|------|-------------|
| FastAPI API | 8000 | `api/main.py` (~66 endpoints) |
| Next.js frontend | 3000 | `frontend/` (proxies `/api/*` → 8000) |
| Flask ops dashboard | 5000 | `dashboard/app.py` |

### Pipeline (`pipeline.py`)

11-stage `run()` function. Steps are non-fatal (catch exceptions, log, continue). Each step is logged via `_log_step(repository, step_number, step_name, status, duration_ms)`.

1. **Ingest** (`ingester.py`) — RSS feeds + optional API ingesters
2. **Deduplicate** (`deduplicator.py`) — LSH MinHash (Jaccard ≥ 0.85)
3. **Embed** (`embedding_model.py`) — 768-dim via `all-mpnet-base-v2`
4. **Cluster** (`clustering.py`) — HDBSCAN with centroid momentum (α=0.15)
5. **Score** (`signals.py`) — velocity, cohesion, polarization, entropy, Ns score, lifecycle stages, burst velocity
6. **Centrality** (`centrality.py`) — NetworkX betweenness + catalyst flagging
7. **Adversarial** (`adversarial.py`) — Coordination burst detection (300s window, 5+ sources)
8. **LLM label** (`llm_client.py`) — Haiku labels + topic classification; Sonnet mutation analysis (budget-gated)
9. **Asset map** (`asset_mapper.py`) — FAISS cosine similarity against S&P 500 ticker library
10. **Output** (`output.py`) — Structured JSON with immutable disclaimer
11. **Persist** (`repository.py`) — SQLite

### FastAPI API (`api/main.py`)

**Module-level stub data** (in-memory, not DB):
- `ASSET_CLASSES` — 7 asset class definitions
- `TRACKED_SECURITIES` — 17 securities with live Finnhub prices (mutated in-place by background refresh loop)
- `NARRATIVE_ASSETS` — 8 narrative↔asset class associations
- `MANIPULATION_INDICATORS` — 6 coordination detection stubs

**Key functions:**
- `get_repo()` — per-request factory, returns `SqliteRepository` or raises 503
- `_build_visible_narrative(n, repo)` — transforms DB narrative dict into API response shape with timeseries, stage, burst_velocity, topic_tags

**Startup hooks** (`@app.on_event("startup")`):
- `_init_narrative_asset_ids()` — replaces placeholder IDs in NARRATIVE_ASSETS with real DB narrative IDs
- `start_price_refresh()` — launches async Finnhub price refresh loop
- `start_impact_score_refresh()` — recalculates narrative impact scores periodically

### Frontend (`frontend/`)

Next.js 14 App Router, TypeScript, Tailwind CSS, Lucide React icons. No component library (no shadcn, Radix, etc.).

**Design system** (`globals.css`): ~65 CSS custom properties. Terminal aesthetic — 2px border-radius everywhere, no glass/blur effects, elevation through background contrast. Colors: Palantir Blueprint dark grays + intent colors (primary blue, success green, warning orange, danger red).

**API client** (`src/lib/api.ts`): Typed fetch functions for all API routes. All call relative `/api/*` paths (proxied to FastAPI via `next.config.mjs` rewrites).

**Key type:** `VisibleNarrative` includes `stage`, `burst_velocity`, `topic_tags` fields.

### Key Design Patterns

**Repository pattern** (`repository.py`): `Repository` ABC + `SqliteRepository`. All DB access uses `with self._get_conn() as conn:` — never raw `self.conn`. Schema migrations via idempotent `ALTER TABLE ... ADD COLUMN` in `_ensure_tables()`.

**Non-fatal pipeline steps**: Catch exceptions, call `_log_step(..., "ERROR", ...)`, continue. Never abort the run.

**Plug-in interfaces**: `Repository`, `VectorStore`, `EmbeddingModel` are abstract. Swap implementations at the `# TODO SCALE` markers.

### Column Names (SQLite `narratives` table)

- Primary key: `narrative_id` (not `id`)
- Stage: `stage` (not `lifecycle_stage`) — values: Emerging, Growing, Mature, Declining, Dormant
- Doc count: `document_count` (not `doc_count`)
- Label: `name` (not `label`)
- `linked_assets` — JSON string, always `json.loads()` before membership checks
- `topic_tags` — JSON string array, always `json.loads()` before use
- `burst_ratio` — REAL, computed by pipeline

### LLM Client (`llm_client.py`)

- Class: `LlmClient` (not `LLMClient`)
- `call_haiku(task_type, narrative_id, prompt)` — labeling, topic classification
- `call_sonnet(narrative_id, prompt)` — mutation analysis (budget-gated)
- Task types: `label_narrative`, `classify_topic`, `classify_stage`, `validate_cluster`, `summarize_mutation_fallback`, `mutation_explanation`
- Every call logged to `llm_audit_log` table with token counts and cost estimate

### Data Layer (`api/services/`, `api/adapters/`)

Multi-provider price data with adapter pattern + circuit breaker:

- **Adapters** (`api/adapters/`): `FinnhubAdapter`, `TwelveDataAdapter`, `CoinGeckoAdapter` — each wraps a provider API behind a common interface
- **DataNormalizer** (`api/services/data_normalizer.py`) — chains adapters, returns `NormalizedQuote` regardless of source
- **CircuitBreaker** (`api/services/circuit_breaker.py`) — per-adapter; opens after 5 consecutive failures, auto-recovers after 5 min
- **WebSocketRelay** (`api/services/websocket_relay.py`) — persistent `wss://ws.finnhub.io` connection for real-time price ticks

Adapters are conditionally enabled via `ENABLE_TWELVE_DATA`, `ENABLE_COINGECKO` env vars.

### Services

- `FinnhubService` (`api/finnhub_service.py`) — REST price quotes with in-memory cache + 60 calls/min rate limiter
- `compute_velocity_price_correlation()` (`api/correlation_service.py`) — Pearson r via scipy, NaN-safe
- `get_price_history()` (`stock_data.py`) — yfinance daily OHLCV with 1-hour cache
- `EarningsService` (`api/earnings_service.py`) — upcoming earnings calendar
- `SECTOR_MAP` (`api/sector_map.py`) — ticker→sector mapping for convergence analysis

### Extension Modules

Manager-pattern classes instantiated in `api/main.py`, backed by `SqliteRepository`:

- `NotificationManager` (`notifications.py`) — rules-based alerting (rule types: `ns_above`, `ns_below`, `new_narrative`, `mutation`, `stage_change`, `catalyst`)
- `PortfolioManager` (`portfolio.py`) — holdings tracking, narrative impact scoring, CSV import (max 1000 rows)
- `WatchlistManager` (`watchlist.py`) — ticker/narrative watchlists
- `ChatManager` (`chat.py`) — multi-turn Haiku Q&A with persistent sessions, built-in prompt templates
- `ExportManager` (`export.py`) — JSON/CSV export, social share text generation

### Signal Redesign Modules

- `convergence.py` — detects multiple independent narratives converging on same ticker; independence via centroid cosine similarity below `CONVERGENCE_INDEPENDENCE_THRESHOLD`
- `source_tiers.py` — 5-tier domain authority classification (1=wire services, 5=social/retail); escalation tracking per narrative
- `validate_signal.py` — diagnostic: plots narrative velocity vs ticker price, outputs PNG + correlation summary

## Configuration

Copy `.env.example` to `.env`. Only `ANTHROPIC_API_KEY` is required. Settings validated by Pydantic v2 (`settings.py`). New fields use plain Python defaults — no `Field()` wrapper.

Key settings beyond the basics:
- `PIPELINE_FREQUENCY_HOURS: int = 4` — Task Scheduler interval
- `BURST_VELOCITY_ALERT_RATIO: float = 3.0` — ratio threshold for SURGE
- `FINNHUB_API_KEY` — enables live stock prices
- `IMPACT_SCORE_REFRESH_SECONDS: int = 600` — narrative impact score refresh
- `AUTH_MODE: str = "stub"` — `"stub"` (single-user MVP) or `"jwt"` (multi-user, requires `JWT_SECRET_KEY` ≥ 32 chars)
- `ENABLE_TWELVE_DATA` / `ENABLE_COINGECKO` — activate additional price data adapters
- `CONVERGENCE_INDEPENDENCE_THRESHOLD: float = 0.30` — centroid similarity cutoff for convergence detection

## Testing

Backend tests use a custom minimal runner (no pytest): `S(section)`, `T(name, condition, details)`. Run with `-X utf8` on Windows. All test files are in `tests/` directory.

Frontend tests use Jest + @testing-library/react. Use `data-testid` for selectors.

**Test naming:** `tests/test_{phase}{number}_api.py` (e.g., `test_f3_api.py`). Phases: C (customer API), D (data pipeline), F (features). Additional conventions:
- `test_v3_phase{N}.py` — V3 signal redesign tests
- `test_signal_p{N}.py` — signal pipeline validation
- `test_*_audit.py` — audit/quality pass tests
- `test_phase{N}_integration.py`, `test_full_integration.py` — end-to-end integration

**Test assertions that check CSS class names:** Several tests match on `bullish`, `bearish`, `alert`, `critical`, `accent-muted`, `accent-text`, `purple`, `line-through`. Changing these Tailwind utility names will break tests.

Backend: ~45 test files in `tests/`. Frontend: 13 Jest suites in `frontend/src/__tests__/`.

## Compliance

- robots.txt checked before every HTTP request (defaults ALLOW on failure)
- LLM audit trail in `llm_audit_log` table
- Disclaimer: `INTELLIGENCE ONLY — NOT FINANCIAL ADVICE` hardcoded in `output.py`
- Source attribution (`source_url`, `source_domain`, `published_at`) mandatory on every document
