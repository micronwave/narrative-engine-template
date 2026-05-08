# Narrative Intelligence Engine

Detects and tracks large-scale financial narratives as they form, mutate, and decay across fragmented media — RSS feeds, SEC filings, Reddit, news APIs. Maps them to S&P 500 tickers, scores their momentum, flags coordinated campaigns, and serves it all through a web dashboard.

Not a trading system. This watches the stories that move markets, not the markets themselves.

## How it works

An 11-stage pipeline runs every 4 hours:

```
RSS / SEC EDGAR / Reddit / MarketAux / NewsData
        │
    Ingest ─── robots.txt check, financial relevance filter
        │
   Deduplicate ─── LSH MinHash, drops anything above 0.85 Jaccard similarity
        │
     Embed ─── 768-dim dense vectors (all-mpnet-base-v2)
        │
    Cluster ─── HDBSCAN with centroid momentum tracking
        │
     Score ─── velocity, cohesion, polarization, entropy, burst detection
        │
   Centrality ─── betweenness centrality, catalyst flagging
        │
  Adversarial ─── coordination burst detection (5+ sources in 300s = suspicious)
        │
   LLM Label ─── Haiku for labeling/topics, Sonnet for mutation analysis (budget-gated)
        │
   Asset Map ─── FAISS cosine similarity against S&P 500 ticker embeddings
        │
    Persist ─── SQLite, structured JSON output
```

Each step is non-fatal — if one stage throws, it logs the error and the pipeline keeps going.

## Stack

**Backend:** Python 3.12, FastAPI (66 endpoints), SQLite, FAISS, HDBSCAN, sentence-transformers, Claude API (Haiku + Sonnet)

**Frontend:** Next.js 14 (App Router), TypeScript, Tailwind. Dark terminal aesthetic — no component library, custom design system with ~65 CSS variables. Palantir Blueprint color palette.

**Price data:** Finnhub (primary), TwelveData and CoinGecko available as secondary adapters behind a circuit breaker. WebSocket relay for real-time ticks.

## Setup

```bash
# 1. Python deps
pip install -r requirements.txt
pip install -r api/requirements.txt

# 2. Frontend deps
cd frontend && npm install && cd ..

# 3. Environment
cp .env.example .env
# Set ANTHROPIC_API_KEY at minimum. FINNHUB_API_KEY for live prices.

# 4. Build the asset library (one-time, ~15-30 min)
#    Downloads S&P 500 10-K summaries from SEC EDGAR and generates embeddings.
#    Pipeline won't run without this.
python build_asset_library.py
```

## Running

```bash
# Pipeline (single cycle)
python pipeline.py

# API server
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload

# Frontend (proxies /api/* to port 8000)
cd frontend && npm run dev

On Windows, `run_pipeline.bat` is the Task Scheduler entry point — set it to run every 4 hours with wake-to-run enabled. Logs go to `logs/`.

## What the frontend shows

| Route | What's there |
|-------|-------------|
| `/` | Signal radar — narrative cards with lifecycle badges, topic tags, velocity sparklines |
| `/narrative/[id]` | Deep dive — momentum, 30-day velocity chart, mutation timeline, linked assets |
| `/constellation` | SVG graph of narrative relationships |
| `/signals` | Signal inbox with coordination flags |
| `/stocks` | Securities table with narrative impact scores |
| `/market-impact` | Cross-narrative market impact view |
| `/manipulation` | Coordination detection dashboard |
| `/brief/[ticker]` | Pre-earnings intelligence brief with price history and correlation links |
| `/correlation` | Velocity-price correlation explorer (Pearson r, lead-time sweep) |
| `/analytics` | Aggregate analytics — leaderboards, timelines, heatmaps |

## Narrative lifecycle

Narratives move through stages automatically based on their metrics:

- **Emerging** — fewer than 8 documents, just appeared
- **Growing** — 8+ docs, velocity above 0.05
- **Mature** — 5+ days old, high entropy, 15+ documents
- **Declining** — 3 consecutive declining days or velocity drops below 0.02
- **Dormant** — 7+ declining days, velocity under 0.01

They can revive. If a dormant narrative's velocity spikes past 0.10, it jumps back to Growing.

Each narrative also gets 1-3 topic tags from the LLM: `regulatory`, `earnings`, `geopolitical`, `macro`, `esg`, `m&a`, `crypto`.

## Key modules

| File | Role |
|------|------|
| `pipeline.py` | Orchestrates the 11-stage run |
| `signals.py` | Velocity, entropy, lifecycle staging, burst detection |
| `clustering.py` | HDBSCAN narrative discovery with centroid momentum |
| `llm_client.py` | Claude API integration — Haiku for labeling, Sonnet for mutation analysis |
| `repository.py` | SQLite persistence, schema migrations, repository pattern |
| `convergence.py` | Detects independent narratives converging on the same ticker |
| `source_tiers.py` | 5-tier domain authority classification, escalation tracking |
| `adversarial.py` | Coordination burst detection |
| `api/main.py` | FastAPI — 66 routes serving the frontend + data exports |
| `api/services/` | Circuit breaker, data normalizer, WebSocket relay |
| `api/adapters/` | Finnhub, TwelveData, CoinGecko price adapters |

Extension modules (notifications, watchlist, export) are manager-pattern classes instantiated in `api/main.py`.

## Tests

Backend tests use a minimal custom runner (no pytest). Frontend tests use Jest + Testing Library.

```bash
# Backend — run from project root, always use -X utf8 on Windows
python -X utf8 tests/test_c2_api.py
python -X utf8 tests/test_d1_api.py
# ... 46 test files total across C (customer), D (data), F (feature),
#     v3 (signal redesign), signal_p (pipeline), and audit suites

# Frontend
cd frontend && npx jest --watchAll=false
npx tsc --noEmit
```

## Configuration

Everything loads from `.env` via Pydantic v2 (`settings.py`). See `.env.example` for the full list.

Only `ANTHROPIC_API_KEY` is required. Everything else has sensible defaults.

Notable optional keys:
- `FINNHUB_API_KEY` — live stock prices
- `MARKETAUX_API_KEY`, `NEWSDATA_API_KEY` — additional news sources
- `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET` — Reddit ingestion
- `ENABLE_TWELVE_DATA`, `ENABLE_COINGECKO` — secondary price adapters
- `SONNET_DAILY_TOKEN_BUDGET` (default 200k) — caps daily Sonnet spend

## Scaling

The architecture is designed for single-machine use right now. Three swap points are marked with `# TODO SCALE` in the code:

- **Storage:** `SqliteRepository` → Postgres
- **Vectors:** `FaissVectorStore` → pgvector or Pinecone
- **Graph:** NetworkX → distributed graph engine (needed past ~10k narratives)

## Compliance

- robots.txt is checked before every HTTP fetch
- Every LLM call is logged to `llm_audit_log` with token counts and cost
- Source attribution (URL, domain, publish date) is mandatory on all ingested documents
- Output carries a hardcoded disclaimer: *INTELLIGENCE ONLY — NOT FINANCIAL ADVICE*

## Disclaimer

This is an intelligence tool, not a financial advisor. It does not generate buy/sell recommendations, price targets, or investment advice.
