# Narrative Intelligence Engine — Template

> **This is a template repository.** Fork it, customize the data sources and asset universe, and build your own narrative intelligence system. This is work in progress, so stay up to date with new commits.

Detects and tracks large-scale financial narratives as they form, mutate, and decay across fragmented media — RSS feeds, SEC filings, Reddit, news APIs. Maps them to S&P 500 tickers, scores their momentum, flags coordinated campaigns, and serves it all through a web dashboard.

Not a trading system. This watches the stories that move markets, not the markets themselves.

<img width="1261" height="1191" alt="btc" src="https://github.com/user-attachments/assets/8ef67db0-6ba7-4143-aa22-7d72f93b90cf" />

## Quick start

```bash
# 1. Clone and enter
git clone https://github.com/YOUR_USERNAME/narrative-engine-template.git
cd narrative-engine-template

# 2. Python deps
pip install -r requirements.txt
pip install -r api/requirements.txt

# 3. Frontend deps
cd frontend && npm install && cd ..

# 4. Environment
cp .env.example .env
# Set ANTHROPIC_API_KEY at minimum. FINNHUB_API_KEY for live prices.

# 5. Build the asset library (one-time, ~15-30 min)
#    Downloads S&P 500 10-K summaries from SEC EDGAR and generates embeddings.
python build_asset_library.py

# 6. Run the pipeline
python pipeline.py

# 7. Start the services
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload   # API
cd frontend && npm run dev                                      # Frontend (port 3000)
python dashboard/app.py                                         # Ops dashboard (port 5000)
```

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

## Signal effectiveness

The system tracks 15 numeric signals per narrative per cycle. Not all signals are equally useful. Early validation across 17 narrative/ticker pairs shows a clear hierarchy:

**Tier 1 — strongest predictors of next-day price movement:**

| Signal | Strength | Direction |
|--------|----------|-----------|
| Burst ratio | 0.415 | Mixed — predicts volatility, not direction |
| Centrality | 0.401 | Negative — hub narratives signal crowded trades |
| Document count | 0.396 | Positive — more coverage, price rises |
| Windowed velocity | 0.375 | Positive — recency-weighted acceleration |

**Key findings:**
- **Volume signals dominate.** Narrative attention (how much coverage, how fast it's growing, whether it's spiking) is more predictive than sentiment, cohesion, or any other signal class.
- **Sentiment is weak.** Sentiment mean (0.242) is the *least* predictive signal. Sentiment variance (0.316) has some value as a volatility indicator but regresses with more data.
- **Centrality is contrarian.** When one narrative connects everything in the graph, it means the market has already organized around it. High centrality → consensus → already priced in.
- **The composite score (Ns) underperforms its own inputs.** The weighted aggregate (0.317) is less predictive than burst ratio, doc count, or velocity individually — the weighting formula dilutes signal from the strong components.
- **Cross-source agreement is contrarian.** When many independent sources agree on a narrative (high cross-source score), prices tend to fall the next day. Consistent with crowded-trade dynamics.

These results are directional (17 pairs, 2-16 observations each), not statistically robust yet. The validation suite in the codebase (`/api/correlations/`) lets you run your own analysis as data accumulates.

## What makes this different

Most news-as-signal systems work at the **entity level** — count articles about $NVDA, score their sentiment, trade on that. This system works at the **narrative level**: it clusters articles into stories first, tracks those stories as units, then maps them to assets.

The difference matters because:

1. **Narratives capture thematic shifts before they localize to tickers.** "Semiconductor supply chain stress" affects $NVDA, $TSM, $INTC, and $ASML simultaneously. Entity-level systems see four separate signals. This system sees one narrative with four asset mappings — and detects it earlier because the cluster forms before any single ticker accumulates enough coverage to trigger an alert.

2. **The graph layer adds structural signal.** The system builds a network where narratives are nodes and semantic similarity creates edges. Betweenness centrality on that graph — which narrative sits at the crossroads of information flow — is a signal type that entity-level systems can't produce. It turned out to be the strongest directional predictor.

3. **Convergence detection finds independent confirmation.** When two narratives form independently (low centroid similarity) but both map to the same ticker, that's convergence — the strongest signal class in the system. Two separate stories pointing the same direction on the same asset, from different source clusters, is more informative than either story alone.

4. **Volume over sentiment.** The academic literature supports this (Fang & Peress 2009, Barber & Odean 2008, Da et al. 2011): the *amount* of coverage predicts price movement more reliably than the *tone* of coverage. This system is built around that finding — burst detection, velocity tracking, source counting, and escalation monitoring are all volume-based signals.

Commercial platforms in this space (RavenPack, MarketPsych/LSEG, Predata, Bloomberg Event-Driven Feeds) sell entity-level news analytics to institutional desks. The narrative-first architecture and graph-topology signals are less explored territory.

## Stack

**Backend:** Python 3.12, FastAPI (66 endpoints), SQLite, FAISS, HDBSCAN, sentence-transformers, Claude API (Haiku + Sonnet)

**Frontend:** Next.js 14 (App Router), TypeScript, Tailwind. Dark terminal aesthetic — no component library, custom design system with ~65 CSS variables.

**Ops:** Flask dashboard on port 5000 for pipeline monitoring.

**Price data:** Finnhub (primary), TwelveData and CoinGecko available as secondary adapters behind a circuit breaker. WebSocket relay for real-time ticks.

## Frontend routes

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
| `/portfolio` | Portfolio holdings with narrative exposure tracking |
| `/alerts` | Alert management with real-time SSE streaming |
| `/dashboard` | Customizable widget grid (10 widget types) |
| `/sentiment` | Social sentiment analysis across sources |
| `/social` | Social mentions and trending narratives |
| `/analytics` | Aggregate analytics — leaderboards, timelines, heatmaps |

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

Extension modules (notifications, portfolio, watchlist, chat, export) are manager-pattern classes instantiated in `api/main.py`.

## Customization

See [CUSTOMIZATION.md](CUSTOMIZATION.md) for a detailed guide. Key extension points:

- **Data sources** — add RSS feeds in `ingester.py`, enable API ingesters in `.env`
- **Asset universe** — modify tickers in `build_asset_library.py` and `api/sector_map.py`
- **Tracked securities** — edit the example stub data in `api/main.py`
- **LLM prompts** — adjust labeling and analysis prompts in `llm_client.py`
- **Frontend theme** — modify CSS variables in `frontend/src/app/globals.css`
- **Social posting** — pipeline has a hook point for adding a bot module (see `pipeline.py`)

## Scaling

Three swap points are marked with `# TODO SCALE` in the code:

- **Storage:** `SqliteRepository` → Postgres
- **Vectors:** `FaissVectorStore` → pgvector or Pinecone
- **Graph:** NetworkX → distributed graph engine (needed past ~10k narratives)

## Tests

Backend tests use a minimal custom runner (no pytest). Frontend tests use Jest + Testing Library. See `tests/README.md` for details.

```bash
# Backend — always use -X utf8 on Windows
python -X utf8 tests/test_c1_api.py

# Frontend
cd frontend && npx jest --watchAll=false
npx tsc --noEmit
```

## Configuration

Everything loads from `.env` via Pydantic v2 (`settings.py`). See `.env.example` for the full list.

Only `ANTHROPIC_API_KEY` is required. Everything else has sensible defaults.

## Compliance

- robots.txt is checked before every HTTP fetch
- Every LLM call is logged to `llm_audit_log` with token counts and cost
- Source attribution (URL, domain, publish date) is mandatory on all ingested documents
- Output carries a hardcoded disclaimer: *INTELLIGENCE ONLY — NOT FINANCIAL ADVICE*

## License

[MIT](LICENSE)

## Disclaimer

This is an intelligence tool, not a financial advisor. It does not generate buy/sell recommendations, price targets, or investment advice.
