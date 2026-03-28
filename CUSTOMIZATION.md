# Customization Guide

This template is designed to be adapted to your specific use case. Below are the main extension points.

## 1. Data sources

### RSS feeds

Edit `ingester.py` — the `_DEFAULT_FEEDS` list contains curated financial RSS feeds. Add, remove, or replace with feeds relevant to your domain.

```python
# Override by passing feed_urls to RssIngester constructor
ingester = RssIngester(feed_urls=["https://your-feed.com/rss"])
```

### API ingesters

Enable additional news sources in `.env`:

```env
ENABLE_MARKETAUX=true
MARKETAUX_API_KEY=your-key

ENABLE_NEWSDATA=true
NEWSDATA_API_KEY=your-key

ENABLE_REDDIT=true
REDDIT_CLIENT_ID=your-id
REDDIT_CLIENT_SECRET=your-secret

ENABLE_EDGAR=true
EDGAR_EMAIL=your-email@example.com
```

## 2. Asset universe

### Ticker library

`build_asset_library.py` builds the FAISS embedding index from SEC EDGAR 10-K filings. Modify the `TICKERS` dict to change which companies are tracked. The default is S&P 500.

### Sector mapping

`api/sector_map.py` maps tickers to sectors for convergence analysis. Update this when you change the ticker universe.

### Tracked securities (API stubs)

`api/main.py` has in-memory stub data (`TRACKED_SECURITIES`, `ASSET_CLASSES`, `NARRATIVE_ASSETS`) that the frontend renders. Replace these with your own asset universe. The data shape is documented in comments above each structure.

## 3. LLM configuration

### Models

Set model versions in `.env`:

```env
HAIKU_MODEL=claude-haiku-4-5-20251001
SONNET_MODEL=claude-sonnet-4-6
```

### Budget

Control daily Sonnet spend:

```env
SONNET_DAILY_TOKEN_BUDGET=200000
SONNET_MAX_TOKENS=2048
HAIKU_MAX_TOKENS=512
```

### Prompts

Labeling and analysis prompts live in `llm_client.py`. The `call_haiku()` method handles labeling, topic classification, and stage classification. The `call_sonnet()` method handles mutation analysis. Modify the prompt templates to fit your domain.

## 4. Frontend theming

### Design system

The design system is defined by ~65 CSS custom properties in `frontend/src/app/globals.css`. Key variables:

```css
--bg-primary       /* Main background */
--bg-secondary     /* Card backgrounds */
--text-primary     /* Main text color */
--accent-primary   /* Primary action color (blue) */
--success          /* Positive signal (green) */
--warning          /* Caution signal (orange) */
--danger           /* Negative signal (red) */
```

### Tailwind

Tailwind config is in `frontend/tailwind.config.ts`. The color palette uses Palantir Blueprint-inspired dark grays by default.

### Typography

The default font is DM Sans (loaded in `frontend/src/app/layout.tsx`). Change the Google Fonts import to swap typefaces.

## 5. Social posting

The pipeline has a hook point for adding a social posting module. In `pipeline.py`, look for:

```python
# Hook point: add social posting module here
```

Create your own bot module and import it there. The pipeline passes `repository` and `settings` to the dispatch function. See the extension modules (notifications, export) for patterns to follow.

## 6. Scaling

Three swap points are marked with `# TODO SCALE` in the code:

| Component | Current | Scale target |
|-----------|---------|-------------|
| Storage | `SqliteRepository` | PostgreSQL (swap in `repository.py`) |
| Vectors | `FaissVectorStore` | pgvector or Pinecone (swap in `vector_store.py`) |
| Graph | NetworkX in-memory | Distributed graph engine (swap in `centrality.py`) |

The `Repository` and `VectorStore` ABCs define the interface contracts. Implement a new class conforming to the ABC and swap at the construction site.

## 7. Authentication

Set `AUTH_MODE` in `.env`:

- `stub` (default) — single-user mode, no real auth
- `jwt` — multi-user with JWT tokens. Requires `JWT_SECRET_KEY` (32+ chars)

## 8. Price data providers

Finnhub is the primary adapter. Enable secondary providers:

```env
ENABLE_TWELVE_DATA=true
TWELVE_DATA_API_KEY=your-key

ENABLE_COINGECKO=true
COINGECKO_API_KEY=your-key
```

Each adapter sits behind a circuit breaker (`api/services/circuit_breaker.py`) that opens after 5 consecutive failures and auto-recovers after 5 minutes.
