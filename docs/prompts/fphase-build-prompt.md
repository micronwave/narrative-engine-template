# Narrative Intelligence Platform — F Phase Build Prompts (F1–F6)
# Pipeline Intelligence, Lifecycle Stages, Intra-Day Velocity & Pre-Earnings Briefs

---

## Stack Constraints (read before building any phase)

You are extending an existing codebase. Adhere to these constraints across all F-phase work:

- **Backend:** Python pipeline (`pipeline.py`) + FastAPI (`api/main.py`) + SQLite (`repository.py`). All pipeline stages are functions called by `pipeline.run()`. New signal functions go in `signals.py`. New settings go in `settings.py` with plain Python defaults (no `Field()` wrapper).
- **Frontend:** Next.js 14 App Router, TypeScript, Tailwind CSS, Lucide React icons. Design tokens from E-phase (`globals.css` CSS variables, `tailwind.config.ts` semantic mappings). Use `bg-surface`, `text-text-primary`, `text-accent-text`, `border-border-subtle` etc.
- **Testing — backend:** Custom minimal runner (`S(section)`, `T(name, condition, details)`). Run with `python -X utf8 test_f{n}_api.py`. No pytest.
- **Testing — frontend:** Jest + @testing-library/react. `data-testid` attributes throughout. Run with `cd frontend && npx jest --watchAll=false`.
- **Regression policy:** All prior test files must still pass before closing any phase.
- **frontend_build_log:** Append one concise line per F-phase. Update `COMPLETE` line.

**Dependency chain:** F1 is the prerequisite for all phases. After F1: F2 and F4 can run in parallel (independent). F3 depends on F1 + F2. F5 depends on F1. F6 depends on F5.
```
F1 (lifecycle stages)
├── F2 (pipeline frequency + burst) ──┐
├── F4 (topic tagging) ──────────────┤
│                                     └── F3 (pre-earnings brief)
└── F5 (historical snapshots + price) ── F6 (correlation)
```

---

## F1 — Narrative Lifecycle Stage Progression

You are building phase F1. The existing codebase hardcodes `stage: "Emerging"` for every narrative (line 165 in `clustering.py`). The `narrative_snapshots` table stores daily snapshots with a `lifecycle_stage` field, but no logic ever advances narratives beyond "Emerging." This phase adds automatic lifecycle progression.

### OBJECTIVE: LIFECYCLE STAGE ENGINE

Read `repository.py`, `signals.py`, `clustering.py`, and `pipeline.py` before writing any code.

### Step 1: Define stage progression rules in `signals.py`

Add a pure function:

```python
def compute_lifecycle_stage(
    current_stage: str,
    document_count: int,
    velocity_windowed: float,
    entropy: float | None,
    consecutive_declining_days: int,
    days_since_creation: int,
) -> str:
    """
    Returns the new lifecycle stage based on narrative metrics.

    Stages: Emerging → Growing → Mature → Declining → Dormant

    Rules:
    - Emerging → Growing: document_count >= 8 AND velocity_windowed > 0.05
    - Growing → Mature: days_since_creation >= 5 AND entropy is not None AND entropy >= 1.5
        AND document_count >= 15
    - Mature → Declining: consecutive_declining_days >= 3 OR velocity_windowed < 0.02
    - Declining → Dormant: consecutive_declining_days >= 7 AND velocity_windowed < 0.01
    - Any stage can regress: if velocity spikes above 0.10 and stage is Declining/Dormant,
        return "Growing" (revival)

    Never skip stages (Emerging cannot jump to Mature).
    """
```

### Step 2: Integrate into pipeline stage 5 (scoring)

In `pipeline.py`, after `compute_ns_score()` is called for each narrative, call `compute_lifecycle_stage()` and update the narrative's `stage` field via `repository.update_narrative_stage(narrative_id, new_stage)`.

Add `update_narrative_stage(narrative_id, stage)` to `repository.py` if it doesn't exist.

### Step 3: Extract `days_since_creation` helper

The age calculation already exists inline in `llm_client.py` (lines 87-99). Extract it into a shared utility function in `signals.py` (or `repository.py`) so both `llm_client.py` and the lifecycle stage logic can reuse it:

```python
def get_narrative_age_days(created_at: str) -> int:
    """Returns days since narrative creation. Reused from llm_client.py logic."""
    created_date = datetime.fromisoformat(created_at).date()
    today = datetime.now(timezone.utc).date()
    return (today - created_date).days
```

Update `llm_client.py` to import and call this shared function instead of its inline version.

### Step 4: Surface lifecycle stage in API and frontend

**`api/main.py`:** The `_build_visible_narrative()` function (line 637-672) does NOT currently include `stage` in its response — you must add it. The `narratives` table has a `stage` column already. Add `"stage": n.get("stage", "Emerging")` to the returned dict.

**Frontend — NarrativeCard:** Add a small stage badge next to the narrative name.
- Emerging: `bg-accent-muted text-accent-text` pill
- Growing: `bg-bullish-bg text-bullish` pill
- Mature: `bg-alert-bg text-alert` pill
- Declining: `bg-bearish-bg text-bearish` pill
- Dormant: `bg-inset text-text-disabled` pill

The badge should have `data-testid="stage-badge"` and render the stage name in lowercase.

**Type update:** Add `stage: string` to `VisibleNarrative` in `frontend/src/lib/api.ts`.

### TESTS

**Backend — `test_f1_api.py`:**
```
F1-U1: compute_lifecycle_stage returns "Emerging" for brand-new narrative (low doc count)
F1-U2: compute_lifecycle_stage returns "Growing" when doc_count >= 8 and velocity > 0.05
F1-U3: compute_lifecycle_stage returns "Mature" when days >= 5, entropy >= 1.5, docs >= 15
F1-U4: compute_lifecycle_stage returns "Declining" when consecutive_declining >= 3
F1-U5: compute_lifecycle_stage returns "Dormant" when declining >= 7 and velocity < 0.01
F1-U6: Revival — Declining + velocity > 0.10 returns "Growing"
F1-U7: Cannot skip stages — Emerging with high entropy still returns "Growing" not "Mature"
F1-U8: GET /api/narratives includes "stage" field in response
```

**Frontend — `frontend/src/__tests__/f1.test.tsx`:**
```
F1-U1: NarrativeCard renders stage badge with data-testid="stage-badge"
F1-U2: Stage "emerging" renders with accent styling
F1-U3: Stage "growing" renders with bullish styling
F1-U4: Stage "mature" renders with alert styling
```

### GOVERNANCE
- Run all prior backend + frontend tests — no regressions
- Append to `frontend_build_log`: `F1 — Lifecycle stage engine (Emerging→Growing→Mature→Declining→Dormant), stage badges on NarrativeCard; [N] backend + [N] frontend tests pass — YYYY-MM-DD`

---

## F2 — Pipeline Frequency + Intra-Day Ingestion Rate

You are building phase F2. The pipeline currently runs once daily via `run_daily.bat`. This phase adds configurable frequency and an intra-day "burst velocity" metric that measures document ingestion rate acceleration.

### OBJECTIVE: HOURLY PIPELINE + BURST VELOCITY METRIC

Read `pipeline.py`, `signals.py`, `settings.py`, `run_daily.bat`, and `repository.py` before writing any code.

### Step 1: Add frequency settings to `settings.py`

```python
PIPELINE_FREQUENCY_HOURS: int = 4        # default: run every 4 hours
BURST_VELOCITY_ALERT_RATIO: float = 3.0  # ratio above baseline that triggers alert
```

### Step 2: Add burst velocity function to `signals.py`

```python
def compute_burst_velocity(
    recent_doc_count: int,
    baseline_docs_per_window: float,
) -> dict:
    """
    Measures document ingestion rate acceleration.

    Returns:
        {
            "rate": float,          # docs per window_hours
            "baseline": float,      # average docs per window over last 7 days
            "ratio": float,         # rate / baseline (1.0 = normal, 3.0 = 3x spike)
            "is_burst": bool,       # ratio >= BURST_VELOCITY_ALERT_RATIO
        }
    """
```

This is NOT centroid-based — it's a simple document count acceleration metric. Much faster to compute and more intuitive: "this narrative just received 3x its normal document rate in the last 2 hours."

**Graceful degradation:** When baseline data is insufficient (fewer than 7 days of sub-daily snapshots), `compute_burst_velocity` must return `{"rate": recent_doc_count, "baseline": 0, "ratio": 0, "is_burst": false}`. Do NOT divide by zero or raise an error. The burst feature activates naturally once enough baseline data accumulates.

### Step 3: Store burst data in repository

Add columns to `narrative_snapshots` (or a new `narrative_bursts` table):
- `burst_ratio REAL` — the ratio value
- `burst_detected_at TEXT` — ISO timestamp of last burst
- `snapshot_time TEXT` — add time-of-day precision to snapshots (currently date only)

Add repository method: `get_baseline_doc_rate(narrative_id, window_hours, lookback_days) -> float`
- Queries `narrative_snapshots` for average doc_count change per window over last `lookback_days`

### Step 4: Integrate into pipeline

After stage 5 (scoring), for each narrative:
1. Count documents assigned to this narrative within the current pipeline cycle
2. Get baseline rate via repository
3. Compute burst velocity
4. Store result
5. If `is_burst`, log a warning: `[BURST] Narrative "{name}" — {ratio}x normal rate`

### Step 5: Create `run_pipeline.bat` (replaces `run_daily.bat`)

```batch
@echo off
cd /d E:\narrative_engine
if not exist logs mkdir logs
python pipeline.py >> logs\pipeline_%date:~-4,4%%date:~-10,2%%date:~-7,2%_%time:~0,2%%time:~3,2%.log 2>&1
exit /b %ERRORLEVEL%
```

Include a comment with Task Scheduler instructions for 4-hour frequency.

### Step 6: Surface burst velocity in API and frontend

**`api/main.py`:** Add `burst_velocity` field to `_build_visible_narrative()` response if available.

**Frontend — NarrativeCard:** When `burst_velocity?.is_burst` is true, show a small `"SURGE"` indicator next to the velocity value.
- Style: `bg-critical-bg text-critical text-xs font-medium px-1.5 py-0.5 rounded-full`
- `data-testid="burst-indicator"`
- Tooltip: "Document ingestion rate is {ratio}x above normal"

### TESTS

**Backend — `test_f2_api.py`:**
```
F2-U1: compute_burst_velocity returns ratio 1.0 when rate equals baseline
F2-U2: compute_burst_velocity returns is_burst=true when ratio >= 3.0
F2-U3: compute_burst_velocity handles zero baseline gracefully (returns ratio 0)
F2-U4: get_baseline_doc_rate returns sensible average from snapshot history
F2-U5: Pipeline run completes without error (basic smoke test)
```

**Frontend — `frontend/src/__tests__/f2.test.tsx`:**
```
F2-U1: Burst indicator renders when burst_velocity.is_burst is true
F2-U2: Burst indicator not rendered when is_burst is false
F2-U3: Burst indicator shows "SURGE" text
```

### GOVERNANCE
- Run all prior tests — no regressions
- Append to `frontend_build_log`

---

## F3 — Ticker-Focused Query Endpoint (Pre-Earnings Brief)

You are building phase F3. This phase adds a focused query layer that packages narrative intelligence around a specific ticker into a structured "brief." It does NOT modify the pipeline — it queries existing data.

### OBJECTIVE: `GET /api/brief/{ticker}` ENDPOINT + FRONTEND PAGE

Read `api/main.py`, `repository.py`, `asset_mapper.py`, and `frontend/src/lib/api.ts` before writing any code.

### Step 1: New endpoint in `api/main.py`

```
GET /api/brief/{ticker}
  Returns a pre-earnings intelligence brief for the given ticker symbol.
  No auth required.

  Response shape:
  {
    "ticker": "AAPL",
    "security": { ...TrackedSecurity fields... } | null,
    "narratives": [
      {
        "id": "nar-001",
        "name": "...",
        "stage": "Growing",
        "velocity_windowed": 0.14,
        "entropy": 1.82,
        "entropy_interpretation": "Multi-source coverage — diverse perspectives",
        "burst_velocity": { "ratio": 1.2, "is_burst": false },
        "coordination_flags": 0,
        "exposure_score": 0.85,
        "direction": "bullish",
        "days_active": 12,
        "signal_count": 23,
        "top_signals": [ ...top 3 signals... ]
      }
    ],
    "risk_summary": {
      "coordination_detected": false,
      "highest_burst_ratio": 1.2,
      "dominant_direction": "bullish",
      "narrative_count": 3,
      "avg_entropy": 1.65,
      "entropy_assessment": "Broad coverage — approaching consensus"
    },
    "generated_at": "2026-03-17T12:00:00Z"
  }
```

**Entropy interpretation logic:**
- entropy < 0.5: "Narrow sourcing — potential echo chamber"
- entropy 0.5–1.0: "Limited diversity — monitor for astroturfing"
- entropy 1.0–2.0: "Multi-source coverage — diverse perspectives"
- entropy > 2.0: "Broad coverage — approaching consensus"

**How to find narratives for a ticker:**
1. Look up ticker in `TRACKED_SECURITIES` → get `asset_class_id`
2. Find `NARRATIVE_ASSETS` where `asset_class_id` matches → get `narrative_id` list
3. For each narrative: query `get_narrative(narrative_id)` from repo, build the response object
4. Also check `MANIPULATION_INDICATORS` for any matching narrative_ids → set `coordination_flags`

### Step 2: Frontend page — `/brief/[ticker]/page.tsx`

Create `frontend/src/app/brief/[ticker]/page.tsx`.

**Layout:**
```
<h1>{ticker} Intelligence Brief</h1>
<p class="subtitle">Narrative intelligence report for {security.name}</p>

[Security header card]
  - Symbol, name, exchange, current price, 24h change
  - data-testid="brief-security-header"

[Risk Summary panel]  data-testid="brief-risk-summary"
  - Cards showing: narrative count, dominant direction, avg entropy + interpretation,
    coordination status, highest burst ratio
  - Use semantic colors: bullish/bearish for direction, alert for coordination

[Affecting Narratives list]  data-testid="brief-narratives"
  Each narrative card:
  - Name (link to /narrative/{id}), stage badge (from F1)
  - Velocity + burst indicator (from F2)
  - Entropy + interpretation text
  - Direction badge
  - Top 3 signal headlines
  - Coordination flag count (if > 0, show alert badge)
```

### Step 3: Navigation — nest Briefs under Stocks (no new NavBar item)

Do NOT add a 6th NavBar item — the mobile bottom bar is already at capacity with 5 items. Instead:

**Option A (recommended):** Add a "View Brief" button on the StockDetailDrawer for each security. When a user clicks a stock row and opens the drawer, include a prominent `"Intelligence Brief →"` link at the top that navigates to `/brief/{symbol}`.

**Option B:** Add a tab or toggle on the `/stocks` page itself: "Table View" | "Brief View".

**Briefs index page:** Create `frontend/src/app/brief/page.tsx` as a standalone page (accessible via URL, not NavBar):
- Fetches `GET /api/securities` (using the existing `fetchSecurities()` function) to list all tracked tickers
- Each ticker links to `/brief/{symbol}`
- Shows current price, impact score, narrative count

### Step 4: Type additions to `frontend/src/lib/api.ts`

```ts
export type TickerBrief = {
  ticker: string;
  security: TrackedSecurity | null;
  narratives: BriefNarrative[];
  risk_summary: RiskSummary;
  generated_at: string;
};

export type BriefNarrative = {
  id: string;
  name: string;
  stage: string;
  velocity_windowed: number;
  entropy: number | null;
  entropy_interpretation: string;
  burst_velocity: { ratio: number; is_burst: boolean } | null;
  coordination_flags: number;
  exposure_score: number;
  direction: string;
  days_active: number;
  signal_count: number;
  top_signals: { headline: string; source: string; timestamp: string }[];
};

export type RiskSummary = {
  coordination_detected: boolean;
  highest_burst_ratio: number;
  dominant_direction: string;
  narrative_count: number;
  avg_entropy: number;
  entropy_assessment: string;
};

export async function fetchBrief(ticker: string): Promise<TickerBrief>
// NOTE: fetchSecurities() already exists at line 282 — do NOT add a duplicate.
```

### TESTS

**Backend — `test_f3_api.py`:**
```
F3-U1: GET /api/brief/TSM returns 200 with ticker field
F3-U2: Response includes narratives array with at least 1 entry
F3-U3: Each narrative has entropy_interpretation string
F3-U4: risk_summary has all required fields
F3-U5: GET /api/brief/INVALID returns 404
F3-U6: Entropy interpretation matches expected ranges
F3-U7: coordination_flags count matches MANIPULATION_INDICATORS data
```

**Frontend — `frontend/src/__tests__/f3.test.tsx`:**
```
F3-U1: /brief/TSM page renders "TSM Intelligence Brief" heading
F3-U2: Risk summary panel renders with data-testid="brief-risk-summary"
F3-U3: Narrative cards render with stage badge and entropy interpretation
F3-U4: /brief index page renders list of tracked securities
```

### GOVERNANCE
- Run all prior tests — no regressions
- Append to `frontend_build_log`

---

## F4 — Regulatory & Sector Topic Tagging

You are building phase F4. This phase adds topic tags to narratives so users can filter by regulatory, sector, or thematic categories. Tags are generated during the LLM labeling step.

### OBJECTIVE: TOPIC TAGS ON NARRATIVES

Read `llm_client.py`, `pipeline.py` (stage 8), `api/main.py`, and `frontend/src/app/page.tsx` before writing any code.

### Step 1: Schema migration — add `topic_tags` column

The `narratives` table does NOT have a `topic_tags` column. Add it via SQLite ALTER TABLE in `repository.py`'s `_ensure_tables()` method (idempotent pattern):

```python
# In _ensure_tables(), after existing CREATE TABLE statements:
try:
    conn.execute("ALTER TABLE narratives ADD COLUMN topic_tags TEXT DEFAULT NULL")
except Exception:
    pass  # Column already exists
```

Add repository method: `update_narrative_tags(narrative_id: str, tags: list[str])` — stores as JSON string via `json.dumps(tags)`.

### Step 2: Add topic tagging to LLM labeling

In `pipeline.py` stage 8 (LLM label), after calling `call_haiku()` for narrative naming, make a second call to classify the narrative's topic — **but only for NEW narratives that don't already have tags** (to avoid redundant API calls on every pipeline run):

```python
# Only tag narratives that haven't been tagged yet
existing_tags = repository.get_narrative(narrative_id).get("topic_tags")
if existing_tags is None:
    topic_prompt = f"""Given this narrative cluster about "{name}", classify it into 1-3 topic tags from this list:
    - regulatory (government policy, legislation, sanctions, tariffs, rate decisions)
    - earnings (corporate earnings, revenue, guidance)
    - geopolitical (international conflict, diplomacy, trade war)
    - macro (inflation, employment, GDP, monetary policy)
    - sector:{sector_name} (industry-specific: tech, energy, healthcare, finance, etc.)
    - esg (environmental, social, governance)
    - m&a (mergers, acquisitions, restructuring)
    - crypto (cryptocurrency, blockchain, digital assets)

    Return only the tag names, comma-separated. Example: "regulatory, macro"
    """
    raw_tags = llm_client.call_haiku("classify_topic", narrative_id, topic_prompt)
    tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    repository.update_narrative_tags(narrative_id, tags)
```

**Cost control:** At hourly pipeline frequency (F2), this only fires once per narrative (when `topic_tags IS NULL`), not on every run. Existing narratives keep their tags.

### Step 3: Surface in API

Add `topic_tags: string[]` field to narrative API responses (`_build_visible_narrative()`). Parse from JSON: `json.loads(n.get("topic_tags") or "[]")`.

Add query parameters to `GET /api/narratives`:
```python
@app.get("/api/narratives")
def get_narratives(
    x_auth_token: Optional[str] = Header(None),
    topic: Optional[str] = None,    # filter by topic tag
    stage: Optional[str] = None,    # filter by lifecycle stage (from F1)
):
```
Currently this endpoint accepts only the auth header — the `topic` and `stage` params are new. Filter logic: if `topic` is provided, only include narratives where `topic` is in their `topic_tags` JSON array. If `stage` is provided, only include narratives where `stage` matches.

### Step 4: Frontend — topic filter on gateway page

Add a topic filter dropdown to `page.tsx` above the narrative grid:
- Options: All, Regulatory, Earnings, Geopolitical, Macro, ESG, M&A, Crypto
- Filters client-side (same pattern as stocks page filters)
- Each NarrativeCard shows topic tags as small pills below the descriptor

### TESTS

**Backend — `test_f4_api.py`:**
```
F4-U1: GET /api/narratives returns items with topic_tags field (list)
F4-U2: GET /api/narratives?topic=regulatory filters correctly
F4-U3: topic_tags field is a list of strings
```

**Frontend — `frontend/src/__tests__/f4.test.tsx`:**
```
F4-U1: Topic filter dropdown renders on gateway page
F4-U2: NarrativeCard renders topic tag pills
```

### GOVERNANCE
- Run all prior tests — no regressions
- Append to `frontend_build_log`

---

## F5 — Historical Snapshot API + Velocity-Price Correlation Groundwork

You are building phase F5. This phase adds a time-travel API for querying historical narrative state, stores linked assets in snapshots, and lays the groundwork for velocity-price correlation analysis.

### OBJECTIVE: HISTORICAL DATA ACCESS + PRICE DATA INTEGRATION

Read `repository.py`, `api/main.py`, `narrative_snapshots` schema, and check `requirements.txt` for `yfinance` before writing any code.

### Step 1: Extend `narrative_snapshots` table

Add columns via idempotent ALTER TABLE in `repository.py`'s `_ensure_tables()`:

```python
for col, coltype in [
    ("linked_assets", "TEXT DEFAULT NULL"),
    ("topic_tags", "TEXT DEFAULT NULL"),
    ("burst_ratio", "REAL DEFAULT NULL"),
]:
    try:
        conn.execute(f"ALTER TABLE narrative_snapshots ADD COLUMN {col} {coltype}")
    except Exception:
        pass  # Column already exists
```

Update the snapshot insertion logic in `pipeline.py` (`repository.save_snapshot()`) to include these new fields when available.

### Step 2: Historical query API

```
GET /api/narrative/{id}/history?days=30
  Returns daily snapshots for the narrative over the specified period.
  Response: [
    {
      "date": "2026-03-17",
      "velocity": 0.14,
      "entropy": 1.82,
      "ns_score": 0.46,
      "document_count": 23,
      "lifecycle_stage": "Growing",
      "linked_assets": ["TSM", "NVDA"],
      "burst_ratio": 1.2
    },
    ...
  ]

GET /api/ticker/{symbol}/price-history?days=30
  Returns daily closing prices from yfinance.
  Response: [
    { "date": "2026-03-17", "close": 142.35, "change_pct": 0.87 },
    ...
  ]
  Cached for 1 hour to avoid hitting yfinance rate limits.
```

### Step 3: Price data integration — extend existing `stock_data.py`

**Do NOT create a new `api/price_service.py`.** The file `stock_data.py` already exists in the project root with `import yfinance as yf` and a `get_quote()` method. Extend it with a `get_price_history()` method:

```python
# In stock_data.py — add alongside existing get_quote()
def get_price_history(symbol: str, days: int = 30) -> list[dict]:
    """Returns daily closing prices from yfinance, cached for 1 hour."""
    ...
```

Add in-memory caching (same pattern as `FinnhubService` in `api/finnhub_service.py`):
```python
_price_history_cache: dict[str, tuple[float, list]] = {}  # symbol -> (timestamp, data)
PRICE_HISTORY_CACHE_TTL = 3600  # 1 hour
```

Import and use this in `api/main.py` for the price-history endpoint.

### Step 4: Frontend — history chart on narrative detail page

Add a simple line chart (pure SVG, like VelocitySparkline but larger) to `/narrative/[id]` showing velocity over time alongside the sparkline.

### TESTS

**Backend — `test_f5_api.py`:**
```
F5-U1: GET /api/narrative/{id}/history returns list of daily snapshots
F5-U2: Each snapshot has velocity, entropy, lifecycle_stage fields
F5-U3: GET /api/ticker/TSM/price-history returns price data
F5-U4: Price data has date, close, change_pct fields
F5-U5: Price cache prevents repeated yfinance calls within TTL
```

### GOVERNANCE
- Run all prior tests — no regressions
- Append to `frontend_build_log`

---

## F6 — Correlation Dashboard (Velocity Leads Price?)

You are building phase F6. This phase adds a correlation analysis view that compares narrative velocity curves to price movements for linked assets. This is the quantitative validation layer.

### OBJECTIVE: VELOCITY-PRICE CORRELATION VIEW

**Prerequisite:** F5 must be complete (historical data + price data available).

**IMPORTANT:** This phase requires at minimum **30 days** of pipeline snapshot data to produce statistically meaningful correlations (with 14 data points, even r=0.5 won't be significant). Ideally 60-90 days. If fewer than 30 days exist, the UI should show a "collecting data" state with a progress indicator showing days accumulated vs 30-day minimum.

**Dependency:** `scipy` is NOT in `requirements.txt`. Add it before implementing:
```
scipy>=1.12.0
```
This provides `scipy.stats.pearsonr` for correlation computation. numpy alone does not have a Pearson function with p-value output.

### Step 1: Add `scipy` to `requirements.txt`

Add `scipy>=1.12.0` to `requirements.txt` and run `pip install scipy`.

### Step 2: Correlation computation in `api/correlation_service.py`

```python
def compute_velocity_price_correlation(
    velocity_history: list[dict],   # from narrative snapshots
    price_history: list[dict],      # from yfinance
    lead_days: int = 1,             # how many days velocity leads price
) -> dict:
    """
    Computes Pearson correlation between velocity changes (day N)
    and price changes (day N + lead_days).

    Returns:
    {
        "correlation": float,       # Pearson r (-1 to 1)
        "p_value": float,           # statistical significance
        "n_observations": int,      # number of data points used
        "is_significant": bool,     # p_value < 0.05
        "lead_days": int,
        "interpretation": str,      # human-readable summary
    }
    """
```

### Step 3: API endpoint

```
GET /api/correlation/{narrative_id}/{ticker}?lead_days=1
  Returns correlation analysis between narrative velocity and ticker price.
```

### Step 4: Frontend — `/correlation` page

Create a correlation dashboard page:
- For each narrative-asset pair, show:
  - Narrative name + ticker symbol
  - Correlation coefficient with color coding (|r| > 0.3 = interesting)
  - Significance indicator (p < 0.05 = green check, else gray)
  - Dual-axis chart: velocity (left axis) vs price change (right axis)
  - Lead time selector (1, 2, 3, 5 days)
- "Collecting data" state when < 30 days of snapshots exist, showing "{N}/30 days collected"

### TESTS

**Backend — `test_f6_api.py`:**
```
F6-U1: compute_velocity_price_correlation returns valid correlation dict
F6-U2: Correlation is between -1 and 1
F6-U3: Returns is_significant=false when n_observations < 30
F6-U4: GET /api/correlation/{id}/{ticker} returns 200
F6-U5: Interpretation string matches correlation magnitude
```

### GOVERNANCE
- Run all prior tests — no regressions
- Append to `frontend_build_log`
- Update `COMPLETE` line with new totals

---

## Post-F6 Notes

**Data collection timeline:** The correlation analysis (F6) becomes meaningful only after 60-90 days of regular pipeline runs. F1–F5 should be implemented immediately; F6 can be deployed but will show "collecting data" until enough history accumulates.

**Pipeline frequency recommendation:** Start at 4-hour intervals after F2. Monitor Anthropic API costs for one week. If Haiku costs are acceptable, move to 2-hour intervals. The `SONNET_DAILY_TOKEN_BUDGET` setting already prevents runaway Sonnet costs.

**Competitive intelligence (future):** Not included in F-phase. This requires custom feed lists per industry vertical — better handled as a separate configuration initiative, not a code phase.
