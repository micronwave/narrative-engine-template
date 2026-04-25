# F-Phase Completion Prompt — Wire Up Remaining Gaps

## Context

F1–F6 backend functions, API endpoints, types, and some frontend components were built, but four phases have gaps where the pipeline doesn't call the functions, or the frontend doesn't consume the API data. This prompt completes the wiring.

**What's already done (DO NOT rebuild):**
- F1: Fully working (lifecycle stages + badges)
- F2: `compute_burst_velocity()` in signals.py, SURGE indicator in NarrativeCard, `burst_velocity` field in API (returns null)
- F3: Fully working (brief pages + API)
- F4: `topic_tags` column in DB, `update_narrative_tags()` method, API returns `topic_tags: []`, topic filter dropdown on gateway, tag pills on NarrativeCard
- F5: `GET /api/narrative/{id}/history`, `GET /api/ticker/{symbol}/price-history`, `fetchNarrativeHistory()` and `fetchPriceHistory()` in api.ts
- F6: `GET /api/correlation/{narrative_id}/{ticker}`, `compute_velocity_price_correlation()` in correlation_service.py

**What's missing (this prompt fixes):**

---

## FIX 1 — F2: Integrate `compute_burst_velocity` into Pipeline

### Problem
`compute_burst_velocity()` exists in `signals.py` but is never called in `pipeline.py`. The API always returns `burst_velocity: null`.

### Fix
In `pipeline.py`, inside the step 10 scoring loop (where `compute_lifecycle_stage` is already called), ADD burst velocity computation after the lifecycle stage block:

```python
# F2: Burst velocity — doc ingestion rate acceleration
try:
    baseline = repository.get_baseline_doc_rate(narrative_id, lookback_days=7)
    burst = compute_burst_velocity(
        recent_doc_count=new_assignment_count,
        baseline_docs_per_window=baseline,
        alert_ratio=settings.BURST_VELOCITY_ALERT_RATIO,
    )
    if burst["is_burst"]:
        logger.warning("[BURST] Narrative '%s' — %.1fx normal rate",
                       narrative.get("name", narrative_id), burst["ratio"])
except Exception as exc:
    logger.debug("Burst velocity skipped for %s: %s", narrative_id, exc)
    burst = {"rate": 0, "baseline": 0, "ratio": 0, "is_burst": False}
```

Also, import `compute_burst_velocity` — it should already be available since `signals.py` was updated, but verify the import at the top of `pipeline.py`.

Then update `_build_visible_narrative()` in `api/main.py` to read burst data from the narrative. Since the burst result isn't stored on the narrative dict directly, you need to either:
- **Option A (simple):** Store `burst_ratio` on the narrative via `repository.update_narrative(narrative_id, {"burst_ratio": burst["ratio"]})` in the pipeline, then read it in the API
- **Option B (better):** Store the full burst dict as JSON in a new `burst_velocity_json TEXT` column

Recommended: Option A — add `burst_ratio` column to narratives table (idempotent ALTER TABLE), store the ratio in the pipeline, then in `_build_visible_narrative()`:
```python
burst_ratio = float(n.get("burst_ratio") or 0)
"burst_velocity": {
    "rate": 0, "baseline": 0,
    "ratio": burst_ratio,
    "is_burst": burst_ratio >= 3.0,
} if burst_ratio > 0 else None,
```

### Files to modify
- `pipeline.py` — add burst computation in step 10 loop
- `repository.py` — add `burst_ratio REAL DEFAULT NULL` to narratives table (idempotent ALTER TABLE)
- `api/main.py` — update `_build_visible_narrative()` to read burst_ratio from narrative

### Verification
- Run pipeline: `python pipeline.py`
- Check: `curl http://localhost:8000/api/narratives | python -c "import sys,json; d=json.load(sys.stdin); print([(n['name'][:30], n['burst_velocity']) for n in d[:3]])"`
- Burst_velocity should no longer be null

---

## FIX 2 — F4: Add Topic Classification to Pipeline Stage 8

### Problem
`topic_tags` column exists, `update_narrative_tags()` method exists, but the pipeline never calls the LLM to classify topics. All `topic_tags` are empty `[]`.

### Fix
In `pipeline.py`, find stage 8 (LLM labeling) — search for `call_haiku("label_narrative"`. After the existing Haiku labeling call that sets the narrative name, add topic classification for narratives that don't have tags yet:

```python
# F4: Topic classification (only for new/untagged narratives)
existing_tags_json = narrative.get("topic_tags")
if existing_tags_json is None or existing_tags_json == "null":
    topic_prompt = (
        f'Classify this narrative about "{raw_label}" into 1-3 topic tags from this list:\n'
        "regulatory, earnings, geopolitical, macro, esg, m&a, crypto\n"
        "Return only the tag names, comma-separated. Example: regulatory, macro"
    )
    try:
        raw_tags = llm_client.call_haiku("classify_topic", narrative_id, topic_prompt)
        tags = [t.strip().lower() for t in raw_tags.split(",") if t.strip()]
        # Filter to valid tags only
        valid_tags = {"regulatory", "earnings", "geopolitical", "macro", "esg", "m&a", "crypto"}
        tags = [t for t in tags if t in valid_tags]
        if tags:
            repository.update_narrative_tags(narrative_id, tags)
    except Exception as exc:
        logger.debug("Topic classification failed for %s: %s", narrative_id, exc)
```

Also add `"classify_topic"` to the task_type fallback map in `llm_client.py` if needed:
```python
# In LlmClient, add to the _FALLBACKS or similar:
"classify_topic": ""
```

### Files to modify
- `pipeline.py` — add topic classification after Haiku labeling
- `llm_client.py` — add "classify_topic" task type to fallback map

### Verification
- Run pipeline: `python pipeline.py`
- Check: `curl http://localhost:8000/api/narratives | python -c "import sys,json; d=json.load(sys.stdin); print([(n['name'][:30], n['topic_tags']) for n in d[:5]])"`
- Topics should be populated (e.g., `["geopolitical", "macro"]`)
- Gateway topic filter dropdown should now filter narratives

---

## FIX 3 — F5: Build Frontend History Views

### Problem
`fetchNarrativeHistory()` and `fetchPriceHistory()` exist in api.ts but are never called by any page or component.

### Fix
Add a "History" section to the narrative detail page (`frontend/src/app/narrative/[id]/page.tsx`). This section shows:

1. **Velocity history chart** — a larger version of VelocitySparkline showing the last 30 days of velocity data from `GET /api/narrative/{id}/history`
2. **Price history** for linked assets — if the narrative has linked securities, show their price history from `GET /api/ticker/{symbol}/price-history`

**Implementation:**
- Create a new component `frontend/src/components/HistoryChart.tsx` — a pure SVG line chart (same pattern as VelocitySparkline but wider, taller, with date labels on X-axis and value labels on Y-axis)
- In `narrative/[id]/page.tsx`, add a new section after the existing "Momentum Trend" section:

```tsx
{/* Historical Velocity (F5) */}
<section className="mb-8">
  <h2 className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2 mb-3">
    Velocity History (30d)
  </h2>
  <HistoryChart data={velocityHistory} dataKey="velocity" color="var(--bullish)" />
</section>
```

- Fetch `fetchNarrativeHistory(id, 30)` in the page's useEffect alongside existing data fetches
- Use the `date` and `velocity` fields from the snapshot response to build the chart

**For price history:** Add it to the brief page (`/brief/[ticker]`) rather than the narrative detail page, since it's ticker-specific:
- Fetch `fetchPriceHistory(ticker, 30)` in the brief page
- Add a price chart section below the risk summary

### Files to create
- `frontend/src/components/HistoryChart.tsx` — reusable SVG line chart

### Files to modify
- `frontend/src/app/narrative/[id]/page.tsx` — add velocity history section
- `frontend/src/app/brief/[ticker]/page.tsx` — add price history chart

### Verification
- Visit `/narrative/{id}` — should see 30-day velocity chart
- Visit `/brief/TSM` — should see price chart (if yfinance data available)

---

## FIX 4 — F6: Build Correlation Frontend Page

### Problem
`GET /api/correlation/{narrative_id}/{ticker}` exists but there's no frontend page, no `fetchCorrelation()` function, and no navigation to it.

### Fix

**Step 1: Add `fetchCorrelation()` to api.ts:**
```typescript
export type CorrelationResult = {
  correlation: number;
  p_value: number;
  n_observations: number;
  is_significant: boolean;
  lead_days: number;
  interpretation: string;
  narrative_id: string;
  ticker: string;
};

export async function fetchCorrelation(
  narrativeId: string,
  ticker: string,
  leadDays: number = 1
): Promise<CorrelationResult> {
  const res = await fetch(`/api/correlation/${narrativeId}/${ticker}?lead_days=${leadDays}`);
  if (!res.ok) throw new Error(`correlation fetch failed: ${res.status}`);
  return res.json();
}
```

**Step 2: Create `/correlation` page:**
Create `frontend/src/app/correlation/page.tsx`:
- Fetches all narratives via `fetchNarratives()`
- For each narrative that has linked assets (via the brief endpoint or known securities), compute correlation
- Display a table/grid of narrative-ticker pairs with:
  - Narrative name + ticker symbol
  - Correlation coefficient (color-coded: |r| > 0.3 green, |r| > 0.5 amber, |r| > 0.7 red)
  - p-value and significance indicator
  - Lead time selector (1, 2, 3, 5 days)
  - Interpretation text
- **"Collecting data" state:** If `n_observations < 30`, show progress indicator: "{n}/30 days"
- Use existing E-phase design tokens (bg-surface, text-accent-text, etc.)

**Step 3: Add to NavBar or Brief page:**
- Option A: Add "Correlation" as a link on the `/brief/[ticker]` page (per-ticker correlation)
- Option B: Add a link on the `/stocks` page filter bar: "View Correlations"
- Do NOT add a 6th NavBar item

### Files to create
- `frontend/src/app/correlation/page.tsx`

### Files to modify
- `frontend/src/lib/api.ts` — add CorrelationResult type + fetchCorrelation()
- `frontend/src/app/brief/[ticker]/page.tsx` — add "View Correlation" link for each narrative-ticker pair
- `frontend/src/app/stocks/page.tsx` — add "Correlations" link in filter bar area

### Tests
```
Frontend tests (add to f6.test.tsx or new file):
- Correlation page renders heading
- Collecting data state shows when n_observations < 30
- Correlation table renders rows with coefficient values
```

### Verification
- Visit `/correlation` — should show narrative-ticker pairs
- Most will show "Collecting data" since pipeline hasn't been running long enough
- Click lead time selector — should update correlation values

---

## Execution Order

1. **FIX 1 (F2 pipeline integration)** — run pipeline once after to populate burst data
2. **FIX 2 (F4 topic classification)** — run pipeline once after to populate topics
3. **FIX 3 (F5 history views)** — frontend only, no pipeline needed
4. **FIX 4 (F6 correlation page)** — frontend only, no pipeline needed

After fixes 1+2, run `python pipeline.py` once to populate burst velocity and topic tags.

## Testing
After each fix:
- `cd frontend && npx jest --watchAll=false` — all tests pass
- `npx tsc --noEmit` — 0 errors
- `python -X utf8 test_f{n}_api.py` — all pass
- Update `BUILD_LOG.md` with what was completed
