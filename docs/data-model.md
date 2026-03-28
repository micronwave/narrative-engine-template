# Narrative Intelligence Platform — Data Model

**Version:** C2
**Date:** 2026-03-16

All API endpoints return payloads matching these shapes. DB field mappings noted where they differ from API names.

---

## Narrative

Full object returned by `GET /api/narrative/{id}` and the 3 visible items in `GET /api/narratives`.

```json
{
  "id": "nar-001",
  "name": "Semiconductor Reshoring Acceleration",
  "descriptor": "US chip manufacturing policy is catalyzing a supply-chain realignment across East Asia.",
  "velocity_summary": "+14.0% signal velocity over 7d",
  "entropy": 0.72,
  "saturation": 0.45,
  "velocity_timeseries": [
    { "date": "2026-03-10", "value": 0.58 },
    { "date": "2026-03-11", "value": 0.62 },
    { "date": "2026-03-12", "value": 0.67 },
    { "date": "2026-03-13", "value": 0.71 },
    { "date": "2026-03-14", "value": 0.74 },
    { "date": "2026-03-15", "value": 0.80 },
    { "date": "2026-03-16", "value": 0.86 }
  ],
  "signals": ["sig-001", "sig-002"],
  "catalysts": ["cat-001"],
  "mutations": ["mut-001", "mut-002"],
  "blurred": false
}
```

**DB field mappings (`narratives` table):**
- `id` ← `narrative_id` (PRIMARY KEY)
- `saturation` ← `cohesion` (mean pairwise embedding similarity; no dedicated saturation metric)
- `velocity_timeseries` ← `narrative_snapshots` table (`snapshot_date`, `velocity`); padded synthetically if <7 days
- `signals` ← IDs from `document_evidence` table (full objects on detail endpoint)
- `catalysts` ← IDs from `mutation_events` with `mutation_type IN ('stage_change','score_spike')`
- `mutations` ← IDs from `mutation_events` (all types)

**Blurred narrative (list endpoint only):**
```json
{ "id": "nar-004", "blurred": true }
```

---

## Signal / Evidence

```json
{
  "id": "sig-001",
  "narrative_id": "nar-001",
  "headline": "TSMC Arizona Fab 2 timeline moved up by 6 months",
  "source": {
    "id": "src-001",
    "name": "Reuters",
    "type": "news",
    "url": "https://example.com/article",
    "credibility_score": 0.85
  },
  "timestamp": "2026-03-15T14:22:00Z",
  "sentiment": 0.5,
  "coordination_flag": false
}
```

**DB field mappings (`document_evidence` table):**
- `id` ← `doc_id`
- `headline` ← `excerpt` (first 150 chars; no dedicated headline field)
- `source.id` ← `source_domain` slug
- `source.name` ← `source_domain`
- `source.url` ← `source_url`
- `source.credibility_score` ← 0.85 default (not stored in DB)
- `timestamp` ← `published_at`
- `sentiment` ← 0.5 default (per-document sentiment not computed by pipeline)
- `coordination_flag` ← `false` default (narrative-level `is_coordinated` not per-signal)

---

## Source

```json
{
  "id": "src-001",
  "name": "Reuters",
  "type": "news",
  "url": "https://example.com/article",
  "credibility_score": 0.85
}
```

---

## Catalyst

```json
{
  "id": "cat-001",
  "narrative_id": "nar-001",
  "description": "CHIPS Act Phase 2 funding announcement",
  "timestamp": "2026-03-14T09:00:00Z",
  "impact_score": 0.85
}
```

**DB field mappings (`mutation_events` table, types: stage_change, score_spike):**
- `id` ← `id`
- `narrative_id` ← `narrative_id`
- `description` ← `haiku_explanation` (if present) or formatted `mutation_type + previous_value → new_value`
- `timestamp` ← `detected_at`
- `impact_score` ← `magnitude` (clamped to 0.0–1.0)

---

## Mutation

```json
{
  "id": "mut-001",
  "narrative_id": "nar-001",
  "from_state": "Policy proposal",
  "to_state": "Active implementation",
  "timestamp": "2026-03-12T00:00:00Z",
  "trigger": "mut-001",
  "description": "Narrative shifted from speculative policy discussion to confirmed funding allocation."
}
```

**DB field mappings (`mutation_events` table):**
- `from_state` ← `previous_value`
- `to_state` ← `new_value`
- `timestamp` ← `detected_at`
- `trigger` ← self-referential `id` (no separate catalyst FK in DB)
- `description` ← `haiku_explanation` or formatted type string

---

## EntropyScore

```json
{
  "narrative_id": "nar-001",
  "score": 0.72,
  "components": {
    "source_diversity": 0.65,
    "temporal_spread": 0.80,
    "sentiment_variance": 0.70
  }
}
```

**DB field mappings (synthesized from `narratives` table):**
- `score` ← `entropy`
- `source_diversity` ← `cross_source_score`
- `temporal_spread` ← computed from `document_evidence` date range / 7-day window (normalized 0–1)
- `sentiment_variance` ← `polarization`

---

## InvestigationCredit

```json
{
  "user_id": "user-001",
  "balance": 5,
  "total_purchased": 20,
  "total_used": 15
}
```

**Storage:** In-memory only (resets on server restart). C4 will persist to DB.

---

## ConstellationGraph

Returned by `GET /api/constellation`.

```json
{
  "nodes": [
    { "id": "nar-001", "name": "Semiconductor Reshoring", "type": "narrative", "entropy": 0.72 },
    { "id": "nar-002", "name": "AI Chip Export Controls", "type": "narrative", "entropy": 0.58 },
    { "id": "cat-001", "name": "CHIPS Act Phase 2", "type": "catalyst" }
  ],
  "edges": [
    { "source": "cat-001", "target": "nar-001", "weight": 0.85, "label": "triggered" },
    { "source": "nar-001", "target": "nar-002", "weight": 0.60, "label": "related" }
  ]
}
```

**Generation logic:**
- Narrative nodes: all active non-suppressed narratives
- Catalyst nodes: narratives with `is_catalyst = 1` prefixed as `"cat-{narrative_id}"`
- Related edges: narrative pairs sharing ≥2 `linked_assets` tickers
- Triggered edges: catalyst→narrative from high-magnitude `mutation_events`
