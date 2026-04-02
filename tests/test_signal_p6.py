"""
Signal Redesign Phase 6 — Directional Asset Impact Scoring Tests

Section 1: compute_directional_impact (4 tests)
  SP6-CDI-1: all None optional inputs -> neutral direction, impact_score=0, valid structure
  SP6-CDI-2: bearish signal + high convergence -> direction="bearish", elevated impact_score
  SP6-CDI-3: convergence agreement boosts confidence
  SP6-CDI-4: time_horizon maps correctly: "immediate"->"1-3d", "near_term"->"1-2w", "long_term"->"1-3m"

Section 2: enrich_linked_assets (3 tests)
  SP6-ELA-1: preserves original ticker, asset_name, similarity_score keys
  SP6-ELA-2: adds direction, impact_score, confidence, time_horizon, signal_components
  SP6-ELA-3: returns list sorted by impact_score DESC

Section 3: Schema (3 tests)
  SP6-SCH-1: impact_scores table created by migrate()
  SP6-SCH-2: upsert_impact_score inserts with UNIQUE(narrative_id, ticker)
  SP6-SCH-3: get_impact_scores_for_ticker returns correct rows

Section 4: Repository queries (1 test)
  SP6-REPO-1: get_top_impact_scores(limit=5) returns sorted by impact_score DESC

Section 5: Pipeline integration (2 tests)
  SP6-INT-1: Pipeline Step 19 completes with enriched linked_assets (direction field in stored JSON)
  SP6-INT-2: Existing tests that parse linked_assets still pass (backward-compatible superset)
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from impact_scorer import compute_directional_impact, enrich_linked_assets
from repository import SqliteRepository

# ---------------------------------------------------------------------------
# Test runner helpers
# ---------------------------------------------------------------------------
_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    _results.append((name, condition))
    marker = "\u2713" if condition else "\u2717"
    msg = f"  [{marker}] {name}"
    if details and not condition:
        msg += f"\n      details: {details}"
    elif details and condition:
        msg += f"  ({details})"
    print(msg)


def _make_repo() -> SqliteRepository:
    tmp = tempfile.mktemp(suffix=".db")
    repo = SqliteRepository(tmp)
    repo.migrate()
    return repo


# ---------------------------------------------------------------------------
# Section 1: compute_directional_impact
# ---------------------------------------------------------------------------
S("Section 1: compute_directional_impact")

# SP6-CDI-1: all None optional inputs -> neutral, impact_score=0, valid structure
result = compute_directional_impact(
    narrative_id="n-test-1",
    ticker="AAPL",
    similarity_score=0.75,
    signal=None,
    convergence=None,
    escalation={},
    catalyst={},
    asset_name="Apple Inc",
)
has_required_keys = all(
    k in result
    for k in ["ticker", "asset_name", "similarity_score", "direction",
              "impact_score", "confidence", "time_horizon", "signal_components"]
)
T("SP6-CDI-1: None inputs -> neutral, low impact, valid structure",
  has_required_keys
  and result["direction"] == "neutral"
  and result["impact_score"] < 0.05
  and result["ticker"] == "AAPL"
  and result["asset_name"] == "Apple Inc"
  and result["similarity_score"] == 0.75,
  f"direction={result['direction']}, impact={result['impact_score']}, keys={list(result.keys())}")

# SP6-CDI-2: bearish signal + high convergence -> direction="bearish", elevated impact
result2 = compute_directional_impact(
    narrative_id="n-test-2",
    ticker="TSLA",
    similarity_score=0.80,
    signal={
        "direction": "bearish",
        "confidence": 0.8,
        "certainty": "confirmed",
        "magnitude": "significant",
        "timeframe": "near_term",
    },
    convergence={
        "convergence_count": 3,
        "direction_consensus": 0.9,
        "pressure_score": 2.5,
    },
    escalation={"source_highest_tier": 1, "source_escalation_velocity": 1.5},
    catalyst={"proximity_score": 0.8, "catalyst_type": "earnings", "macro_alignment": 0.5},
)
T("SP6-CDI-2: bearish + convergence -> bearish, elevated impact",
  result2["direction"] == "bearish" and result2["impact_score"] > 0.3,
  f"direction={result2['direction']}, impact={result2['impact_score']:.4f}, conf={result2['confidence']:.4f}")

# SP6-CDI-3: convergence agreement boosts confidence
result_no_conv = compute_directional_impact(
    narrative_id="n-test-3a",
    ticker="NVDA",
    similarity_score=0.70,
    signal={"direction": "bullish", "confidence": 0.6, "certainty": "expected", "magnitude": "significant", "timeframe": "near_term"},
    convergence=None,
    escalation={},
    catalyst={},
)
result_with_conv = compute_directional_impact(
    narrative_id="n-test-3b",
    ticker="NVDA",
    similarity_score=0.70,
    signal={"direction": "bullish", "confidence": 0.6, "certainty": "expected", "magnitude": "significant", "timeframe": "near_term"},
    convergence={"convergence_count": 4, "direction_consensus": 0.95, "pressure_score": 3.0},
    escalation={},
    catalyst={},
)
T("SP6-CDI-3: convergence boosts confidence",
  result_with_conv["confidence"] > result_no_conv["confidence"],
  f"without={result_no_conv['confidence']:.4f}, with={result_with_conv['confidence']:.4f}")

# SP6-CDI-4: time_horizon mapping
for tf, expected_horizon in [("immediate", "1-3d"), ("near_term", "1-2w"), ("long_term", "1-3m")]:
    r = compute_directional_impact(
        narrative_id="n-test-4",
        ticker="SPY",
        similarity_score=0.60,
        signal={"direction": "bullish", "confidence": 0.5, "certainty": "expected",
                "magnitude": "incremental", "timeframe": tf},
        convergence=None,
        escalation={},
        catalyst={},
    )
    T(f"SP6-CDI-4: {tf} -> {expected_horizon}",
      r["time_horizon"] == expected_horizon,
      f"got={r['time_horizon']}")


# ---------------------------------------------------------------------------
# Section 2: enrich_linked_assets
# ---------------------------------------------------------------------------
S("Section 2: enrich_linked_assets")

# Set up mock repo for enrich tests
mock_repo = MagicMock()
mock_repo.get_narrative_signal.return_value = {
    "direction": "bullish",
    "confidence": 0.7,
    "certainty": "confirmed",
    "magnitude": "significant",
    "timeframe": "near_term",
}
mock_repo.get_narrative.return_value = {
    "narrative_id": "n-enrich",
    "source_highest_tier": 2,
    "source_escalation_velocity": 1.0,
    "catalyst_proximity_score": 0.5,
    "catalyst_type": "earnings",
    "days_to_catalyst": 5,
    "macro_alignment": 0.3,
}
mock_repo.get_ticker_convergence.return_value = {
    "convergence_count": 2,
    "direction_consensus": 0.8,
    "pressure_score": 1.5,
}

raw_assets = [
    {"ticker": "AAPL", "asset_name": "Apple Inc", "similarity_score": 0.85},
    {"ticker": "MSFT", "asset_name": "Microsoft Corp", "similarity_score": 0.72},
    {"ticker": "NVDA", "asset_name": "NVIDIA Corp", "similarity_score": 0.68},
]

enriched = enrich_linked_assets("n-enrich", raw_assets, mock_repo)

# SP6-ELA-1: preserves original keys
T("SP6-ELA-1: preserves ticker, asset_name, similarity_score",
  all(
      "ticker" in a and "asset_name" in a and "similarity_score" in a
      for a in enriched
  ) and len(enriched) == 3,
  f"count={len(enriched)}, keys={list(enriched[0].keys()) if enriched else []}")

# SP6-ELA-2: adds new keys
new_keys = {"direction", "impact_score", "confidence", "time_horizon", "signal_components"}
T("SP6-ELA-2: adds direction, impact_score, confidence, time_horizon, signal_components",
  all(new_keys.issubset(set(a.keys())) for a in enriched),
  f"first item keys={list(enriched[0].keys()) if enriched else []}")

# SP6-ELA-3: sorted by impact_score DESC
scores = [a["impact_score"] for a in enriched]
T("SP6-ELA-3: sorted by impact_score DESC",
  all(scores[i] >= scores[i + 1] for i in range(len(scores) - 1)),
  f"scores={scores}")


# ---------------------------------------------------------------------------
# Section 3: Schema
# ---------------------------------------------------------------------------
S("Section 3: Schema")

# SP6-SCH-1: impact_scores table exists after migrate()
repo = _make_repo()
try:
    import sqlite3
    conn = sqlite3.connect(repo._db_path)
    conn.row_factory = sqlite3.Row
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    conn.close()
    T("SP6-SCH-1: impact_scores table exists",
      "impact_scores" in tables,
      f"tables={tables}")
except Exception as e:
    T("SP6-SCH-1: impact_scores table exists", False, str(e))

# SP6-SCH-2: upsert_impact_score with UNIQUE constraint
repo.upsert_impact_score({
    "narrative_id": "n-schema-1",
    "ticker": "AAPL",
    "direction": "bullish",
    "impact_score": 0.65,
    "confidence": 0.70,
    "time_horizon": "1-2w",
    "signal_components": {"llm_direction": "bullish"},
})
repo.upsert_impact_score({
    "narrative_id": "n-schema-1",
    "ticker": "AAPL",
    "direction": "bearish",
    "impact_score": 0.80,
    "confidence": 0.85,
    "time_horizon": "1-3d",
    "signal_components": {"llm_direction": "bearish"},
})
rows = repo.get_impact_scores_for_narrative("n-schema-1")
T("SP6-SCH-2: upsert updates on UNIQUE(narrative_id, ticker)",
  len(rows) == 1 and rows[0]["direction"] == "bearish" and rows[0]["impact_score"] == 0.80,
  f"count={len(rows)}, dir={rows[0]['direction'] if rows else 'N/A'}, score={rows[0]['impact_score'] if rows else 'N/A'}")

# SP6-SCH-3: get_impact_scores_for_ticker
repo.upsert_impact_score({
    "narrative_id": "n-schema-2",
    "ticker": "AAPL",
    "direction": "bullish",
    "impact_score": 0.50,
    "confidence": 0.55,
    "time_horizon": "1-3m",
    "signal_components": {},
})
rows_ticker = repo.get_impact_scores_for_ticker("AAPL")
T("SP6-SCH-3: get_impact_scores_for_ticker returns correct rows",
  len(rows_ticker) == 2,
  f"count={len(rows_ticker)}")


# ---------------------------------------------------------------------------
# Section 4: Repository queries
# ---------------------------------------------------------------------------
S("Section 4: Repository queries")

# SP6-REPO-1: get_top_impact_scores sorted DESC
repo2 = _make_repo()
for i, score in enumerate([0.3, 0.9, 0.1, 0.7, 0.5, 0.2]):
    repo2.upsert_impact_score({
        "narrative_id": f"n-top-{i}",
        "ticker": f"T{i}",
        "direction": "bullish",
        "impact_score": score,
        "confidence": 0.5,
        "time_horizon": "1-2w",
        "signal_components": {},
    })
top = repo2.get_top_impact_scores(limit=5)
top_scores = [r["impact_score"] for r in top]
T("SP6-REPO-1: get_top_impact_scores(5) sorted DESC, limit=5",
  len(top) == 5
  and all(top_scores[i] >= top_scores[i + 1] for i in range(len(top_scores) - 1))
  and top_scores[0] == 0.9,
  f"scores={top_scores}")


# ---------------------------------------------------------------------------
# Section 5: Pipeline integration
# ---------------------------------------------------------------------------
S("Section 5: Pipeline integration")

# SP6-INT-1: Step 19 produces enriched linked_assets with direction field
repo3 = _make_repo()
# Insert a narrative to work with
from datetime import datetime, timezone
repo3.insert_narrative({
    "narrative_id": "n-pipe-1",
    "name": "Test Narrative",
    "stage": "Growing",
    "created_at": datetime.now(timezone.utc).isoformat(),
    "last_updated_at": datetime.now(timezone.utc).isoformat(),
    "document_count": 10,
    "ns_score": 0.5,
    "linked_assets": json.dumps([
        {"ticker": "AAPL", "asset_name": "Apple Inc", "similarity_score": 0.85},
    ]),
})
# Signal for this narrative
repo3.upsert_narrative_signal({
    "narrative_id": "n-pipe-1",
    "direction": "bullish",
    "confidence": 0.7,
    "timeframe": "near_term",
    "magnitude": "significant",
    "certainty": "expected",
    "key_actors": "[]",
    "affected_sectors": "[]",
    "catalyst_type": "earnings",
})

# Run enrichment
raw = [{"ticker": "AAPL", "asset_name": "Apple Inc", "similarity_score": 0.85}]
enriched_pipe = enrich_linked_assets("n-pipe-1", raw, repo3)
has_direction = (
    len(enriched_pipe) == 1
    and "direction" in enriched_pipe[0]
    and enriched_pipe[0]["direction"] == "bullish"
)
T("SP6-INT-1: enriched linked_assets has direction field",
  has_direction,
  f"enriched={enriched_pipe[0] if enriched_pipe else '{}'}")

# SP6-INT-2: backward-compatible — original keys preserved
original_keys = {"ticker", "asset_name", "similarity_score"}
T("SP6-INT-2: backward-compatible superset (original keys present)",
  len(enriched_pipe) == 1
  and original_keys.issubset(set(enriched_pipe[0].keys()))
  and enriched_pipe[0]["ticker"] == "AAPL"
  and enriched_pipe[0]["asset_name"] == "Apple Inc"
  and enriched_pipe[0]["similarity_score"] == 0.85,
  f"keys={list(enriched_pipe[0].keys()) if enriched_pipe else []}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Phase 6 — Directional Impact: {passed}/{total} passed")
if passed < total:
    print("FAILED:")
    for name, ok in _results:
        if not ok:
            print(f"  - {name}")
    sys.exit(1)
else:
    print("ALL PASSED")
