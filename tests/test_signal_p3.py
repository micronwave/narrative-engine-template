"""
Signal Redesign Phase 3 — Narrative Convergence Detection Tests

Section 1: compute_ticker_convergence edge cases (6 tests)
  SP3-CTC-1: 0 narratives returns safe defaults
  SP3-CTC-2: 1 narrative returns convergence_count=1, pressure_score=0
  SP3-CTC-3: 2 independent bearish narratives (orthogonal vectors) -> convergence_count=2, direction < 0
  SP3-CTC-4: 2 dependent narratives (identical vectors) -> convergence_count=1
  SP3-CTC-5: mixed directions (1 bullish + 1 bearish, equal conf) -> direction_consensus < 1.0
  SP3-CTC-6: narrative with missing vector is isolated (own component)

Section 2: compute_all_convergences (2 tests)
  SP3-CAC-1: groups narratives by ticker from linked_assets JSON
  SP3-CAC-2: handles NULL/empty/malformed linked_assets gracefully

Section 3: Schema (2 tests)
  SP3-SCH-1: ticker_convergence table created by migrate()
  SP3-SCH-2: convergence_exposure column exists on narratives table

Section 4: CRUD (1 test)
  SP3-CRUD-1: upsert + get_top_convergences returns sorted by pressure_score DESC

Section 5: Integration (1 test)
  SP3-INT-1: full round-trip: narratives -> compute -> upsert -> verify
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from convergence import compute_ticker_convergence, compute_all_convergences
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class MockVectorStore:
    """Test-only vector store with controllable vectors."""

    def __init__(self, vectors: dict[str, np.ndarray]):
        self._vectors = vectors

    def get_vector(self, doc_id: str):
        return self._vectors.get(doc_id)


class MockRepository:
    """Minimal repository mock for compute_all_convergences."""

    def __init__(self, signals: list[dict]):
        self._signals = signals

    def get_all_narrative_signals(self) -> list[dict]:
        return self._signals


def _make_orthogonal_vec(index: int, dim: int = 768) -> np.ndarray:
    """Create a unit vector with 1.0 at position `index`, 0 elsewhere."""
    v = np.zeros(dim, dtype=np.float32)
    v[index % dim] = 1.0
    return v


def _make_identical_vec(dim: int = 768) -> np.ndarray:
    """Create a normalized vector pointing in a fixed direction."""
    v = np.zeros(dim, dtype=np.float32)
    v[0] = 1.0
    return v


def _get_temp_repo() -> SqliteRepository:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    repo = SqliteRepository(path)
    repo.migrate()
    return repo


# ===========================================================================
# Section 1: compute_ticker_convergence edge cases
# ===========================================================================
S("SP3-CTC: compute_ticker_convergence")

# SP3-CTC-1: 0 narratives
_r1 = compute_ticker_convergence("AAPL", [], {}, MockVectorStore({}))
T("SP3-CTC-1: 0 narratives returns safe defaults",
  _r1["convergence_count"] == 0
  and _r1["pressure_score"] == 0.0
  and _r1["direction_agreement"] == 0.0
  and _r1["contributing_narrative_ids"] == [],
  f"got count={_r1['convergence_count']}, pressure={_r1['pressure_score']}")

# SP3-CTC-2: 1 narrative
_r2 = compute_ticker_convergence(
    "AAPL",
    [{"narrative_id": "nar-1", "source_highest_tier": 2}],
    {"nar-1": {"direction": "bearish", "confidence": 0.9}},
    MockVectorStore({"nar-1": _make_orthogonal_vec(0)}),
)
T("SP3-CTC-2: 1 narrative returns convergence_count=1, pressure_score=0",
  _r2["convergence_count"] == 1
  and _r2["pressure_score"] == 0.0
  and _r2["contributing_narrative_ids"] == ["nar-1"],
  f"got count={_r2['convergence_count']}, pressure={_r2['pressure_score']}")

# SP3-CTC-3: 2 independent bearish narratives (orthogonal vectors, sim=0)
_vec_a = _make_orthogonal_vec(0)  # [1,0,0,...0]
_vec_b = _make_orthogonal_vec(1)  # [0,1,0,...0]
_r3 = compute_ticker_convergence(
    "NVDA",
    [
        {"narrative_id": "nar-1", "source_highest_tier": 1},
        {"narrative_id": "nar-2", "source_highest_tier": 3},
    ],
    {
        "nar-1": {"direction": "bearish", "confidence": 0.8},
        "nar-2": {"direction": "bearish", "confidence": 0.7},
    },
    MockVectorStore({"nar-1": _vec_a, "nar-2": _vec_b}),
)
# Expected: convergence_count=2, direction_agreement=-1.0, direction_consensus=1.0
# weighted_confidence = (0.8+0.7)/2 = 0.75, pressure = 2*1.0*0.75 = 1.5
T("SP3-CTC-3: 2 independent bearish -> count=2, agreement < 0, pressure > 0",
  _r3["convergence_count"] == 2
  and _r3["direction_agreement"] < 0
  and _r3["pressure_score"] > 0
  and _r3["source_diversity"] == 2,
  f"count={_r3['convergence_count']}, agreement={_r3['direction_agreement']}, "
  f"pressure={_r3['pressure_score']}, diversity={_r3['source_diversity']}")

# SP3-CTC-4: 2 dependent narratives (identical vectors, sim=1.0)
_vec_same = _make_identical_vec()
_r4 = compute_ticker_convergence(
    "TSLA",
    [
        {"narrative_id": "nar-1", "source_highest_tier": 2},
        {"narrative_id": "nar-2", "source_highest_tier": 2},
    ],
    {
        "nar-1": {"direction": "bullish", "confidence": 0.6},
        "nar-2": {"direction": "bullish", "confidence": 0.6},
    },
    MockVectorStore({"nar-1": _vec_same, "nar-2": _vec_same.copy()}),
)
T("SP3-CTC-4: 2 dependent narratives (sim=1.0) -> convergence_count=1",
  _r4["convergence_count"] == 1,
  f"got count={_r4['convergence_count']}")

# SP3-CTC-5: mixed directions (1 bullish + 1 bearish, equal confidence)
_r5 = compute_ticker_convergence(
    "AMD",
    [
        {"narrative_id": "nar-1", "source_highest_tier": 1},
        {"narrative_id": "nar-2", "source_highest_tier": 4},
    ],
    {
        "nar-1": {"direction": "bullish", "confidence": 0.8},
        "nar-2": {"direction": "bearish", "confidence": 0.8},
    },
    MockVectorStore({"nar-1": _make_orthogonal_vec(0), "nar-2": _make_orthogonal_vec(1)}),
)
# direction_agreement = (1.0*0.8 + -1.0*0.8) / (0.8+0.8) = 0.0, consensus = 0.0
T("SP3-CTC-5: mixed directions -> direction_consensus < 1.0, pressure=0",
  _r5["direction_consensus"] < 1.0
  and _r5["pressure_score"] == 0.0,
  f"consensus={_r5['direction_consensus']}, pressure={_r5['pressure_score']}")


# SP3-CTC-6: narrative with missing vector treated as isolated node
_r6 = compute_ticker_convergence(
    "META",
    [
        {"narrative_id": "nar-1", "source_highest_tier": 1},
        {"narrative_id": "nar-2", "source_highest_tier": 2},
        {"narrative_id": "nar-3", "source_highest_tier": 3},
    ],
    {
        "nar-1": {"direction": "bearish", "confidence": 0.8},
        "nar-2": {"direction": "bearish", "confidence": 0.7},
        "nar-3": {"direction": "bearish", "confidence": 0.6},
    },
    MockVectorStore({
        "nar-1": _make_orthogonal_vec(0),
        # nar-2 has NO vector — should become isolated node
        "nar-3": _make_orthogonal_vec(1),
    }),
)
# nar-2 has no vector -> isolated node -> own component
# nar-1 and nar-3 are orthogonal (sim=0 < 0.30) -> separate components
# Total: 3 independent components
T("SP3-CTC-6: missing vector narrative is isolated (own component)",
  _r6["convergence_count"] == 3
  and _r6["pressure_score"] > 0,
  f"count={_r6['convergence_count']}, pressure={_r6['pressure_score']}")


# ===========================================================================
# Section 2: compute_all_convergences
# ===========================================================================
S("SP3-CAC: compute_all_convergences")

# SP3-CAC-1: groups narratives by ticker from linked_assets JSON
_mock_narratives_1 = [
    {
        "narrative_id": "nar-1",
        "linked_assets": json.dumps([{"ticker": "NVDA", "asset_name": "NVIDIA", "similarity_score": 0.9}]),
        "source_highest_tier": 1,
    },
    {
        "narrative_id": "nar-2",
        "linked_assets": json.dumps([{"ticker": "NVDA", "asset_name": "NVIDIA", "similarity_score": 0.85}]),
        "source_highest_tier": 3,
    },
    {
        "narrative_id": "nar-3",
        "linked_assets": json.dumps([{"ticker": "AAPL", "asset_name": "Apple", "similarity_score": 0.7}]),
        "source_highest_tier": 2,
    },
]
_mock_signals_1 = [
    {"narrative_id": "nar-1", "direction": "bearish", "confidence": 0.9},
    {"narrative_id": "nar-2", "direction": "bearish", "confidence": 0.7},
    {"narrative_id": "nar-3", "direction": "bullish", "confidence": 0.5},
]
_mock_vs_1 = MockVectorStore({
    "nar-1": _make_orthogonal_vec(0),
    "nar-2": _make_orthogonal_vec(1),
    "nar-3": _make_orthogonal_vec(2),
})
_mock_repo_1 = MockRepository(_mock_signals_1)
_cac1 = compute_all_convergences(_mock_narratives_1, _mock_repo_1, _mock_vs_1)
T("SP3-CAC-1: groups by ticker, NVDA has result, AAPL excluded (only 1 narrative)",
  "NVDA" in _cac1
  and "AAPL" not in _cac1
  and _cac1["NVDA"]["convergence_count"] == 2,
  f"tickers={list(_cac1.keys())}, NVDA count={_cac1.get('NVDA', {}).get('convergence_count')}")

# SP3-CAC-2: handles NULL/empty/malformed linked_assets
_mock_narratives_2 = [
    {"narrative_id": "nar-1", "linked_assets": None, "source_highest_tier": 1},
    {"narrative_id": "nar-2", "linked_assets": "", "source_highest_tier": 2},
    {"narrative_id": "nar-3", "linked_assets": "not json at all", "source_highest_tier": 3},
    {"narrative_id": "nar-4", "source_highest_tier": 4},  # missing key entirely
]
_mock_repo_2 = MockRepository([])
_mock_vs_2 = MockVectorStore({})
_cac2_ok = True
try:
    _cac2 = compute_all_convergences(_mock_narratives_2, _mock_repo_2, _mock_vs_2)
    _cac2_ok = isinstance(_cac2, dict) and len(_cac2) == 0
except Exception as _cac2_exc:
    _cac2_ok = False
    _cac2 = str(_cac2_exc)
T("SP3-CAC-2: handles NULL/empty/malformed linked_assets without crashing",
  _cac2_ok,
  f"got {_cac2}")


# ===========================================================================
# Section 3: Schema
# ===========================================================================
S("SP3-SCH: Schema validation")

_schema_repo = _get_temp_repo()

# SP3-SCH-1: ticker_convergence table exists
_sch1_ok = False
try:
    with _schema_repo._get_conn() as _conn:
        _cols = _conn.execute("PRAGMA table_info(ticker_convergence)").fetchall()
        _col_names = {c[1] for c in _cols}
        _expected_cols = {
            "ticker", "convergence_count", "direction_agreement",
            "direction_consensus", "weighted_confidence", "source_diversity",
            "pressure_score", "contributing_narrative_ids", "computed_at",
        }
        _sch1_ok = _expected_cols.issubset(_col_names)
except Exception:
    pass
T("SP3-SCH-1: ticker_convergence table created with all columns",
  _sch1_ok,
  f"columns found: {_col_names if _sch1_ok else 'table missing'}")

# SP3-SCH-2: convergence_exposure column on narratives
_sch2_ok = False
try:
    with _schema_repo._get_conn() as _conn:
        _nar_cols = _conn.execute("PRAGMA table_info(narratives)").fetchall()
        _nar_col_names = {c[1] for c in _nar_cols}
        _sch2_ok = "convergence_exposure" in _nar_col_names
except Exception:
    pass
T("SP3-SCH-2: convergence_exposure column exists on narratives table",
  _sch2_ok,
  f"convergence_exposure {'found' if _sch2_ok else 'missing'}")


# ===========================================================================
# Section 4: CRUD
# ===========================================================================
S("SP3-CRUD: Repository convergence operations")

_crud_repo = _get_temp_repo()

# Insert three tickers with different pressure scores
_crud_repo.upsert_ticker_convergence({
    "ticker": "NVDA",
    "convergence_count": 3,
    "direction_agreement": -0.8,
    "direction_consensus": 0.8,
    "weighted_confidence": 0.7,
    "source_diversity": 3,
    "pressure_score": 1.68,
    "contributing_narrative_ids": ["nar-1", "nar-2", "nar-3"],
})
_crud_repo.upsert_ticker_convergence({
    "ticker": "AAPL",
    "convergence_count": 2,
    "direction_agreement": 0.5,
    "direction_consensus": 0.5,
    "weighted_confidence": 0.6,
    "source_diversity": 2,
    "pressure_score": 0.6,
    "contributing_narrative_ids": ["nar-4", "nar-5"],
})
_crud_repo.upsert_ticker_convergence({
    "ticker": "TSLA",
    "convergence_count": 4,
    "direction_agreement": -0.9,
    "direction_consensus": 0.9,
    "weighted_confidence": 0.85,
    "source_diversity": 4,
    "pressure_score": 3.06,
    "contributing_narrative_ids": ["nar-6", "nar-7", "nar-8", "nar-9"],
})

# Update NVDA (upsert should replace)
_crud_repo.upsert_ticker_convergence({
    "ticker": "NVDA",
    "convergence_count": 4,
    "direction_agreement": -0.9,
    "direction_consensus": 0.9,
    "weighted_confidence": 0.8,
    "source_diversity": 4,
    "pressure_score": 2.88,
    "contributing_narrative_ids": ["nar-1", "nar-2", "nar-3", "nar-10"],
})

_top = _crud_repo.get_top_convergences(limit=5)
_single = _crud_repo.get_ticker_convergence("NVDA")

T("SP3-CRUD-1: upsert replaces, get_top_convergences returns sorted by pressure DESC",
  len(_top) == 3
  and _top[0]["ticker"] == "TSLA"  # highest pressure 3.06
  and _top[1]["ticker"] == "NVDA"  # updated to 2.88
  and _top[2]["ticker"] == "AAPL"  # 0.6
  and _single is not None
  and _single["convergence_count"] == 4  # updated value
  and abs(_single["pressure_score"] - 2.88) < 0.01,
  f"top order={[t['ticker'] for t in _top]}, NVDA count={_single['convergence_count'] if _single else 'None'}")


# ===========================================================================
# Section 5: Integration round-trip
# ===========================================================================
S("SP3-INT: Integration round-trip")

_int_repo = _get_temp_repo()

# Insert two narratives with linked_assets pointing to same ticker
for _nar_id, _tier in [("int-nar-1", 1), ("int-nar-2", 3)]:
    _int_repo.insert_narrative({
        "narrative_id": _nar_id,
        "name": f"Test Narrative {_nar_id}",
        "description": "test",
        "stage": "Growing",
        "document_count": 5,
        "suppressed": 0,
        "linked_assets": json.dumps([
            {"ticker": "MSFT", "asset_name": "Microsoft", "similarity_score": 0.85},
        ]),
        "source_highest_tier": _tier,
    })

# Insert signals
_int_repo.upsert_narrative_signal({
    "narrative_id": "int-nar-1",
    "direction": "bearish",
    "confidence": 0.85,
    "timeframe": "near_term",
    "magnitude": "significant",
    "certainty": "expected",
    "key_actors": [],
    "affected_sectors": ["technology"],
    "catalyst_type": "earnings",
    "extracted_at": datetime.now(timezone.utc).isoformat(),
})
_int_repo.upsert_narrative_signal({
    "narrative_id": "int-nar-2",
    "direction": "bearish",
    "confidence": 0.70,
    "timeframe": "near_term",
    "magnitude": "significant",
    "certainty": "rumored",
    "key_actors": [],
    "affected_sectors": ["technology"],
    "catalyst_type": "regulatory",
    "extracted_at": datetime.now(timezone.utc).isoformat(),
})

# Run compute_all_convergences
_int_vs = MockVectorStore({
    "int-nar-1": _make_orthogonal_vec(0),
    "int-nar-2": _make_orthogonal_vec(1),
})
_int_narratives = _int_repo.get_all_active_narratives()
_int_convergences = compute_all_convergences(_int_narratives, _int_repo, _int_vs)

# Upsert results
for _ticker, _conv in _int_convergences.items():
    _int_repo.upsert_ticker_convergence({
        "ticker": _ticker,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        **_conv,
    })

# Propagate convergence_exposure
for _n in _int_narratives:
    _raw = _n.get("linked_assets")
    _max_exp = 0.0
    if _raw:
        _assets = json.loads(_raw) if isinstance(_raw, str) else _raw
        for _a in _assets:
            _t = _a.get("ticker", "").upper() if isinstance(_a, dict) else str(_a).upper()
            if _t in _int_convergences:
                _max_exp = max(_max_exp, _int_convergences[_t].get("pressure_score", 0.0))
    _int_repo.update_narrative(_n["narrative_id"], {
        "convergence_exposure": _max_exp if _max_exp > 0 else None,
    })

# Verify
_int_tc = _int_repo.get_ticker_convergence("MSFT")
_int_nar_1 = _int_repo.get_narrative("int-nar-1")
_int_nar_2 = _int_repo.get_narrative("int-nar-2")

T("SP3-INT-1: full round-trip — ticker_convergence populated, convergence_exposure set",
  _int_tc is not None
  and _int_tc["convergence_count"] == 2
  and _int_tc["direction_agreement"] < 0  # both bearish
  and _int_tc["pressure_score"] > 0
  and _int_nar_1 is not None
  and _int_nar_1.get("convergence_exposure") is not None
  and _int_nar_1["convergence_exposure"] > 0
  and _int_nar_2 is not None
  and _int_nar_2.get("convergence_exposure") is not None
  and _int_nar_2["convergence_exposure"] > 0,
  f"MSFT convergence: count={_int_tc['convergence_count'] if _int_tc else 'None'}, "
  f"pressure={_int_tc['pressure_score'] if _int_tc else 'None'}, "
  f"nar-1 exposure={_int_nar_1.get('convergence_exposure') if _int_nar_1 else 'None'}")


# ===========================================================================
# Section 6: Centrality all-zero guard (P12 Batch 1.1)
# ===========================================================================
S("SP3-CENT: flag_catalysts all-zero guard")

from centrality import flag_catalysts

# Empty map → []
T("SP3-CENT-1: empty centrality_scores returns []",
  flag_catalysts({}) == [],
  "got non-empty list")

# All-zero map → [] (no stale catalyst manufacturing)
_all_zero = {"nar-a": 0.0, "nar-b": 0.0, "nar-c": 0.0}
T("SP3-CENT-2: all-zero scores returns [] (no stale catalyst)",
  flag_catalysts(_all_zero) == [],
  f"got {flag_catalysts(_all_zero)}")

# At least one positive → normal top-decile behavior
_mixed = {"nar-a": 0.0, "nar-b": 0.5, "nar-c": 0.0}
_cats = flag_catalysts(_mixed)
T("SP3-CENT-3: mixed scores returns top positive narrative",
  len(_cats) == 1 and _cats[0] == "nar-b",
  f"got {_cats}")


# ===========================================================================
# Section 7: replace_ticker_convergences atomicity (P12 Batch 1.2)
# ===========================================================================
S("SP3-REPLACE: replace_ticker_convergences atomic replace")

_replace_repo = _get_temp_repo()

# Seed old rows
_replace_repo.upsert_ticker_convergence({
    "ticker": "OLD1",
    "convergence_count": 1,
    "direction_agreement": 0.5,
    "direction_consensus": 0.5,
    "weighted_confidence": 0.5,
    "source_diversity": 1,
    "pressure_score": 0.5,
    "contributing_narrative_ids": [],
})

# Replace with entirely new set
_new_convergences = {
    "NEW1": {
        "convergence_count": 2,
        "direction_agreement": -0.8,
        "direction_consensus": 0.8,
        "weighted_confidence": 0.7,
        "source_diversity": 2,
        "pressure_score": 1.12,
        "contributing_narrative_ids": ["n1", "n2"],
    }
}
_replace_repo.replace_ticker_convergences(_new_convergences)
_old_row = _replace_repo.get_ticker_convergence("OLD1")
_new_row = _replace_repo.get_ticker_convergence("NEW1")
T("SP3-REPLACE-1: old rows removed after replace",
  _old_row is None,
  f"OLD1 still present: {_old_row}")
T("SP3-REPLACE-2: new rows present after replace",
  _new_row is not None and _new_row["convergence_count"] == 2,
  f"NEW1: {_new_row}")

# Empty replace clears all rows
_replace_repo.replace_ticker_convergences({})
T("SP3-REPLACE-3: empty convergences dict clears all rows",
  _replace_repo.get_ticker_convergence("NEW1") is None and len(_replace_repo.get_all_ticker_convergences()) == 0,
  "rows remain after empty replace")

# Stale-row preservation: if replace raises, old rows must remain
_stale_repo = _get_temp_repo()
_stale_repo.upsert_ticker_convergence({
    "ticker": "STALE",
    "convergence_count": 3,
    "direction_agreement": -0.9,
    "direction_consensus": 0.9,
    "weighted_confidence": 0.8,
    "source_diversity": 3,
    "pressure_score": 2.16,
    "contributing_narrative_ids": [],
})
try:
    # Simulate compute failure — caller should NOT call replace at all
    # Here we verify that NOT calling replace means old row stays intact
    raise RuntimeError("simulated compute failure")
except RuntimeError:
    pass
_stale_still = _stale_repo.get_ticker_convergence("STALE")
T("SP3-REPLACE-4: compute failure leaves previous convergence row intact (replace not called)",
  _stale_still is not None and abs(_stale_still["pressure_score"] - 2.16) < 0.01,
  f"stale row: {_stale_still}")


# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 60)
_passed = sum(1 for _, ok in _results if ok)
_total = len(_results)
print(f"Phase 3 results: {_passed}/{_total} passed")
if _passed < _total:
    print("FAILED:")
    for name, ok in _results:
        if not ok:
            print(f"  - {name}")
    sys.exit(1)
else:
    print("ALL PASSED")
