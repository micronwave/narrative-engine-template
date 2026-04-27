"""
Audit tests for asset_mapper.py, build_asset_library.py, output.py, and
the related repository.py ticker-lookup fix.

Run with:
    python -X utf8 tests/test_asset_output_audit.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import json
import os
import pickle
import sys
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np

# ---------------------------------------------------------------------------
import logging
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s", stream=sys.stderr)

# ---------------------------------------------------------------------------
# Simple test runner (same convention as project)
# ---------------------------------------------------------------------------

_results: list[dict] = []
_current_section: str = "Unset"
_pass = 0
_fail = 0


def S(section_name: str) -> None:
    global _current_section
    _current_section = section_name


def T(name: str, condition: bool, details: str = "") -> None:
    global _pass, _fail
    _results.append({"section": _current_section, "name": name, "passed": bool(condition), "details": details})
    if condition:
        _pass += 1
    else:
        _fail += 1
        print(f"  FAIL [{_current_section}] {name}" + (f" — {details}" if details else ""), file=sys.stderr)


def _print_summary() -> None:
    sections: dict[str, dict] = {}
    for r in _results:
        sec = r["section"]
        if sec not in sections:
            sections[sec] = {"pass": 0, "fail": 0}
        if r["passed"]:
            sections[sec]["pass"] += 1
        else:
            sections[sec]["fail"] += 1
    print("\n" + "=" * 60)
    print(f"{'Section':<40} {'Pass':>5} {'Fail':>5}")
    print("-" * 60)
    for sec, counts in sections.items():
        marker = "" if counts["fail"] == 0 else " <--"
        print(f"  {sec:<38} {counts['pass']:>5} {counts['fail']:>5}{marker}")
    print("=" * 60)
    print(f"  TOTAL: {_pass} passed, {_fail} failed out of {_pass + _fail} tests")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Imports under test
# ---------------------------------------------------------------------------
from output import build_output_object, validate_output, write_outputs, DISCLAIMER
from build_asset_library import TICKERS

# ===========================================================================
S("A. Ticker Data Quality (Fixes 5-7)")
# ===========================================================================

# A1: No duplicate keys in TICKERS dict source (verify by scanning file)
try:
    _src_path = Path(__file__).resolve().parent.parent / "build_asset_library.py"
    _src = _src_path.read_text(encoding="utf-8")
    import re as _re
    _ticker_keys = _re.findall(r'^\s+"([A-Z0-9.\-]+)":\s+"', _src, _re.MULTILINE)
    _dupes = [t for t in set(_ticker_keys) if _ticker_keys.count(t) > 1]
    # Filter to only TICKERS dict entries (before NON_FILING_ASSETS)
    _tickers_section = _src.split("NON_FILING_ASSETS")[0]
    _ticker_keys_section = _re.findall(r'^\s+"([A-Z0-9.\-]+)":\s+"', _tickers_section, _re.MULTILINE)
    _dupes_section = [t for t in set(_ticker_keys_section) if _ticker_keys_section.count(t) > 1]
    T("A1 no duplicate ticker keys in TICKERS dict", len(_dupes_section) == 0,
      f"duplicates: {_dupes_section}" if _dupes_section else "")
except Exception as _e:
    T("A1 no duplicate ticker keys", False, str(_e))

# A2: Invalid/stale tickers removed
_removed_tickers = ["L3H", "ATVI", "PXD", "SFR", "SNH", "PEAK"]
for _rt in _removed_tickers:
    T(f"A2 stale ticker {_rt} not in TICKERS", _rt not in TICKERS)

# A3: Valid tickers still present (spot check — ensure we didn't over-delete)
_must_exist = ["LHX", "MRK", "LLY", "ABBV", "BMY", "AMGN", "GILD", "REGN", "MCK", "AAPL", "MSFT"]
for _me in _must_exist:
    T(f"A3 ticker {_me} still in TICKERS", _me in TICKERS)


# ===========================================================================
S("B. Output — float safety (Fixes 2-3)")
# ===========================================================================

_BASE_NARRATIVE = {
    "narrative_id": str(uuid.uuid4()),
    "name": "Test Narrative",
    "description": "desc",
    "stage": "Emerging",
    "velocity": 1.0,
    "velocity_windowed": 0.5,
    "centrality": 0.3,
    "is_catalyst": 0,
    "is_coordinated": 0,
    "suppressed": 0,
    "human_review_required": 0,
    "ns_score": 0.75,
    "entropy": 0.42,
    "intent_weight": 0.1,
    "cross_source_score": 0.6,
    "document_count": 5,
}

# B1: build_output_object with None score_component value doesn't crash
try:
    _sc_none = {"velocity": 0.5, "cohesion": None, "centrality": 0.3}
    _obj_b1 = build_output_object(
        narrative=_BASE_NARRATIVE,
        linked_assets=[],
        supporting_evidence=[],
        lifecycle_reasoning="test",
        mutation_analysis=None,
        score_components=_sc_none,
    )
    T("B1 None score_component doesn't crash", True)
    T("B1 None contribution becomes 0", _obj_b1["reasoning_trace"][1]["contribution"] == 0.0)
except Exception as _e:
    T("B1 None score_component doesn't crash", False, str(_e))

# B2: entropy serializes as JSON number not string
try:
    _nar_np = dict(_BASE_NARRATIVE)
    _nar_np["entropy"] = np.float64(0.523)
    _obj_b2 = build_output_object(
        narrative=_nar_np,
        linked_assets=[],
        supporting_evidence=[],
        lifecycle_reasoning="test",
        mutation_analysis=None,
        score_components={"v": 1.0},
    )
    _json_str = json.dumps(_obj_b2)
    _parsed = json.loads(_json_str)
    T("B2 entropy is float not string", isinstance(_parsed["entropy"], float),
      f"type={type(_parsed['entropy']).__name__}, val={_parsed['entropy']!r}")
except Exception as _e:
    T("B2 entropy is float not string", False, str(_e))

# B3: entropy=None stays None
try:
    _nar_none = dict(_BASE_NARRATIVE)
    _nar_none["entropy"] = None
    _obj_b3 = build_output_object(
        narrative=_nar_none,
        linked_assets=[],
        supporting_evidence=[],
        lifecycle_reasoning="test",
        mutation_analysis=None,
        score_components={"v": 1.0},
    )
    T("B3 entropy=None stays null", _obj_b3["entropy"] is None)
except Exception as _e:
    T("B3 entropy=None stays null", False, str(_e))


# ===========================================================================
S("C. Output — write_outputs safety (Fixes 8-9)")
# ===========================================================================

# C1: Invalid date format raises ValueError
try:
    _raised_c1 = False
    try:
        write_outputs([], "../../etc/hack")
    except ValueError:
        _raised_c1 = True
    T("C1 path traversal date raises ValueError", _raised_c1)
except Exception as _e:
    T("C1 path traversal date raises ValueError", False, str(_e))

# C2: Valid date format works
try:
    _orig_cwd = os.getcwd()
    _tmp_c2 = tempfile.mkdtemp()
    os.chdir(_tmp_c2)
    write_outputs([], "2024-01-15")
    _out_path = Path(_tmp_c2) / "data" / "outputs" / "2024-01-15" / "narratives.json"
    T("C2 valid date creates output file", _out_path.exists())
    T("C2 empty list writes []", json.loads(_out_path.read_text(encoding="utf-8")) == [])
    os.chdir(_orig_cwd)
except Exception as _e:
    os.chdir(_orig_cwd)
    T("C2 valid date writes correctly", False, str(_e))

# C3: Atomic write — file is valid JSON even if content is large
try:
    _orig_cwd = os.getcwd()
    _tmp_c3 = tempfile.mkdtemp()
    os.chdir(_tmp_c3)
    _big_outputs = [build_output_object(
        narrative={**_BASE_NARRATIVE, "narrative_id": str(uuid.uuid4())},
        linked_assets=[{"ticker": "AAPL", "asset_name": "Apple", "similarity_score": 0.9}],
        supporting_evidence=[{"source_url": "http://test.com", "source_domain": "test.com",
                              "published_at": "2024-01-15", "excerpt": "test"}],
        lifecycle_reasoning="test",
        mutation_analysis=None,
        score_components={"v": 1.0},
    ) for _ in range(50)]
    write_outputs(_big_outputs, "2024-06-01")
    _out = Path(_tmp_c3) / "data" / "outputs" / "2024-06-01" / "narratives.json"
    _parsed_c3 = json.loads(_out.read_text(encoding="utf-8"))
    T("C3 atomic write produces valid JSON", len(_parsed_c3) == 50)
    # No .tmp files left behind
    _tmp_files = list(Path(_tmp_c3, "data", "outputs", "2024-06-01").glob("*.tmp"))
    T("C3 no temp files left", len(_tmp_files) == 0)
    os.chdir(_orig_cwd)
except Exception as _e:
    os.chdir(_orig_cwd)
    T("C3 atomic write", False, str(_e))


# ===========================================================================
S("D. AssetMapper — query normalization (Fix 4)")
# ===========================================================================

try:
    from asset_mapper import AssetMapper
    from embedding_model import MiniLMEmbedder
    from settings import Settings

    _settings_d = Settings()
    _embedder_d = MiniLMEmbedder(_settings_d)
    _dim = _embedder_d.dimension()

    # Build a small test library
    _d_dir = tempfile.mkdtemp()
    _d_lib = os.path.join(_d_dir, "test_lib.pkl")
    _d_emb = _embedder_d.embed(["Apple technology iPhone smartphone"])
    _d_asset_lib = {"AAPL": {"name": "Apple Inc", "embedding": _d_emb[0]}}
    with open(_d_lib, "wb") as _df:
        pickle.dump(_d_asset_lib, _df)

    _mapper_d = AssetMapper(_d_lib, _embedder_d)

    # D1: Zero vector returns empty (no crash)
    _zero_vec = np.zeros(_dim, dtype=np.float32)
    _results_d1 = _mapper_d.map_narrative(_zero_vec, min_similarity=0.0)
    T("D1 zero vector returns empty list", _results_d1 == [])

    # D2: Unnormalized vector still produces valid results
    _unnorm = _embedder_d.embed_single("Apple iPhone tech") * 100.0  # scale up 100x
    _results_d2 = _mapper_d.map_narrative(_unnorm, min_similarity=0.0)
    T("D2 unnormalized vector returns results", len(_results_d2) > 0)

    # D3: Similarity scores are in [0, 1] range (cosine sim, not raw dot product)
    if _results_d2:
        _max_sim = max(r["similarity_score"] for r in _results_d2)
        T("D3 similarity in [0,1] range", 0.0 <= _max_sim <= 1.01,
          f"max_sim={_max_sim}")
    else:
        T("D3 similarity in [0,1] range (no results)", True)

except Exception as _e:
    T("D1-D3 AssetMapper normalization", False, str(_e))


# ===========================================================================
S("E. Repository — ticker lookup (Fix 1)")
# ===========================================================================

try:
    import sqlite3
    from repository import SqliteRepository

    _e_dir = tempfile.mkdtemp()
    _e_db = os.path.join(_e_dir, "test.db")
    _e_repo = SqliteRepository(_e_db)
    _e_repo.migrate()

    # Insert a narrative with dict-format linked_assets (as produced by AssetMapper)
    _e_nid = str(uuid.uuid4())
    _e_linked = json.dumps([
        {"ticker": "AAPL", "asset_name": "Apple Inc", "similarity_score": 0.85},
        {"ticker": "MSFT", "asset_name": "Microsoft", "similarity_score": 0.72},
    ])
    _e_repo.insert_narrative({
        "narrative_id": _e_nid,
        "name": "Tech Earnings",
        "stage": "Growing",
        "ns_score": 0.8,
        "linked_assets": _e_linked,
        "suppressed": 0,
    })

    # E1: get_narratives_for_ticker finds AAPL
    _e_results = _e_repo.get_narratives_for_ticker("AAPL")
    T("E1 ticker lookup finds AAPL in dict-format linked_assets",
      len(_e_results) == 1 and _e_results[0]["narrative_id"] == _e_nid,
      f"results={_e_results}")

    # E2: Case insensitive
    _e_results_lower = _e_repo.get_narratives_for_ticker("aapl")
    T("E2 ticker lookup is case-insensitive", len(_e_results_lower) == 1)

    # E3: Non-matching ticker returns empty
    _e_results_none = _e_repo.get_narratives_for_ticker("TSLA")
    T("E3 non-matching ticker returns empty", len(_e_results_none) == 0)

    # E4: get_ticker_impact_score returns non-zero for linked ticker
    _e_impact = _e_repo.get_ticker_impact_score("AAPL")
    T("E4 impact score is non-zero for linked ticker", _e_impact > 0,
      f"impact={_e_impact}")

except Exception as _e:
    T("E1-E4 ticker lookup", False, str(_e))


# ===========================================================================
S("F. SEC Email Setting (Fix 10)")
# ===========================================================================

try:
    from settings import Settings as _Settings_F
    _s = _Settings_F()
    T("F1 SEC_EDGAR_EMAIL setting exists", hasattr(_s, "SEC_EDGAR_EMAIL"))
    T("F1 SEC_EDGAR_EMAIL has default value", _s.SEC_EDGAR_EMAIL == "research@example.com")
except Exception as _e:
    T("F1 SEC_EDGAR_EMAIL", False, str(_e))


# ===========================================================================
S("G. Output validation edge cases")
# ===========================================================================

# G1: validate_output with evidence that has blank source_domain
try:
    _g1_obj = build_output_object(
        narrative=_BASE_NARRATIVE,
        linked_assets=[],
        supporting_evidence=[
            {"source_url": "http://test.com", "source_domain": "", "published_at": "2024-01-01",
             "excerpt": "test"}
        ],
        lifecycle_reasoning="test",
        mutation_analysis=None,
        score_components={"v": 1.0},
    )
    _g1_valid = validate_output(_g1_obj)
    # evidence present but all domains blank → domains is empty → validation fails
    T("G1 blank source_domain with evidence fails validation", _g1_valid is False)
except Exception as _e:
    T("G1 blank source_domain validation", False, str(_e))

# G2: validate_output with proper evidence passes
try:
    _g2_obj = build_output_object(
        narrative=_BASE_NARRATIVE,
        linked_assets=[],
        supporting_evidence=[
            {"source_url": "http://test.com", "source_domain": "test.com",
             "published_at": "2024-01-01", "excerpt": "test"}
        ],
        lifecycle_reasoning="test",
        mutation_analysis=None,
        score_components={"v": 1.0},
    )
    T("G2 valid evidence passes validation", validate_output(_g2_obj) is True)
except Exception as _e:
    T("G2 valid evidence validation", False, str(_e))

# G3: DISCLAIMER constant hasn't changed
T("G3 DISCLAIMER constant", DISCLAIMER == "INTELLIGENCE ONLY \u2014 NOT FINANCIAL ADVICE. For informational purposes only.")


# ===========================================================================
S("H. Asset Library Freshness (Phase 4)")
# ===========================================================================

import time as _time_h

try:
    from settings import Settings as _Settings_H
    from safe_pickle import safe_load as _safe_load_h

    _settings_h = _Settings_H()
    _lib_path_h = Path(_settings_h.ASSET_LIBRARY_PATH)

    # H1: asset_library.pkl exists at the configured path
    T("H1 asset_library.pkl exists",
      _lib_path_h.exists(),
      f"expected at: {_lib_path_h.resolve()} — run: python build_asset_library.py")

    if _lib_path_h.exists():
        _lib_h = _safe_load_h(str(_lib_path_h), allowed={
            "builtins": {"dict", "list", "tuple", "str", "int", "float", "bool"},
            "numpy": {"ndarray", "dtype", "float32", "float64"},
            "numpy.core.multiarray": {"scalar", "_reconstruct"},
            "numpy._core.multiarray": {"scalar", "_reconstruct"},
            "numpy._core.numeric": {"_frombuffer"},
            "numpy.core.numeric": {"_frombuffer"},
        })
        T("H1b asset_library.pkl payload is dict", isinstance(_lib_h, dict),
          f"type={type(_lib_h).__name__}")

        if isinstance(_lib_h, dict):
            def _entry_embedding_h(lib: dict, ticker: str):
                row = lib.get(ticker)
                if isinstance(row, dict):
                    return row.get("embedding")
                return None

            # H2: NKE present with a valid non-zero float32 embedding
            _nke_emb_h = _entry_embedding_h(_lib_h, "NKE")
            _nke_ok = (
                isinstance(_nke_emb_h, np.ndarray)
                and _nke_emb_h.ndim == 1
                and _nke_emb_h.shape[0] > 0
                and _nke_emb_h.dtype == np.float32
                and float(np.linalg.norm(_nke_emb_h)) > 0.0
            )
            T("H2 NKE present with valid embedding", _nke_ok,
              "NKE missing or embedding is zero/empty/non-float32 — rebuild asset library")

            # H3: LULU present with a valid non-zero float32 embedding
            _lulu_emb_h = _entry_embedding_h(_lib_h, "LULU")
            _lulu_ok = (
                isinstance(_lulu_emb_h, np.ndarray)
                and _lulu_emb_h.ndim == 1
                and _lulu_emb_h.shape[0] > 0
                and _lulu_emb_h.dtype == np.float32
                and float(np.linalg.norm(_lulu_emb_h)) > 0.0
            )
            T("H3 LULU present with valid embedding", _lulu_ok,
              "LULU missing or embedding is zero/empty/non-float32 — rebuild asset library")

            # H4: File timestamp must be sane and not stale (0 <= age <= 180 days)
            _age_days_h = (_time_h.time() - _lib_path_h.stat().st_mtime) / 86400.0
            T("H4 asset_library.pkl age <= 180 days",
              0.0 <= _age_days_h <= 180.0,
              f"age={_age_days_h:.1f} days — rebuild with: python build_asset_library.py")

            # H5: Representative critical tickers present across asset classes
            _critical_h = ["AAPL", "MSFT", "NVDA", "GLD", "BTC-USD"]
            for _ct_h in _critical_h:
                T(f"H5 critical ticker {_ct_h} present",
                  _ct_h in _lib_h,
                  f"{_ct_h} missing from asset library — rebuild")

            # H6: Every embedding is valid and matches the active pipeline dimension
            _expected_dims_h = {"dense": 768, "hybrid": 832}
            _expected_dim_h = _expected_dims_h.get(_settings_h.EMBEDDING_MODE)
            T("H6a EMBEDDING_MODE supported for audit", _expected_dim_h is not None,
              f"mode={_settings_h.EMBEDDING_MODE!r}")
            if _expected_dim_h is not None:
                _invalid_embeddings_h = []
                for _ticker_h, _row_h in _lib_h.items():
                    _emb_h = _row_h.get("embedding") if isinstance(_row_h, dict) else None
                    if not isinstance(_emb_h, np.ndarray):
                        _invalid_embeddings_h.append(_ticker_h)
                        continue
                    if _emb_h.ndim != 1 or _emb_h.shape[0] != _expected_dim_h:
                        _invalid_embeddings_h.append(_ticker_h)
                T("H6 all embeddings match pipeline dimension",
                  len(_invalid_embeddings_h) == 0,
                  f"invalid entries (first 5): {_invalid_embeddings_h[:5]} "
                  f"expected_dim={_expected_dim_h} — rebuild with matching EMBEDDING_MODE")
        else:
            _skipped_bad_type_h = [
                "H2 NKE embedding", "H3 LULU embedding", "H4 staleness",
                "H5 AAPL", "H5 MSFT", "H5 NVDA", "H5 GLD", "H5 BTC-USD",
                "H6a embedding mode check", "H6 dimension consistency",
            ]
            for _sh in _skipped_bad_type_h:
                T(f"{_sh} (skipped — invalid payload type)", False,
                  "asset_library.pkl must deserialize to dict")

    else:
        _skipped_h = [
            "H2 NKE embedding", "H3 LULU embedding", "H4 staleness",
            "H5 AAPL", "H5 MSFT", "H5 NVDA", "H5 GLD", "H5 BTC-USD",
            "H6 dimension consistency",
        ]
        for _sh in _skipped_h:
            T(f"{_sh} (skipped — library missing)", False,
              "run python build_asset_library.py first")

except Exception as _e_h:
    T("H section setup", False, str(_e_h))


# ===========================================================================
# Summary
# ===========================================================================

_print_summary()
sys.exit(0 if _fail == 0 else 1)
