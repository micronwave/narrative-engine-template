"""
Security Audit S4 Checkpoint B test suite — Pickle Deserialization Safety (M7).

Tests:
  - safe_pickle.py: RestrictedUnpickler blocks forbidden classes
  - safe_pickle.py: safe_load allows permitted classes
  - All 6 pickle.load sites replaced (source inspection)

Run with:
    python -X utf8 tests/test_sec_s4b.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import io
import logging
import os
import pickle
import sys
import tempfile
from pathlib import Path

import numpy as np

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)

# ---------------------------------------------------------------------------
# Custom test runner
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
    _results.append({
        "section": _current_section,
        "name": name,
        "passed": bool(condition),
        "details": details,
    })
    if condition:
        _pass += 1
    else:
        _fail += 1
    mark = "PASS" if condition else "FAIL"
    det = f"  ({details})" if details else ""
    print(f"  [{mark}] {name}{det}")


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from safe_pickle import RestrictedUnpickler, safe_load


# ===================================================================
# M7 — RestrictedUnpickler blocks forbidden classes
# ===================================================================

S("M7 — RestrictedUnpickler blocks forbidden classes")


def _make_pickle_bytes(obj) -> bytes:
    """Serialize obj to bytes."""
    buf = io.BytesIO()
    pickle.dump(obj, buf)
    return buf.getvalue()


def _load_with_allowed(data: bytes, allowed: dict) -> object:
    buf = io.BytesIO(data)
    return RestrictedUnpickler(buf, allowed).load()


# Test: plain dict loads fine
plain_dict = {"key": "value", "num": 42}
plain_bytes = _make_pickle_bytes(plain_dict)

try:
    result = _load_with_allowed(plain_bytes, {"builtins": {"dict", "str", "int"}})
    T("plain dict loads with correct allowlist",
      result == plain_dict, f"got: {result}")
except Exception as e:
    T("plain dict loads with correct allowlist", False, str(e))

# Test: custom class blocked when not in allowlist
# Plain dicts use pickle opcodes (no find_class); use numpy array to test class blocking
_numpy_arr = np.array([1.0, 2.0], dtype=np.float32)
_numpy_bytes = _make_pickle_bytes(_numpy_arr)

try:
    _load_with_allowed(_numpy_bytes, {"builtins": {"dict"}})  # numpy not in allowlist
    T("numpy class blocked when not in allowlist", False, "no exception raised")
except pickle.UnpicklingError as e:
    T("numpy class blocked when not in allowlist", "Forbidden" in str(e), str(e)[:80])
except Exception as e:
    T("numpy class blocked when not in allowlist", False, f"wrong exception: {e}")

# Test: malicious pickle raises UnpicklingError
# Craft a pickle that would call os.system("echo HACKED") on load


class _MaliciousPayload:
    def __reduce__(self):
        return (os.system, ("echo HACKED",))


malicious_bytes = _make_pickle_bytes(_MaliciousPayload())

with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tf:
    tf.write(malicious_bytes)
    malicious_path = tf.name

try:
    safe_load(malicious_path, allowed={"builtins": {"dict", "list"}})
    T("malicious pickle raises UnpicklingError", False, "no exception raised")
except pickle.UnpicklingError as e:
    T("malicious pickle raises UnpicklingError", True, str(e)[:80])
except Exception as e:
    T("malicious pickle raises UnpicklingError", False, f"wrong exception: {type(e).__name__}: {e}")
finally:
    os.unlink(malicious_path)


# ===================================================================
# M7 — safe_load with numpy array (asset library pattern)
# ===================================================================

S("M7 — safe_load with numpy array")

emb = np.random.rand(768).astype(np.float32)
asset_lib = {"AAPL": {"name": "Apple Inc.", "embedding": emb}}
asset_bytes = _make_pickle_bytes(asset_lib)

with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tf:
    tf.write(asset_bytes)
    asset_path = tf.name

_ASSET_ALLOWED = {
    "builtins": {"dict", "list", "tuple", "str", "int", "float", "bool"},
    "numpy": {"ndarray", "dtype", "float32", "float64"},
    "numpy.core.multiarray": {"scalar", "_reconstruct"},
    "numpy._core.multiarray": {"scalar", "_reconstruct"},
    "numpy._core.numeric": {"_frombuffer"},
    "numpy.core.numeric": {"_frombuffer"},
}

try:
    result = safe_load(asset_path, allowed=_ASSET_ALLOWED)
    T("numpy array dict loads with asset allowlist",
      "AAPL" in result and "name" in result["AAPL"],
      f"keys: {list(result.keys())}")
    T("loaded numpy array has correct shape",
      result["AAPL"]["embedding"].shape == (768,),
      f"shape: {result['AAPL']['embedding'].shape}")
except Exception as e:
    T("numpy array dict loads with asset allowlist", False, str(e))
    T("loaded numpy array has correct shape", False, str(e))
finally:
    os.unlink(asset_path)


# ===================================================================
# M7 — Source inspection: no raw pickle.load in production files
# ===================================================================

S("M7 — Source inspection: no raw pickle.load")

_PRODUCTION_FILES = [
    ROOT / "asset_mapper.py",
    ROOT / "deduplicator.py",
    ROOT / "embedding_model.py",
    ROOT / "visualize_clusters.py",
    ROOT / "vector_store.py",
    ROOT / "signal_trainer.py",
]

for fpath in _PRODUCTION_FILES:
    source = fpath.read_text(encoding="utf-8")
    # Allow "pickle.dump" but not "pickle.load"
    raw_load_count = source.count("pickle.load")
    T(f"{fpath.name}: no raw pickle.load",
      raw_load_count == 0,
      f"found {raw_load_count} occurrence(s)")

# Test file
test_file = ROOT / "tests" / "test_phase5_integration.py"
test_source = test_file.read_text(encoding="utf-8")
raw_count = test_source.count("pickle.load")
T("test_phase5_integration.py: no raw pickle.load",
  raw_count == 0,
  f"found {raw_count} occurrence(s)")


# ===================================================================
# M7 — safe_pickle.py is importable and has correct interface
# ===================================================================

S("M7 — safe_pickle module interface")

T("RestrictedUnpickler is a class",
  isinstance(RestrictedUnpickler, type))

T("safe_load is callable",
  callable(safe_load))

# Verify safe_load signature accepts path and allowed params
import inspect
sig = inspect.signature(safe_load)
params = list(sig.parameters.keys())
T("safe_load has 'path' parameter",
  "path" in params, f"params: {params}")
T("safe_load has 'allowed' parameter",
  "allowed" in params, f"params: {params}")


# ===================================================================
# L4 — Path Parameter Length Constraints (api/main.py)
# ===================================================================

S("L4 — FastAPI Path import")

_main_source = (ROOT / "api" / "main.py").read_text(encoding="utf-8")

T("Path imported as FPath from fastapi",
  "from fastapi import" in _main_source and "Path as FPath" in _main_source)

S("L4 — narrative_id endpoints have max_length=50")

_NARRATIVE_ID_FUNCS = [
    "def get_narrative_detail(",
    "def get_narrative_assets(",
    "def export_narrative(",
    "def get_narrative_manipulation(",
    "def get_narrative_history(",
    "def get_narrative_coordination(",
    "def get_narrative_correlations(",
    "def get_narrative_sources(",
    "def get_narrative_documents(",
    "def get_narrative_timeline(",
    "def get_narrative_changelog(",
    "def compare_narrative_snapshots(",
    "def analyze_narrative(",
]

for _fn in _NARRATIVE_ID_FUNCS:
    # Search block starting at function def (covers multi-line signatures up to first ':')
    _idx = _main_source.find(_fn)
    _block = _main_source[_idx:_idx + 600] if _idx >= 0 else ""
    _end = _block.find("):") + 2 if "):" in _block else len(_block)
    _sig_block = _block[:_end]
    _found = _idx >= 0 and "FPath" in _sig_block and "max_length=50" in _sig_block
    _first_line = _main_source[_idx:_idx + 80].split("\n")[0] if _idx >= 0 else "NOT FOUND"
    T(f"{_fn.strip('def (')}: narrative_id has FPath max_length=50", _found,
      f"sig: {_first_line}")

# get_correlation has narrative_id in a multi-path endpoint
_corr_lines = [l for l in _main_source.splitlines() if "def get_correlation(" in l]
T("get_correlation: narrative_id has FPath max_length=50",
  len(_corr_lines) > 0 and "FPath" in _corr_lines[0] and "max_length=50" in _corr_lines[0],
  f"sig: {_corr_lines[0][:100] if _corr_lines else 'NOT FOUND'}")

S("L4 — symbol/ticker endpoints have FPath and _validate_symbol")

_SYMBOL_FUNCS = [
    "def get_security_quote(",
    "def get_stock_detail(",
    "def get_brief(",
    "def get_price_history_endpoint(",
]
for _fn in _SYMBOL_FUNCS:
    _idx = _main_source.find(_fn)
    _block = _main_source[_idx:_idx + 600] if _idx >= 0 else ""
    _end = _block.find("):") + 2 if "):" in _block else len(_block)
    _sig_block = _block[:_end]
    _found = _idx >= 0 and "FPath" in _sig_block and "max_length=12" in _sig_block
    _sig_lines = [l for l in _main_source.splitlines() if _fn in l]
    T(f"{_fn.strip('def (')}: symbol/ticker has FPath max_length=12", _found,
      f"sig: {_sig_lines[0][:80] if _sig_lines else 'NOT FOUND'}")

# _validate_symbol added to 3 unvalidated endpoints
for _fn, _var in [
    ("def get_security_quote(", "symbol = _validate_symbol(symbol)"),
    ("def get_stock_detail(", "symbol = _validate_symbol(symbol)"),
    ("def get_brief(", "ticker = _validate_symbol(ticker)"),
]:
    # find the block of code from function def to end of body start
    _idx = _main_source.find(_fn)
    _block = _main_source[_idx:_idx + 500] if _idx >= 0 else ""
    T(f"{_fn.strip('def (')}: calls _validate_symbol",
      _var in _block,
      f"found: {_var in _block}")

S("L4 — other ID endpoints have max_length=50")

_OTHER_ID_FUNCS = [
    "def remove_from_watchlist(",
    "def delete_alert_rule(",
    "def toggle_alert_rule(",
    "def mark_alert_read(",
    "def remove_holding(",
]
for _fn in _OTHER_ID_FUNCS:
    _sig_lines = [l for l in _main_source.splitlines() if _fn in l]
    _found = len(_sig_lines) > 0 and any("FPath" in l and "max_length=50" in l for l in _sig_lines)
    T(f"{_fn.strip('def (')}: ID param has FPath max_length=50", _found,
      f"sig: {_sig_lines[0][:80] if _sig_lines else 'NOT FOUND'}")


# ===================================================================
# Summary
# ===================================================================

print(f"\n{'=' * 60}")
print(f"S4 Checkpoint B+C — {_pass} passed, {_fail} failed out of {_pass + _fail}")
print(f"{'=' * 60}")

sys.exit(0 if _fail == 0 else 1)
