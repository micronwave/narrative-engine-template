"""
Signal Redesign Phase 1 — LLM Structured Signal Extraction Tests

Section 1: Converter Functions (6 tests)
  SP1-DIR-1: direction_to_float correct for all 3 valid inputs
  SP1-DIR-2: direction_to_float returns 0.0 for unknown/invalid
  SP1-CER-1: certainty_to_float correct for all 4 valid inputs
  SP1-CER-2: certainty_to_float returns 0.2 for unknown/invalid
  SP1-MAG-1: magnitude_to_float correct for all 3 valid inputs
  SP1-MAG-2: magnitude_to_float returns 0.3 for unknown/invalid

Section 2: validate_signal_fields (8 tests)
  SP1-VAL-1: Complete valid input passes through unchanged
  SP1-VAL-2: Empty dict returns all-defaults dict
  SP1-VAL-3: Missing fields get safe defaults
  SP1-VAL-4: confidence outside [0,1] is clamped
  SP1-VAL-5: confidence as string is coerced to float
  SP1-VAL-6: Invalid direction defaults to neutral
  SP1-VAL-7: key_actors as non-list string is coerced
  SP1-VAL-8: List fields exceeding max count/length are truncated

Section 3: parse_signal_json (4 tests)
  SP1-PSJ-1: Clean SIGNAL_JSON line parsed correctly
  SP1-PSJ-2: JSON in multi-line NAME/DESC/SIGNAL response extracted
  SP1-PSJ-3: Malformed JSON returns fallback dict
  SP1-PSJ-4: Empty string returns fallback dict

Section 4: Repository (5 tests)
  SP1-REP-1: narrative_signals table created by migrate()
  SP1-REP-2: upsert inserts, get retrieves
  SP1-REP-3: upsert same ID updates (not duplicates)
  SP1-REP-4: get returns None for non-existent
  SP1-REP-5: get_all returns correct count

Section 5: Settings + Fallback (2 tests)
  SP1-SET-1: SIGNAL_EXTRACTION_STALENESS_HOURS exists, defaults 24
  SP1-SET-2: _HAIKU_FALLBACKS["extract_signal"] valid JSON with all 8 keys
"""

import json
import os
import sys
import tempfile
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from signals import (
    direction_to_float,
    certainty_to_float,
    magnitude_to_float,
    validate_signal_fields,
)
from llm_client import parse_signal_json, _HAIKU_FALLBACKS
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


# ===========================================================================
# Section 1: Converter Functions
# ===========================================================================
S("SP1-DIR: direction_to_float")

T("SP1-DIR-1: correct for all 3 valid inputs",
  direction_to_float("bullish") == 1.0
  and direction_to_float("bearish") == -1.0
  and direction_to_float("neutral") == 0.0,
  f"bullish={direction_to_float('bullish')}, bearish={direction_to_float('bearish')}, neutral={direction_to_float('neutral')}")

T("SP1-DIR-2: unknown/invalid returns 0.0",
  direction_to_float("UNKNOWN") == 0.0
  and direction_to_float("") == 0.0
  and direction_to_float("sideways") == 0.0,
  f"UNKNOWN={direction_to_float('UNKNOWN')}, empty={direction_to_float('')}")

S("SP1-CER: certainty_to_float")

T("SP1-CER-1: correct for all 4 valid inputs",
  certainty_to_float("speculative") == 0.2
  and certainty_to_float("rumored") == 0.4
  and certainty_to_float("expected") == 0.7
  and certainty_to_float("confirmed") == 1.0,
  f"spec={certainty_to_float('speculative')}, rum={certainty_to_float('rumored')}, exp={certainty_to_float('expected')}, conf={certainty_to_float('confirmed')}")

T("SP1-CER-2: unknown returns 0.2",
  certainty_to_float("maybe") == 0.2
  and certainty_to_float("") == 0.2,
  f"maybe={certainty_to_float('maybe')}")

S("SP1-MAG: magnitude_to_float")

T("SP1-MAG-1: correct for all 3 valid inputs",
  magnitude_to_float("incremental") == 0.3
  and magnitude_to_float("significant") == 0.6
  and magnitude_to_float("transformative") == 1.0,
  f"inc={magnitude_to_float('incremental')}, sig={magnitude_to_float('significant')}, trans={magnitude_to_float('transformative')}")

T("SP1-MAG-2: unknown returns 0.3",
  magnitude_to_float("tiny") == 0.3
  and magnitude_to_float("") == 0.3,
  f"tiny={magnitude_to_float('tiny')}")

# ===========================================================================
# Section 2: validate_signal_fields
# ===========================================================================
S("SP1-VAL: validate_signal_fields")

valid_input = {
    "direction": "bearish",
    "confidence": 0.82,
    "timeframe": "near_term",
    "magnitude": "significant",
    "certainty": "expected",
    "key_actors": ["USTR", "EU Commission"],
    "affected_sectors": ["automotive", "semiconductor"],
    "catalyst_type": "regulatory",
}
result = validate_signal_fields(valid_input)
T("SP1-VAL-1: complete valid input passes through",
  result["direction"] == "bearish"
  and result["confidence"] == 0.82
  and result["timeframe"] == "near_term"
  and result["magnitude"] == "significant"
  and result["certainty"] == "expected"
  and result["key_actors"] == ["USTR", "EU Commission"]
  and result["affected_sectors"] == ["automotive", "semiconductor"]
  and result["catalyst_type"] == "regulatory",
  f"got={result}")

empty_result = validate_signal_fields({})
T("SP1-VAL-2: empty dict returns all-defaults",
  empty_result["direction"] == "neutral"
  and empty_result["confidence"] == 0.0
  and empty_result["timeframe"] == "unknown"
  and empty_result["magnitude"] == "incremental"
  and empty_result["certainty"] == "speculative"
  and empty_result["key_actors"] == []
  and empty_result["affected_sectors"] == []
  and empty_result["catalyst_type"] == "unknown",
  f"got={empty_result}")

partial = validate_signal_fields({"direction": "bullish"})
T("SP1-VAL-3: missing fields get safe defaults",
  partial["direction"] == "bullish"
  and partial["confidence"] == 0.0
  and partial["timeframe"] == "unknown"
  and partial["catalyst_type"] == "unknown",
  f"got={partial}")

clamped = validate_signal_fields({"confidence": 1.5})
clamped_low = validate_signal_fields({"confidence": -0.3})
T("SP1-VAL-4: confidence outside [0,1] is clamped",
  clamped["confidence"] == 1.0 and clamped_low["confidence"] == 0.0,
  f"1.5->{clamped['confidence']}, -0.3->{clamped_low['confidence']}")

str_conf = validate_signal_fields({"confidence": "0.75"})
T("SP1-VAL-5: confidence as string coerced to float",
  str_conf["confidence"] == 0.75,
  f"'0.75'->{str_conf['confidence']}")

bad_dir = validate_signal_fields({"direction": "sideways"})
T("SP1-VAL-6: invalid direction defaults to neutral",
  bad_dir["direction"] == "neutral",
  f"sideways->{bad_dir['direction']}")

str_actors = validate_signal_fields({"key_actors": "Federal Reserve"})
T("SP1-VAL-7: key_actors as string is coerced to list",
  isinstance(str_actors["key_actors"], list)
  and len(str_actors["key_actors"]) == 1
  and str_actors["key_actors"][0] == "Federal Reserve",
  f"got={str_actors['key_actors']}")

long_list = validate_signal_fields({
    "key_actors": [f"actor_{i}" for i in range(20)],
    "affected_sectors": [f"sector_{i}" for i in range(10)],
})
T("SP1-VAL-8: list fields truncated to max count",
  len(long_list["key_actors"]) == 10
  and len(long_list["affected_sectors"]) == 5,
  f"actors={len(long_list['key_actors'])}, sectors={len(long_list['affected_sectors'])}")

# ===========================================================================
# Section 3: parse_signal_json
# ===========================================================================
S("SP1-PSJ: parse_signal_json")

clean_json = '{"direction":"bearish","confidence":0.82,"timeframe":"near_term","magnitude":"significant","certainty":"expected","key_actors":["USTR"],"affected_sectors":["auto"],"catalyst_type":"regulatory"}'
parsed1 = parse_signal_json(f"SIGNAL_JSON: {clean_json}")
T("SP1-PSJ-1: clean SIGNAL_JSON line parsed",
  parsed1["direction"] == "bearish" and parsed1["confidence"] == 0.82,
  f"direction={parsed1.get('direction')}, confidence={parsed1.get('confidence')}")

multi_line = (
    "NAME: Tariff Escalation Fears\n"
    "DESCRIPTION: Rising trade tensions between US and EU threaten automotive sector.\n"
    f"SIGNAL_JSON: {clean_json}"
)
parsed2 = parse_signal_json(multi_line)
T("SP1-PSJ-2: multi-line NAME/DESC/SIGNAL extracted",
  parsed2["direction"] == "bearish" and parsed2["catalyst_type"] == "regulatory",
  f"direction={parsed2.get('direction')}")

parsed3 = parse_signal_json("This is not JSON at all {broken")
T("SP1-PSJ-3: malformed JSON returns fallback",
  parsed3["direction"] == "neutral" and parsed3["confidence"] == 0.0,
  f"direction={parsed3.get('direction')}")

parsed4 = parse_signal_json("")
T("SP1-PSJ-4: empty string returns fallback",
  parsed4["direction"] == "neutral" and parsed4["confidence"] == 0.0,
  f"direction={parsed4.get('direction')}")

# ===========================================================================
# Section 4: Repository — narrative_signals table
# ===========================================================================
S("SP1-REP: narrative_signals repository")

# Create temp DB for testing
_tmp_fd, _tmp_path = tempfile.mkstemp(suffix=".db")
os.close(_tmp_fd)
try:
    repo = SqliteRepository(_tmp_path)
    repo.migrate()

    # Test 1: table exists
    import sqlite3
    conn = sqlite3.connect(_tmp_path)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    conn.close()
    T("SP1-REP-1: narrative_signals table created by migrate()",
      "narrative_signals" in tables,
      f"tables={tables}")

    # Test 2: upsert inserts, get retrieves
    now_iso = "2026-03-24T12:00:00+00:00"
    test_signal = {
        "narrative_id": "test-nar-001",
        "direction": "bearish",
        "confidence": 0.85,
        "timeframe": "near_term",
        "magnitude": "significant",
        "certainty": "expected",
        "key_actors": ["USTR", "EU Commission"],
        "affected_sectors": ["automotive"],
        "catalyst_type": "regulatory",
        "extracted_at": now_iso,
        "raw_response": "test raw response",
    }
    repo.upsert_narrative_signal(test_signal)
    retrieved = repo.get_narrative_signal("test-nar-001")
    T("SP1-REP-2: upsert inserts and get retrieves",
      retrieved is not None
      and retrieved["direction"] == "bearish"
      and retrieved["confidence"] == 0.85
      and retrieved["catalyst_type"] == "regulatory"
      and retrieved["extracted_at"] == now_iso,
      f"got={retrieved}")

    # Test 3: upsert same ID updates (not duplicates)
    updated_signal = dict(test_signal)
    updated_signal["direction"] = "bullish"
    updated_signal["confidence"] = 0.95
    repo.upsert_narrative_signal(updated_signal)
    updated = repo.get_narrative_signal("test-nar-001")
    all_signals = repo.get_all_narrative_signals()
    T("SP1-REP-3: upsert same ID updates not duplicates",
      updated["direction"] == "bullish"
      and updated["confidence"] == 0.95
      and len([s for s in all_signals if s["narrative_id"] == "test-nar-001"]) == 1,
      f"direction={updated['direction']}, count={len([s for s in all_signals if s['narrative_id'] == 'test-nar-001'])}")

    # Test 4: get returns None for non-existent
    missing = repo.get_narrative_signal("does-not-exist")
    T("SP1-REP-4: get returns None for non-existent",
      missing is None,
      f"got={missing}")

    # Test 5: get_all returns correct count
    second_signal = dict(test_signal)
    second_signal["narrative_id"] = "test-nar-002"
    repo.upsert_narrative_signal(second_signal)
    all_after = repo.get_all_narrative_signals()
    T("SP1-REP-5: get_all returns correct count",
      len(all_after) == 2,
      f"count={len(all_after)}")

finally:
    try:
        os.unlink(_tmp_path)
    except OSError:
        pass

# ===========================================================================
# Section 5: Settings + Fallback
# ===========================================================================
S("SP1-SET: Settings and fallback")

from settings import Settings
_test_settings = Settings(_env_file=None, ANTHROPIC_API_KEY="sk-test-dummy-key")
T("SP1-SET-1: SIGNAL_EXTRACTION_STALENESS_HOURS exists and defaults to 24",
  hasattr(_test_settings, "SIGNAL_EXTRACTION_STALENESS_HOURS")
  and _test_settings.SIGNAL_EXTRACTION_STALENESS_HOURS == 24,
  f"value={getattr(_test_settings, 'SIGNAL_EXTRACTION_STALENESS_HOURS', 'MISSING')}")

fallback_str = _HAIKU_FALLBACKS.get("extract_signal")
fb_ok = False
fb_keys = set()
if fallback_str:
    try:
        fb = json.loads(fallback_str)
        fb_keys = set(fb.keys())
        expected_keys = {"direction", "confidence", "timeframe", "magnitude",
                         "certainty", "key_actors", "affected_sectors", "catalyst_type"}
        fb_ok = fb_keys == expected_keys
    except json.JSONDecodeError:
        pass
T("SP1-SET-2: extract_signal fallback is valid JSON with all 8 keys",
  fb_ok,
  f"keys={fb_keys}")

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 60)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  FAIL: {name}")
    sys.exit(1)
else:
    print("All Phase 1 signal extraction tests passed.")
