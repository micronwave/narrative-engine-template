"""
D1 API test suite — Narrative Intelligence Platform, Phase D1.

Tests: D1-U1 through D1-I1 (asset classes, securities, narrative assets).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 test_d1_api.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import logging
import sys
from pathlib import Path

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
        print(
            f"  FAIL [{_current_section}] {name}" + (f" — {details}" if details else ""),
            file=sys.stderr,
        )


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
    print(f"{'Section':<35} {'Pass':>5} {'Fail':>5}")
    print("-" * 60)
    for sec, counts in sections.items():
        marker = "" if counts["fail"] == 0 else " <--"
        print(f"  {sec:<33} {counts['pass']:>5} {counts['fail']:>5}{marker}")
    print("=" * 60)
    print(f"  TOTAL: {_pass} passed, {_fail} failed out of {_pass + _fail} tests")
    print("=" * 60)


# ---------------------------------------------------------------------------
# TestClient + imports
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app, ASSET_CLASSES, TRACKED_SECURITIES, NARRATIVE_ASSETS  # noqa: E402

VALID_ASSET_CLASS_TYPES = {"sector", "commodity", "currency", "index", "crypto"}
VALID_DIRECTIONS = {"bullish", "bearish", "mixed", "uncertain"}

# Use context manager so @app.on_event("startup") fires, updating NARRATIVE_ASSETS
# with real DB narrative IDs if available.
with TestClient(app) as client:

    # ===========================================================================
    # D1-U1: GET /api/asset-classes returns 6+ items with required fields
    # ===========================================================================
    S("D1-U1: GET /api/asset-classes — schema + count")

    resp = client.get("/api/asset-classes")
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")

    data = resp.json()
    T("returns list", isinstance(data, list), type(data).__name__)
    T("at least 6 items", len(data) >= 6, f"got {len(data)}")

    if data:
        item = data[0]
        T("has id", "id" in item, str(item))
        T("has name", "name" in item, str(item))
        T("has type", "type" in item, str(item))
        T("has description", "description" in item, str(item))

    # ===========================================================================
    # D1-U2: All type values are valid enum members
    # ===========================================================================
    S("D1-U2: GET /api/asset-classes — valid type enum values")

    types = [ac.get("type") for ac in data]
    T("all types are valid", all(t in VALID_ASSET_CLASS_TYPES for t in types),
      f"invalid types: {[t for t in types if t not in VALID_ASSET_CLASS_TYPES]}")

    # ===========================================================================
    # D1-U3: GET /api/securities — schema + count
    # ===========================================================================
    S("D1-U3: GET /api/securities — schema + count")

    resp = client.get("/api/securities")
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")

    sec_data = resp.json()
    T("returns list", isinstance(sec_data, list), type(sec_data).__name__)
    T("at least 15 items", len(sec_data) >= 15, f"got {len(sec_data)}")

    if sec_data:
        s = sec_data[0]
        T("has id", "id" in s, str(s))
        T("has symbol", "symbol" in s, str(s))
        T("has name", "name" in s, str(s))
        T("has asset_class_id", "asset_class_id" in s, str(s))
        T("has exchange", "exchange" in s, str(s))

    # ===========================================================================
    # D1-U4: current_price and price_change_24h are null (not yet populated)
    # ===========================================================================
    S("D1-U4: GET /api/securities — prices are null")

    T("current_price fields are null",
      all(s.get("current_price") is None for s in sec_data),
      "some prices unexpectedly populated")
    T("price_change_24h fields are null",
      all(s.get("price_change_24h") is None for s in sec_data),
      "some changes unexpectedly populated")

    # ===========================================================================
    # D1-U5: GET /api/narratives/{id}/assets — full response shape
    # ===========================================================================
    S("D1-U5: GET /api/narratives/{id}/assets — full schema")

    # Use whatever narrative_id was set in NARRATIVE_ASSETS[0] after startup
    test_narrative_id = NARRATIVE_ASSETS[0]["narrative_id"]
    resp = client.get(f"/api/narratives/{test_narrative_id}/assets")
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")

    assets_data = resp.json()
    T("returns list", isinstance(assets_data, list), type(assets_data).__name__)
    T("at least 1 association", len(assets_data) >= 1, f"got {len(assets_data)} for id={test_narrative_id}")

    if assets_data:
        na = assets_data[0]
        T("has exposure_score", "exposure_score" in na, str(list(na.keys())))
        T("has direction", "direction" in na, str(list(na.keys())))
        T("has rationale", "rationale" in na, str(list(na.keys())))
        T("has asset_class nested object", isinstance(na.get("asset_class"), dict),
          f"got {type(na.get('asset_class'))}")
        T("has securities list", isinstance(na.get("securities"), list),
          f"got {type(na.get('securities'))}")

    # ===========================================================================
    # D1-U6: Nested asset_class has all required fields
    # ===========================================================================
    S("D1-U6: GET /api/narratives/{id}/assets — nested asset_class schema")

    if assets_data:
        ac = assets_data[0].get("asset_class", {})
        T("asset_class has id", "id" in ac, str(ac))
        T("asset_class has name", "name" in ac, str(ac))
        T("asset_class has type", "type" in ac, str(ac))
        T("asset_class has description", "description" in ac, str(ac))
        T("asset_class type is valid enum",
          ac.get("type") in VALID_ASSET_CLASS_TYPES,
          f"got '{ac.get('type')}'")

    # ===========================================================================
    # D1-U7: Securities in assets have null price fields
    # ===========================================================================
    S("D1-U7: GET /api/narratives/{id}/assets — securities schema + null prices")

    if assets_data:
        securities_in_na = assets_data[0].get("securities", [])
        T("securities list is present", isinstance(securities_in_na, list),
          f"got {type(securities_in_na)}")
        if securities_in_na:
            sec = securities_in_na[0]
            T("security has symbol", "symbol" in sec, str(sec))
            T("security has name", "name" in sec, str(sec))
            T("security current_price is null", sec.get("current_price") is None,
              f"got {sec.get('current_price')}")
            T("security price_change_24h is null", sec.get("price_change_24h") is None,
              f"got {sec.get('price_change_24h')}")

    # ===========================================================================
    # D1-U8: GET /api/narratives/{id} includes "assets" field
    # ===========================================================================
    S("D1-U8: GET /api/narratives/{id} — includes assets field")

    resp = client.get(f"/api/narratives/{test_narrative_id}")
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")

    detail = resp.json()
    T("response has assets field", "assets" in detail,
      f"keys: {list(detail.keys())}")
    T("assets is a list", isinstance(detail.get("assets"), list),
      f"got {type(detail.get('assets'))}")

    # ===========================================================================
    # D1-I1: GET /api/narratives/{id}/assets for unknown ID returns []
    # ===========================================================================
    S("D1-I1: GET /api/narratives/{id}/assets — unknown ID returns 404")

    resp = client.get("/api/narratives/does-not-exist-xyz999/assets")
    T("status 404", resp.status_code == 404, f"got {resp.status_code}")


# ===========================================================================
# Summary + exit
# ===========================================================================
_print_summary()

passed = _fail == 0

sys.exit(0 if passed else 1)
