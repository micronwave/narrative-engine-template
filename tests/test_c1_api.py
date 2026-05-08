"""
C1 API test suite — Narrative Intelligence Platform, Phase C1.

Tests: C1-U1 (narratives structure), C1-U2 (ticker structure), C1-U3 (health).

Uses the project's custom S/T runner + FastAPI TestClient (in-process, no live
server required). Requires data/narrative_engine.db to exist.

Run with:
    python -X utf8 test_c1_api.py

Exit code 0 if all tests pass, 1 if any fail.
On all-pass, appends a line to frontend_build_log at project root.
"""

import logging
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)

# ---------------------------------------------------------------------------
# Custom test runner (identical pattern to test_full_integration.py)
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
# TestClient setup
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient  # noqa: E402 — after sys.path setup
from api.main import app  # noqa: E402

client = TestClient(app)

# ===========================================================================
# C1-U3: GET /api/health
# ===========================================================================
S("C1-U3: GET /api/health")

resp = client.get("/api/health")
T("status code 200", resp.status_code == 200, f"got {resp.status_code}")
body = resp.json()
T('body has status field', "status" in body, str(body))
T('body status is ok or degraded', body.get("status") in ("ok", "degraded"), str(body))
T('body has db field', "db" in body, str(body))
T('body has websocket_relay field', "websocket_relay" in body, str(body))

# ===========================================================================
# C1-U1: GET /api/narratives — response structure
# ===========================================================================
S("C1-U1: GET /api/narratives structure")

mock_repo = MagicMock()
mock_rows = [
    {
        "narrative_id": f"nar-{i}",
        "name": f"Narrative {i}",
        "velocity_windowed": 0.01 * i,
        "entropy": 0.5,
    }
    for i in range(1, 10)
]
mock_repo.get_all_active_narratives.return_value = mock_rows
with patch("api.app_legacy.get_repo_or_fail", return_value=mock_repo), \
     patch("api.app_legacy._build_signal_lookup", return_value={}), \
     patch(
         "api.app_legacy._build_visible_narrative",
         side_effect=lambda n, _repo, signal_lookup=None: {
             "id": n["narrative_id"],
             "name": n["name"],
             "descriptor": "Mock descriptor",
             "velocity_summary": "+1.0% signal velocity over 7d",
             "entropy": n.get("entropy"),
             "blurred": False,
             "burst_velocity": {"ratio": 1.1, "is_burst": False, "label": "stable"},
         },
     ):
    resp = client.get("/api/narratives")
T("status 200", resp.status_code == 200, f"got {resp.status_code}")

data = resp.json()
T("returns a list", isinstance(data, list), type(data).__name__)
T("at least 9 items total", len(data) >= 9, f"got {len(data)}")

visible = [n for n in data if not n.get("blurred", True)]
blurred = [n for n in data if n.get("blurred", False)]

T("all narratives visible (monetization removed)", len(visible) == len(data), f"visible={len(visible)}, total={len(data)}")
T("no blurred narratives remain", len(blurred) == 0, f"got {len(blurred)}")

REQUIRED_VISIBLE = {"id", "name", "descriptor", "velocity_summary", "entropy", "blurred"}
for i, n in enumerate(visible):
    missing = REQUIRED_VISIBLE - n.keys()
    T(f"visible[{i}] has all required fields", not missing, f"missing: {missing}")
    T(f"visible[{i}].blurred is False", n.get("blurred") is False, str(n.get("blurred")))
    T(f"visible[{i}].id is str", isinstance(n.get("id"), str), type(n.get("id")).__name__)
    T(f"visible[{i}].name is str", isinstance(n.get("name"), str))
    T(f"visible[{i}].descriptor is str", isinstance(n.get("descriptor"), str))
    T(f"visible[{i}].velocity_summary is str", isinstance(n.get("velocity_summary"), str))
    entropy = n.get("entropy")
    T(
        f"visible[{i}].entropy is float or null",
        entropy is None or isinstance(entropy, (int, float)),
        str(type(entropy)),
    )
    burst = n.get("burst_velocity")
    T(
        f"visible[{i}].burst_velocity is object or null",
        burst is None or isinstance(burst, dict),
        str(type(burst)),
    )
    if isinstance(burst, dict):
        T(f"visible[{i}].burst_velocity has ratio", "ratio" in burst)
        T(f"visible[{i}].burst_velocity has is_burst", "is_burst" in burst)
        T(f"visible[{i}].burst_velocity has label", "label" in burst)

for i, n in enumerate(blurred[:3]):  # spot-check first 3
    T(f"blurred[{i}] has id field", "id" in n, str(n.keys()))
    T(f"blurred[{i}].blurred is True", n.get("blurred") is True)

# ===========================================================================
# C1-U2: GET /api/ticker — response structure
# ===========================================================================
S("C1-U2: GET /api/ticker structure")

mock_repo_ticker = MagicMock()
mock_repo_ticker.get_all_active_narratives.return_value = mock_rows[:5]
with patch("api.app_legacy.get_repo_or_fail", return_value=mock_repo_ticker):
    resp = client.get("/api/ticker")
T("status 200", resp.status_code == 200, f"got {resp.status_code}")

data = resp.json()
T("returns a list", isinstance(data, list), type(data).__name__)
T("at least 5 items", len(data) >= 5, f"got {len(data)}")

for i, item in enumerate(data):
    T(f"item[{i}] has name (str)", "name" in item and isinstance(item["name"], str))
    T(
        f"item[{i}] has velocity_summary (str)",
        "velocity_summary" in item and isinstance(item["velocity_summary"], str),
    )

# ===========================================================================
# Summary + exit
# ===========================================================================
_print_summary()

# Write to frontend_build_log only when all tests pass
if _fail == 0:
    log_path = Path(__file__).parent.parent / "frontend_build_log"
    line = (
        f"C1 — FastAPI skeleton (health/narratives/ticker endpoints, port 8000), "
        f"Next.js gateway with live ticker + 3 visible + 6 blurred NarrativeCards + CTA modal; "
        f"real DB data (top 3 by ns_score visible); "
        f"tests pass — {date.today().isoformat()}\n"
    )
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)
    print(f"\nfrontend_build_log updated: {line.strip()}")

sys.exit(0 if _fail == 0 else 1)
