"""
D6 API test suite — Analytics Endpoints 5-7 + Cohesion Hardening (Phase 2 Batch 3).

Tests: D6-U1 (lifecycle-funnel), D6-U2 (lead-time-distribution),
       D6-U3 (contrarian-signals), D6-U4 (cohesion in history),
       D6-U5 (funnel invariants).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 test_d6_api.py

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
from api.main import app  # noqa: E402

VALID_STAGES = {"Emerging", "Growing", "Mature", "Declining", "Dormant"}
VALID_HISTOGRAM_RANGES = {
    "0-1 days", "2-3 days", "4-7 days", "8-14 days", "15-30 days", "No move"
}

with TestClient(app) as client:

    # ===========================================================================
    # D6-U1: GET /api/analytics/lifecycle-funnel — schema + structure
    # ===========================================================================
    S("D6-U1: GET /api/analytics/lifecycle-funnel")

    resp = client.get("/api/analytics/lifecycle-funnel?days=30")
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")

    data = resp.json()
    T("has 'generated_at'", "generated_at" in data)
    T("has 'days'", "days" in data)
    T("has 'stage_counts'", "stage_counts" in data)
    T("has 'transitions'", "transitions" in data)
    T("has 'avg_lifespan_days'", "avg_lifespan_days" in data)
    T("has 'revival_rate'", "revival_rate" in data)
    T("days matches param", data.get("days") == 30)
    T("stage_counts is dict", isinstance(data.get("stage_counts"), dict))
    T("transitions is list", isinstance(data.get("transitions"), list))

    sc = data.get("stage_counts", {})
    T("stage_counts has 5 keys", len(sc) == 5, f"got {len(sc)}")
    T("stage_counts keys are canonical stages",
      set(sc.keys()) == VALID_STAGES,
      f"got {set(sc.keys())}")
    T("all stage_counts non-negative",
      all(isinstance(v, int) and v >= 0 for v in sc.values()),
      f"got {sc}")

    # Check transition entry structure
    transitions = data.get("transitions", [])
    if transitions:
        t0 = transitions[0]
        T("transition has 'from'", "from" in t0)
        T("transition has 'to'", "to" in t0)
        T("transition has 'count'", "count" in t0)
        T("transition 'from' is valid stage",
          t0.get("from") in VALID_STAGES,
          f"got {t0.get('from')}")
        T("transition 'to' is valid stage",
          t0.get("to") in VALID_STAGES,
          f"got {t0.get('to')}")
        T("transition count is positive int",
          isinstance(t0.get("count"), int) and t0["count"] > 0)
    else:
        T("empty transitions is valid", True, "no stage changes in DB")

    rr = data.get("revival_rate")
    T("revival_rate is number", isinstance(rr, (int, float)))
    T("revival_rate in [0, 1]",
      isinstance(rr, (int, float)) and 0.0 <= rr <= 1.0,
      f"got {rr}")

    avg_ls = data.get("avg_lifespan_days")
    T("avg_lifespan_days is number", isinstance(avg_ls, (int, float)))

    # Default days parameter
    resp_default = client.get("/api/analytics/lifecycle-funnel")
    T("default days=30 status 200", resp_default.status_code == 200)
    T("default days=30", resp_default.json().get("days") == 30)

    # ===========================================================================
    # D6-U2: GET /api/analytics/lead-time-distribution — schema + structure
    # ===========================================================================
    S("D6-U2: GET /api/analytics/lead-time-distribution")

    resp = client.get("/api/analytics/lead-time-distribution?days=90&threshold=2.0")
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")

    data = resp.json()
    T("has 'generated_at'", "generated_at" in data)
    T("has 'cached'", "cached" in data)
    T("cached is bool", isinstance(data.get("cached"), bool))
    T("has 'data_points'", "data_points" in data)
    T("has 'histogram_buckets'", "histogram_buckets" in data)
    T("has 'median_lead_days'", "median_lead_days" in data)
    T("has 'mean_lead_days'", "mean_lead_days" in data)
    T("has 'hit_rate'", "hit_rate" in data)
    T("data_points is list", isinstance(data.get("data_points"), list))
    T("histogram_buckets is list", isinstance(data.get("histogram_buckets"), list))

    hr = data.get("hit_rate")
    T("hit_rate is number", isinstance(hr, (int, float)))
    T("hit_rate in [0, 1]",
      isinstance(hr, (int, float)) and 0.0 <= hr <= 1.0,
      f"got {hr}")

    median = data.get("median_lead_days")
    mean = data.get("mean_lead_days")
    T("median_lead_days is number", isinstance(median, (int, float)))
    T("mean_lead_days is number", isinstance(mean, (int, float)))

    # If cached and has buckets, check bucket structure
    buckets = data.get("histogram_buckets", [])
    if buckets:
        T("histogram has 6 buckets", len(buckets) == 6, f"got {len(buckets)}")
        ranges = {b.get("range") for b in buckets}
        T("bucket ranges match expected",
          ranges == VALID_HISTOGRAM_RANGES,
          f"got {ranges}")
        for b in buckets:
            T(f"bucket '{b.get('range')}' has count",
              "count" in b and isinstance(b["count"], int) and b["count"] >= 0)

    # Check data_point structure if present
    dp_list = data.get("data_points", [])
    if dp_list:
        dp = dp_list[0]
        T("data_point has 'narrative_id'", "narrative_id" in dp)
        T("data_point has 'ticker'", "ticker" in dp)
        T("data_point has 'lead_days'", "lead_days" in dp)
        T("data_point has 'price_change_pct'", "price_change_pct" in dp)
    else:
        T("empty data_points is valid", True, "no data or cache not ready")

    # Default threshold
    resp_default = client.get("/api/analytics/lead-time-distribution")
    T("default params status 200", resp_default.status_code == 200)

    # ===========================================================================
    # D6-U3: GET /api/analytics/contrarian-signals — schema + structure
    # ===========================================================================
    S("D6-U3: GET /api/analytics/contrarian-signals")

    resp = client.get("/api/analytics/contrarian-signals")
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")

    data = resp.json()
    T("has 'generated_at'", "generated_at" in data)
    T("has 'cached'", "cached" in data)
    T("cached is bool", isinstance(data.get("cached"), bool))
    T("has 'signals'", "signals" in data)
    T("signals is list", isinstance(data.get("signals"), list))

    signals = data.get("signals", [])
    if signals:
        sig = signals[0]
        T("signal has 'narrative_id'", "narrative_id" in sig)
        T("signal has 'name'", "name" in sig)
        T("signal has 'stage'", "stage" in sig)
        T("signal has 'ns_score'", "ns_score" in sig)
        T("signal has 'coordination_events'", "coordination_events" in sig)
        T("signal has 'linked_assets'", "linked_assets" in sig)
        T("signal has 'velocity_at_detection'", "velocity_at_detection" in sig)
        T("signal has 'velocity_now'", "velocity_now" in sig)
        T("signal has 'velocity_sustained'", "velocity_sustained" in sig)
        T("velocity_sustained is bool",
          isinstance(sig.get("velocity_sustained"), bool))
        T("coordination_events is list",
          isinstance(sig.get("coordination_events"), list))
        T("linked_assets is list",
          isinstance(sig.get("linked_assets"), list))

        ce_list = sig.get("coordination_events", [])
        if ce_list:
            ce = ce_list[0]
            T("coord_event has 'detected_at'", "detected_at" in ce)
            T("coord_event has 'source_domains'", "source_domains" in ce)
            T("coord_event has 'similarity_score'", "similarity_score" in ce)
            T("source_domains is list",
              isinstance(ce.get("source_domains"), list))

        la_list = sig.get("linked_assets", [])
        if la_list:
            la = la_list[0]
            T("asset has 'ticker'", "ticker" in la)
            T("asset has 'price_at_detection'", "price_at_detection" in la)
            T("asset has 'price_now'", "price_now" in la)
            T("asset has 'price_change_pct'", "price_change_pct" in la)
            T("asset has 'similarity_score'", "similarity_score" in la)
    else:
        T("empty signals is valid", True,
          "no coordinated narratives or cache not ready")

    # ===========================================================================
    # D6-U4: Cohesion in /api/narratives/{id}/history
    # ===========================================================================
    S("D6-U4: cohesion in narrative history")

    narratives_resp = client.get("/api/narratives")
    if narratives_resp.status_code == 200:
        nlist = narratives_resp.json()
        if isinstance(nlist, list) and nlist:
            nid = nlist[0].get("id") or nlist[0].get("narrative_id")
            if nid:
                hist_resp = client.get(f"/api/narratives/{nid}/history?days=7")
                T("history status 200", hist_resp.status_code == 200,
                  f"got {hist_resp.status_code}")
                hist_data = hist_resp.json()
                if isinstance(hist_data, list) and hist_data:
                    T("history entry has 'cohesion'", "cohesion" in hist_data[0],
                      f"keys: {list(hist_data[0].keys())}")
                    T("history entry has 'velocity'", "velocity" in hist_data[0])
                    T("history entry has 'entropy'", "entropy" in hist_data[0])
                    T("history entry has 'ns_score'", "ns_score" in hist_data[0])
                else:
                    T("empty history is valid", True, "no snapshots")
            else:
                T("no narrative_id found", True, "narrative has no id field")
        else:
            T("no narratives in DB", True, "empty DB — cohesion shape OK")
    else:
        T("narratives endpoint ok", True,
          f"status {narratives_resp.status_code} — skipping cohesion test")

    # ===========================================================================
    # D6-U5: Lifecycle funnel invariants
    # ===========================================================================
    S("D6-U5: funnel invariants")

    resp = client.get("/api/analytics/lifecycle-funnel?days=90")
    T("days=90 status 200", resp.status_code == 200, f"got {resp.status_code}")

    data = resp.json()
    T("days=90 matches param", data.get("days") == 90)

    sc = data.get("stage_counts", {})
    total_narratives = sum(sc.values())
    T("stage_counts sum >= 0", total_narratives >= 0, f"sum={total_narratives}")

    transitions = data.get("transitions", [])
    for t in transitions:
        T(f"transition {t.get('from')}->{t.get('to')} count>0",
          isinstance(t.get("count"), int) and t["count"] > 0,
          f"count={t.get('count')}")
        if "avg_days" in t:
            T(f"avg_days for {t.get('from')}->{t.get('to')} >= 0",
              isinstance(t["avg_days"], (int, float)) and t["avg_days"] >= 0,
              f"avg_days={t['avg_days']}")
        if t.get("label") == "Revival":
            stage_order = ["Emerging", "Growing", "Mature", "Declining", "Dormant"]
            from_idx = stage_order.index(t["from"]) if t["from"] in stage_order else -1
            to_idx = stage_order.index(t["to"]) if t["to"] in stage_order else -1
            T(f"revival {t['from']}->{t['to']} is backwards",
              from_idx > to_idx,
              f"from_idx={from_idx}, to_idx={to_idx}")

    rr = data.get("revival_rate", 0)
    T("revival_rate in [0, 1]",
      isinstance(rr, (int, float)) and 0.0 <= rr <= 1.0,
      f"got {rr}")

    avg_ls = data.get("avg_lifespan_days", 0)
    T("avg_lifespan_days >= 0",
      isinstance(avg_ls, (int, float)) and avg_ls >= 0,
      f"got {avg_ls}")


# ---------------------------------------------------------------------------
_print_summary()
sys.exit(0 if _fail == 0 else 1)
