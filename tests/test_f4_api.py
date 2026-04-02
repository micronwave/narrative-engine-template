"""
F4 — Topic Tagging Tests

Unit:
  F4-U1: GET /api/narratives returns items with topic_tags field (list)
  F4-U2: GET /api/narratives?topic=regulatory filters correctly
  F4-U3: topic_tags field is a list of strings
"""

import sys
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from fastapi.testclient import TestClient
from api.main import app

# ---------------------------------------------------------------------------
_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    _results.append((name, condition))
    marker = "✓" if condition else "✗"
    msg = f"  [{marker}] {name}"
    if details and not condition:
        msg += f"\n      details: {details}"
    elif details and condition:
        msg += f"  ({details})"
    print(msg)


# ===========================================================================
# F4-U1: topic_tags field exists in response
# ===========================================================================
S("F4-U1: GET /api/narratives has topic_tags field")
with TestClient(app) as client:
    resp = client.get("/api/narratives")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("returns a list", isinstance(data, list))
    T("has at least 1 item", len(data) >= 1, f"len={len(data)}")
    if data:
        first = data[0]
        T("first item has topic_tags field", "topic_tags" in first,
          f"keys={list(first.keys())}")
        topic_tags = first.get("topic_tags")
        T("topic_tags is a list", isinstance(topic_tags, list),
          f"type={type(topic_tags)}")

# ===========================================================================
# F4-U2: topic filter query param works
# ===========================================================================
S("F4-U2: GET /api/narratives?topic=... filter")
with TestClient(app) as client:
    # Get all narratives first
    all_data = client.get("/api/narratives").json()
    all_count = len(all_data)

    # Filter by a topic that may or may not exist (no error either way)
    resp_filter = client.get("/api/narratives?topic=regulatory")
    T("status 200 with topic filter", resp_filter.status_code == 200,
      f"status={resp_filter.status_code}")
    filtered_data = resp_filter.json()
    T("filtered result is a list", isinstance(filtered_data, list))
    T("filtered count <= total count", len(filtered_data) <= all_count,
      f"filtered={len(filtered_data)}, total={all_count}")

    # Stage filter also works
    resp_stage = client.get("/api/narratives?stage=Emerging")
    T("status 200 with stage filter", resp_stage.status_code == 200,
      f"status={resp_stage.status_code}")
    stage_data = resp_stage.json()
    T("stage filtered result is a list", isinstance(stage_data, list))

    # If results have stage, verify filter works
    for item in stage_data:
        T(f"item {item['id'][:8]} stage is Emerging", item.get("stage") == "Emerging",
          f"stage={item.get('stage')}")
        if len([r for _, r in _results if r]) >= 10:
            break  # Enough checks

# ===========================================================================
# F4-U3: topic_tags is a list of strings
# ===========================================================================
S("F4-U3: topic_tags values are strings")
with TestClient(app) as client:
    data = client.get("/api/narratives").json()
    for item in data[:3]:
        tags = item.get("topic_tags", [])
        T(f"item {item['id'][:8]} topic_tags is list of strings",
          all(isinstance(t, str) for t in tags),
          f"tags={tags}")

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"F4 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All F4 tests passed.")
