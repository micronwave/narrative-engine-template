"""
Extension Module Audit Tests

Tests the fixes from the production code audit of:
portfolio.py, watchlist.py, notifications.py, export.py

C1: Notification threshold None guard
C2: Portfolio ns_score None guard
H1: Notification deduplication
H2: Notification UTC date
H3: CSV import row cap
H4: Notification target_type validation
M2: Portfolio remove_holding updates timestamp
M3: Export share_text fallback
M4: Watchlist duplicate prevention
M5: Dashboard user_id explicit
"""

import json
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    _results.append((name, condition))
    marker = "\u2713" if condition else "\u2717"
    msg = f"  [{marker}] {name}"
    if details:
        msg += f"  ({details})"
    print(msg)


# ---------------------------------------------------------------------------
# Mock repository that stores data in-memory
# ---------------------------------------------------------------------------
class MockRepository:
    def __init__(self):
        self.portfolios = {}
        self.holdings = {}
        self.watchlists = {}
        self.watchlist_items = {}
        self.notification_rules = {}
        self.notifications = {}
        self.narratives = {}
        self.mutations = []

    # --- Portfolio ---
    def get_portfolio_by_user(self, user_id):
        for p in self.portfolios.values():
            if p["user_id"] == user_id:
                return p
        return None

    def create_portfolio(self, portfolio):
        self.portfolios[portfolio["id"]] = portfolio

    def add_portfolio_holding(self, holding):
        self.holdings[holding["id"]] = holding

    def get_portfolio_holding(self, holding_id):
        h = self.holdings.get(holding_id)
        if h:
            p = self.portfolios.get(h["portfolio_id"])
            return {**h, "user_id": p["user_id"] if p else "local"}
        return None

    def delete_portfolio_holding(self, holding_id):
        self.holdings.pop(holding_id, None)

    def get_portfolio_holdings(self, portfolio_id):
        return [h for h in self.holdings.values() if h["portfolio_id"] == portfolio_id]

    def update_portfolio_timestamp(self, portfolio_id):
        if portfolio_id in self.portfolios:
            self.portfolios[portfolio_id]["updated_at"] = datetime.now(timezone.utc).isoformat()

    # --- Narratives ---
    def get_narrative(self, narrative_id):
        return self.narratives.get(narrative_id)

    def get_narratives_for_ticker(self, ticker):
        results = []
        for n in self.narratives.values():
            linked = n.get("linked_assets", [])
            if isinstance(linked, str):
                try:
                    linked = json.loads(linked)
                except Exception:
                    linked = []
            if ticker.upper() in linked:
                results.append({
                    "narrative_id": n["narrative_id"],
                    "name": n["name"],
                    "ns_score": n.get("ns_score"),
                    "stage": n.get("stage"),
                })
        return results

    # --- Watchlist ---
    def create_watchlist(self, watchlist):
        self.watchlists[watchlist["id"]] = watchlist

    def get_watchlist(self, watchlist_id):
        return self.watchlists.get(watchlist_id)

    def list_watchlists(self, user_id):
        return [w for w in self.watchlists.values() if w["user_id"] == user_id]

    def add_watchlist_item(self, item):
        self.watchlist_items[item["id"]] = item

    def delete_watchlist_item(self, item_id):
        self.watchlist_items.pop(item_id, None)

    def get_watchlist_items(self, watchlist_id):
        return [i for i in self.watchlist_items.values() if i["watchlist_id"] == watchlist_id]

    # --- Notifications ---
    def create_notification_rule(self, rule):
        self.notification_rules[rule["id"]] = rule

    def get_enabled_notification_rules(self):
        return [r for r in self.notification_rules.values() if r.get("enabled")]

    def list_notification_rules(self, user_id):
        return [r for r in self.notification_rules.values() if r["user_id"] == user_id]

    def update_notification_rule_enabled(self, rule_id, enabled):
        if rule_id in self.notification_rules:
            self.notification_rules[rule_id]["enabled"] = 1 if enabled else 0

    def delete_notification_rule(self, rule_id):
        self.notification_rules.pop(rule_id, None)

    def create_notification(self, notification):
        self.notifications[notification["id"]] = notification

    def get_notifications(self, user_id, unread_only=False):
        results = [n for n in self.notifications.values() if n["user_id"] == user_id]
        if unread_only:
            results = [n for n in results if not n.get("is_read")]
        return sorted(results, key=lambda x: x["created_at"], reverse=True)

    def mark_notification_read(self, notification_id, user_id=None):
        if notification_id in self.notifications:
            self.notifications[notification_id]["is_read"] = 1

    def mark_all_notifications_read(self, user_id):
        for n in self.notifications.values():
            if n["user_id"] == user_id:
                n["is_read"] = 1

    def has_notification_today(self, rule_id):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for n in self.notifications.values():
            if n.get("rule_id") == rule_id and n.get("created_at", "").startswith(today):
                return True
        return False

    def get_narratives_created_on_date(self, date_str):
        results = []
        for n in self.narratives.values():
            if n.get("created_at", "").startswith(date_str):
                results.append(n)
        return results

    def get_mutations_today_for_narrative(self, narrative_id):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return [m for m in self.mutations
                if m["narrative_id"] == narrative_id
                and m.get("detected_at", "").startswith(today)]

    # --- Export ---
    def get_narratives_by_date(self, date_str):
        return list(self.narratives.values())


class MockStockProvider:
    def get_quotes_batch(self, tickers, force_refresh=False):
        return {t: {"price": 150.0, "change_pct": 1.5} for t in tickers}


class MockLlmClient:
    def __init__(self, fail=False):
        self.fail = fail

    def call_haiku(self, task_type, narrative_id, prompt):
        if self.fail:
            return "Analysis unavailable"
        return "Mock LLM response for " + task_type

    def call_haiku_chat(self, system_prompt, messages):
        return {"content": "Mock chat response", "tokens": 100, "cost": 0.001}


# ===========================================================================
# Tests
# ===========================================================================

def test_notifications():
    S("C1 — Notification threshold None guard")

    from notifications import NotificationManager, VALID_TARGET_TYPES

    repo = MockRepository()
    nm = NotificationManager(repo)

    # Create a narrative
    nid = str(uuid.uuid4())
    repo.narratives[nid] = {
        "narrative_id": nid, "name": "Test", "ns_score": 0.75,
        "stage": "Growing", "linked_assets": "[]", "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Create rule with None threshold
    rule_id = nm.create_rule("local", "ns_above", "narrative", nid, threshold=None)
    T("C1-a: Rule with None threshold created OK", bool(rule_id))

    # check_rules should NOT crash
    try:
        triggered = nm.check_rules()
        T("C1-b: check_rules with None threshold does not crash", True)
        T("C1-c: None threshold does not trigger", len(triggered) == 0,
          f"triggered={len(triggered)}")
    except TypeError as e:
        T("C1-b: check_rules with None threshold does not crash", False, str(e))
        T("C1-c: None threshold does not trigger", False)

    # Create rule with valid threshold that should trigger
    rule_id2 = nm.create_rule("local", "ns_above", "narrative", nid, threshold=0.5)
    triggered = nm.check_rules()
    T("C1-d: Valid threshold triggers correctly", len(triggered) == 1,
      f"triggered={len(triggered)}")

    S("H1 — Notification deduplication")

    # Same rule should NOT fire again today
    triggered2 = nm.check_rules()
    T("H1-a: Same rule does not fire twice in one day", len(triggered2) == 0,
      f"triggered={len(triggered2)}")

    S("H2 — UTC date for new_narrative")

    nid2 = str(uuid.uuid4())
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    repo.narratives[nid2] = {
        "narrative_id": nid2, "name": "New Today", "ns_score": 0.5,
        "stage": "Emerging", "linked_assets": '["AAPL"]',
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    rule_id3 = nm.create_rule("local", "new_narrative", "ticker", "AAPL")
    triggered3 = nm.check_rules()
    T("H2-a: new_narrative rule finds today's narrative (UTC)", len(triggered3) == 1,
      f"triggered={len(triggered3)}")

    S("H4 — target_type validation")

    try:
        nm.create_rule("local", "ns_above", "invalid_type", nid)
        T("H4-a: Invalid target_type raises ValueError", False)
    except ValueError as e:
        T("H4-a: Invalid target_type raises ValueError", True, str(e))

    T("H4-b: VALID_TARGET_TYPES exported correctly",
      VALID_TARGET_TYPES == {"narrative", "ticker", "portfolio"})

    S("P09b — Notification rule lifecycle + read state")

    lifecycle_rule = nm.create_rule("local", "ns_above", "narrative", nid, threshold=0.4)
    T("Lifecycle-a: create_rule returns id", bool(lifecycle_rule))
    nm.toggle_rule(lifecycle_rule, False)
    rule_after_disable = repo.notification_rules.get(lifecycle_rule, {})
    T("Lifecycle-b: toggle_rule disables rule",
      int(rule_after_disable.get("enabled", 1)) == 0,
      f"rule={rule_after_disable}")
    nm.toggle_rule(lifecycle_rule, True)
    rule_after_enable = repo.notification_rules.get(lifecycle_rule, {})
    T("Lifecycle-c: toggle_rule enables rule",
      int(rule_after_enable.get("enabled", 0)) == 1,
      f"rule={rule_after_enable}")
    nm.delete_rule(lifecycle_rule)
    T("Lifecycle-d: delete_rule removes rule",
      lifecycle_rule not in repo.notification_rules)

    manual_n1 = {
        "id": str(uuid.uuid4()),
        "user_id": "local",
        "rule_id": "manual",
        "title": "Manual 1",
        "message": "m1",
        "link": "/x",
        "is_read": 0,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manual_n2 = {
        "id": str(uuid.uuid4()),
        "user_id": "local",
        "rule_id": "manual",
        "title": "Manual 2",
        "message": "m2",
        "link": "/y",
        "is_read": 0,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    repo.create_notification(manual_n1)
    repo.create_notification(manual_n2)
    unread_before = nm.get_notifications("local", unread_only=True)
    T("Lifecycle-e: unread_only returns unread notifications",
      len(unread_before) >= 2, f"count={len(unread_before)}")
    nm.mark_read(manual_n1["id"])
    unread_after_one = nm.get_notifications("local", unread_only=True)
    T("Lifecycle-f: mark_read reduces unread count",
      len(unread_after_one) == len(unread_before) - 1,
      f"before={len(unread_before)}, after={len(unread_after_one)}")
    nm.mark_all_read("local")
    unread_after_all = nm.get_notifications("local", unread_only=True)
    T("Lifecycle-g: mark_all_read clears unread notifications",
      len(unread_after_all) == 0, f"count={len(unread_after_all)}")

    S("L3 — new_narrative collects all matches")

    nid3 = str(uuid.uuid4())
    repo.narratives[nid3] = {
        "narrative_id": nid3, "name": "Also New Today", "ns_score": 0.3,
        "stage": "Emerging", "linked_assets": '["MSFT"]',
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    nid4 = str(uuid.uuid4())
    repo.narratives[nid4] = {
        "narrative_id": nid4, "name": "Third New", "ns_score": 0.4,
        "stage": "Emerging", "linked_assets": '["MSFT"]',
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    rule_id_msft = nm.create_rule("local", "new_narrative", "ticker", "MSFT")
    triggered_msft = nm.check_rules()
    T("L3-a: new_narrative returns multiple matches",
      len(triggered_msft) == 2,
      f"triggered={len(triggered_msft)}")

    # Also test ns_below with None ns_score from DB
    nid5 = str(uuid.uuid4())
    repo.narratives[nid5] = {
        "narrative_id": nid5, "name": "No Score", "ns_score": None,
        "stage": None, "linked_assets": "[]",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    rule_id5 = nm.create_rule("local", "ns_below", "narrative", nid5, threshold=0.5)
    try:
        triggered5 = nm.check_rules()
        T("C1-e: ns_below with None ns_score does not crash", True)
    except TypeError as e:
        T("C1-e: ns_below with None ns_score does not crash", False, str(e))


def test_portfolio():
    S("C2 — Portfolio ns_score None guard")

    from portfolio import PortfolioManager, MAX_IMPORT_ROWS

    repo = MockRepository()
    stock = MockStockProvider()
    pm = PortfolioManager(repo, stock)

    pid = pm.get_or_create_portfolio()

    # Add holding
    pm.add_holding(pid, "AAPL", 10)

    # Create narrative with None ns_score linked to AAPL
    nid = str(uuid.uuid4())
    repo.narratives[nid] = {
        "narrative_id": nid, "name": "Null Score", "ns_score": None,
        "stage": None, "linked_assets": '["AAPL"]',
    }

    try:
        impact = pm.calculate_impact(pid)
        T("C2-a: calculate_impact with None ns_score does not crash", True)
        touching = impact["narratives_touching"]
        if touching:
            T("C2-b: ns_score defaults to 0", touching[0]["ns_score"] == 0,
              f"ns_score={touching[0]['ns_score']}")
            T("C2-c: stage defaults to Emerging", touching[0]["stage"] == "Emerging",
              f"stage={touching[0]['stage']}")
        else:
            T("C2-b: ns_score defaults to 0", False, "no narratives found")
            T("C2-c: stage defaults to Emerging", False)
    except TypeError as e:
        T("C2-a: calculate_impact with None ns_score does not crash", False, str(e))
        T("C2-b: ns_score defaults to 0", False)
        T("C2-c: stage defaults to Emerging", False)

    S("H3 — CSV import row cap")

    # Generate CSV with more than MAX_IMPORT_ROWS
    lines = ["ticker,shares,cost_basis"]
    for i in range(MAX_IMPORT_ROWS + 50):
        lines.append(f"TEST{i},{i + 1},100.00")
    csv_content = "\n".join(lines)

    result = pm.import_csv(pid, csv_content)
    T("H3-a: Import capped at MAX_IMPORT_ROWS", result["imported"] == MAX_IMPORT_ROWS,
      f"imported={result['imported']}")
    T("H3-b: Cap message in errors", any("capped" in e for e in result["errors"]),
      f"errors={result['errors'][-1:]}")

    S("M2 — remove_holding updates timestamp")

    pid2 = str(uuid.uuid4())
    now = "2025-01-01T00:00:00+00:00"
    repo.portfolios[pid2] = {"id": pid2, "user_id": "local", "name": "Test", "created_at": now, "updated_at": now}
    hid = pm.add_holding(pid2, "MSFT", 5)

    repo.portfolios[pid2]["updated_at"] = "2025-01-01T00:00:00+00:00"
    before_ts = repo.portfolios[pid2]["updated_at"]
    pm.remove_holding(hid)
    after_ts = repo.portfolios[pid2]["updated_at"]
    T("M2-a: remove_holding updates portfolio timestamp", after_ts > before_ts,
      f"before={before_ts}, after={after_ts}")

    # remove_holding with non-existent holding shouldn't crash
    try:
        pm.remove_holding("nonexistent-id")
        T("M2-b: remove non-existent holding does not crash", True)
    except Exception as e:
        T("M2-b: remove non-existent holding does not crash", False, str(e))


def test_export():
    S("M3 — Export share_text fallback")

    from export import ExportManager

    repo = MockRepository()
    nid = str(uuid.uuid4())
    repo.narratives[nid] = {
        "narrative_id": nid, "name": "Test Narrative", "ns_score": 0.65,
        "stage": "Growing", "description": "A test narrative",
        "linked_assets": '["AAPL", "MSFT"]',
    }

    # With LLM that returns fallback string
    failing_llm = MockLlmClient(fail=True)
    em = ExportManager(repo, failing_llm)
    result = em.generate_share_text(nid, "twitter")
    T("M3-a: Fallback used when LLM returns generic", "Not financial advice" in result,
      f"result starts with: {result[:50]}")
    T("M3-b: Fallback is platform-specific (twitter)", "#FinTwit" in result,
      f"result={result[:80]}")

    # With no LLM at all
    em_no_llm = ExportManager(repo, None)
    result2 = em_no_llm.generate_share_text(nid, "discord")
    T("M3-c: No LLM generates discord fallback", "**" in result2 and "Ns:" in result2)

    # Unknown platform falls back to twitter
    result3 = em_no_llm.generate_share_text(nid, "unknown_platform")
    T("M3-d: Unknown platform falls back to twitter", "#FinTwit" in result3)


def test_watchlist():
    S("M4 — Watchlist duplicate prevention")

    from watchlist import WatchlistManager

    repo = MockRepository()
    wm = WatchlistManager(repo)

    wl_id = wm.create_watchlist()

    # Add ticker
    item_id = wm.add_item(wl_id, "ticker", "AAPL")
    T("M4-a: First add succeeds", bool(item_id))

    # Duplicate should raise
    try:
        wm.add_item(wl_id, "ticker", "AAPL")
        T("M4-b: Duplicate ticker raises ValueError", False)
    except ValueError as e:
        T("M4-b: Duplicate ticker raises ValueError", True, str(e))

    # Same ticker lowercase should also be caught
    try:
        wm.add_item(wl_id, "ticker", "aapl")
        T("M4-c: Case-insensitive duplicate caught", False)
    except ValueError:
        T("M4-c: Case-insensitive duplicate caught", True)

    # Different ticker should work
    try:
        wm.add_item(wl_id, "ticker", "MSFT")
        T("M4-d: Different ticker succeeds", True)
    except ValueError:
        T("M4-d: Different ticker succeeds", False)

    # Narrative type should work independently
    try:
        wm.add_item(wl_id, "narrative", "some-narrative-id")
        T("M4-e: Narrative type independent of ticker", True)
    except ValueError:
        T("M4-e: Narrative type independent of ticker", False)

    # Duplicate narrative should fail
    try:
        wm.add_item(wl_id, "narrative", "some-narrative-id")
        T("M4-f: Duplicate narrative raises ValueError", False)
    except ValueError:
        T("M4-f: Duplicate narrative raises ValueError", True)

    # Ownership/listing behavior
    wm.create_watchlist(user_id="other-user", name="Other Watchlist")
    local_lists = wm.list_watchlists("local")
    other_lists = wm.list_watchlists("other-user")
    T("P09b-w1: list_watchlists filters by user ownership",
      len(local_lists) == 1 and len(other_lists) == 1,
      f"local={len(local_lists)} other={len(other_lists)}")


# ===========================================================================

if __name__ == "__main__":
    test_notifications()
    test_portfolio()
    test_export()
    test_watchlist()

    print("\n" + "=" * 60)
    passed = sum(1 for _, ok in _results if ok)
    failed = sum(1 for _, ok in _results if not ok)
    print(f"  TOTAL: {passed} passed, {failed} failed, {len(_results)} total")

    if failed:
        print("\n  FAILURES:")
        for name, ok in _results:
            if not ok:
                print(f"    - {name}")

    print("=" * 60)
    sys.exit(1 if failed else 0)
