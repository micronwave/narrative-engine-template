"""
User-state security checks for notification ownership.

Run with:
    python -X utf8 tests/test_user_state_security.py
"""

import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app  # noqa: E402
from api.app_legacy import get_optional_user  # noqa: E402
import api.app_legacy as main_mod  # noqa: E402
from repository import SqliteRepository  # noqa: E402

_results: list[tuple[str, bool, str]] = []


def T(name: str, condition: bool, details: str = "") -> None:
    _results.append((name, bool(condition), details))
    if not condition:
        print(f"FAIL {name}" + (f" — {details}" if details else ""), file=sys.stderr)


def _print_summary() -> None:
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = len(_results) - passed
    print("\n" + "=" * 60)
    print(f"TOTAL: {passed} passed, {failed} failed out of {len(_results)} tests")
    print("=" * 60)


tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
tmp_db.close()
repo = SqliteRepository(tmp_db.name)
repo.migrate()

own_id = str(uuid.uuid4())
other_id = str(uuid.uuid4())
now = datetime.now(timezone.utc).isoformat()

repo.create_notification(
    {
        "id": own_id,
        "user_id": "local",
        "rule_id": None,
        "title": "Own notification",
        "message": "owned by local",
        "link": "/",
        "is_read": 0,
        "created_at": now,
    }
)
repo.create_notification(
    {
        "id": str(uuid.uuid4()),
        "user_id": "local",
        "rule_id": None,
        "title": "Second own notification",
        "message": "owned by local 2",
        "link": "/",
        "is_read": 0,
        "created_at": now,
    }
)
repo.create_notification(
    {
        "id": other_id,
        "user_id": "other-user",
        "rule_id": None,
        "title": "Other notification",
        "message": "owned by other",
        "link": "/",
        "is_read": 0,
        "created_at": now,
    }
)

orig_get_repo = main_mod.get_repo
main_mod.get_repo = lambda: repo
app.dependency_overrides[get_optional_user] = lambda: {"user_id": "local", "role": "user"}

try:
    with TestClient(app) as client:
        # Another user's notification must be blocked.
        resp_forbidden = client.post(f"/api/alerts/read/{other_id}")
        T("cross-user notification read returns 403", resp_forbidden.status_code == 403, str(resp_forbidden.status_code))
        other_after = repo.get_notification(other_id)
        T("cross-user notification remains unread", int(other_after.get("is_read") or 0) == 0 if other_after else False)

        # Own notification should be markable.
        resp_ok = client.post(f"/api/alerts/read/{own_id}")
        T("own notification read returns 200", resp_ok.status_code == 200, str(resp_ok.status_code))
        own_after = repo.get_notification(own_id)
        T("own notification marked read", int(own_after.get("is_read") or 0) == 1 if own_after else False)

        # Read-all should affect only current user notifications.
        resp_read_all = client.post("/api/alerts/read-all")
        T("read-all returns 200", resp_read_all.status_code == 200, str(resp_read_all.status_code))
        own_unread_after = repo.get_notifications("local", unread_only=True)
        other_unread_after = repo.get_notifications("other-user", unread_only=True)
        T("read-all clears local unread notifications", len(own_unread_after) == 0, str(own_unread_after))
        T("read-all does not clear other-user notifications", len(other_unread_after) >= 1, str(other_unread_after))

        # Alert-rule endpoints: own toggle + cross-user delete forbidden.
        create_rule_resp = client.post(
            "/api/alerts/rules",
            json={
                "rule_type": "new_narrative",
                "target_type": "portfolio",
                "target_id": "",
                "threshold": 0.0,
            },
        )
        T("create alert rule returns 200", create_rule_resp.status_code == 200, str(create_rule_resp.status_code))
        created_rule_id = create_rule_resp.json().get("rule_id")
        T("create alert rule returns rule_id", bool(created_rule_id), str(create_rule_resp.json()))

        toggle_resp = client.post(f"/api/alerts/rules/{created_rule_id}/toggle")
        T("toggle own alert rule returns 200", toggle_resp.status_code == 200, str(toggle_resp.status_code))
        T("toggle own alert rule includes enabled", "enabled" in toggle_resp.json(), str(toggle_resp.json()))

        other_rule_id = str(uuid.uuid4())
        repo.create_notification_rule(
            {
                "id": other_rule_id,
                "user_id": "other-user",
                "rule_type": "new_narrative",
                "target_type": "portfolio",
                "target_id": "",
                "threshold": 0.0,
                "enabled": 1,
                "created_at": now,
            }
        )
        delete_other_resp = client.delete(f"/api/alerts/rules/{other_rule_id}")
        T(
            "cross-user alert rule delete returns 403",
            delete_other_resp.status_code == 403,
            str(delete_other_resp.status_code),
        )

        # Signal endpoint direct coverage.
        signal_resp = client.get("/api/narratives/nonexistent-narrative/signal")
        T("narrative signal endpoint returns 200", signal_resp.status_code == 200, str(signal_resp.status_code))
        signal_body = signal_resp.json()
        T("narrative signal includes requested narrative_id", signal_body.get("narrative_id") == "nonexistent-narrative", str(signal_body))
        T("narrative signal returns None when absent", signal_body.get("signal") is None, str(signal_body))

        # Sentiment history endpoint direct coverage.
        sentiment_resp = client.get("/api/sentiment/AAPL/history?hours=24")
        T("sentiment history endpoint returns 200", sentiment_resp.status_code == 200, str(sentiment_resp.status_code))
        sentiment_body = sentiment_resp.json()
        T("sentiment history includes ticker", sentiment_body.get("ticker") == "AAPL", str(sentiment_body))
        T("sentiment history includes hours", int(sentiment_body.get("hours", -1)) == 24, str(sentiment_body))
        T("sentiment history includes data list", isinstance(sentiment_body.get("data"), list), str(sentiment_body))
finally:
    app.dependency_overrides.pop(get_optional_user, None)
    main_mod.get_repo = orig_get_repo
    Path(tmp_db.name).unlink(missing_ok=True)

_print_summary()
sys.exit(0 if all(ok for _, ok, _ in _results) else 1)
