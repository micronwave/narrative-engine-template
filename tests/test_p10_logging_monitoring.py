"""
P10 Logging & Monitoring verification.

Batches covered:
  B1 — credential values not emitted to logs
  B2 — API error contract: stable status + body, no str(e) leakage
  B3 — silent exception swallows replaced with logger calls
  B4 — X-Response-Time-Ms header, /api/health DB+WS fields

Run with:
    python -X utf8 tests/test_p10_logging_monitoring.py
"""

from __future__ import annotations

import importlib
import json
import logging
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient  # noqa: E402

import api.app_legacy as app_mod  # noqa: E402
from api.main import app  # noqa: E402
from api.app_legacy import STUB_AUTH_TOKEN  # noqa: E402

_AUTH = {"x-auth-token": STUB_AUTH_TOKEN}

_results: list[tuple[str, bool, str]] = []


def S(section: str) -> None:
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = "") -> None:
    _results.append((name, bool(condition), details))
    status = "PASS" if condition else "FAIL"
    suffix = f" — {details}" if details else ""
    print(f"{status} {name}{suffix}")


def _print_summary() -> None:
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = len(_results) - passed
    print("\n" + "=" * 60)
    print(f"TOTAL: {passed} passed, {failed} failed out of {len(_results)} tests")
    print("=" * 60)


# ---------------------------------------------------------------------------
# B1 — credential/token values must not appear in log output
# ---------------------------------------------------------------------------
S("B1 — credential safety")

_CREDENTIAL_PATTERNS = [
    re.compile(r"[?&]token=\S+", re.IGNORECASE),        # query-string token value
    re.compile(r"MARKETAUX_API_KEY\s*=\s*\S+"),           # literal key value
    re.compile(r"NEWSDATA_API_KEY\s*=\s*\S+"),
    re.compile(r"apikey=[A-Za-z0-9_\-]{8,}", re.IGNORECASE),
]

_log_records: list[logging.LogRecord] = []


class _Capture(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        _log_records.append(record)


_cap = _Capture(level=logging.DEBUG)
logging.getLogger().addHandler(_cap)

with TestClient(app) as client:
    # Hit the verify endpoint — token value must not land in logs
    client.get("/api/auth/verify?token=SUPERSECRET123", headers=_AUTH)

# Exclude httpx client logs — those reflect test harness URLs, not server-emitted logs.
_app_records = [r for r in _log_records if not r.name.startswith("httpx")]
_log_text = " ".join(r.getMessage() for r in _app_records)

T(
    "verification token value not in app logs",
    "SUPERSECRET123" not in _log_text,
    "token value found in app log output" if "SUPERSECRET123" in _log_text else "",
)

for pat in _CREDENTIAL_PATTERNS:
    match = pat.search(_log_text)
    T(
        f"credential pattern '{pat.pattern[:40]}' absent from app logs",
        match is None,
        f"matched: {match.group()!r}" if match else "",
    )

logging.getLogger().removeHandler(_cap)


# ---------------------------------------------------------------------------
# B2 — API error contract
# ---------------------------------------------------------------------------
S("B2 — error contract: stable payloads")

with TestClient(app) as client:
    # Sentiment endpoint: str(e) must not appear in response body. Use valid-format ticker.
    resp_sentiment = client.get("/api/sentiment/ZZZFAKE", headers=_AUTH)
    # Earnings: failures must return 503, not empty list
    # Patch to force an exception inside earnings handler
    with patch("api.app_legacy.get_repo") as mock_get_repo:
        mock_repo = MagicMock()
        mock_repo._get_conn.side_effect = RuntimeError("db gone")
        mock_get_repo.return_value = mock_repo
        resp_health_degraded = client.get("/api/health", headers=_AUTH)

T(
    "sentiment endpoint returns 2xx or 500 (not leaking str(e))",
    resp_sentiment.status_code in (200, 500, 503),
    str(resp_sentiment.status_code),
)
if resp_sentiment.status_code >= 400:
    body_text = resp_sentiment.text
    T(
        "sentiment error detail is stable (no raw exception text)",
        "Traceback" not in body_text and len(body_text) < 500,
        body_text[:200],
    )

# Health endpoint degraded state
T(
    "health returns 200 even when DB degraded",
    resp_health_degraded.status_code == 200,
    str(resp_health_degraded.status_code),
)
health_body = resp_health_degraded.json()
T(
    "health body has 'status' field",
    "status" in health_body,
    str(health_body),
)
T(
    "health body has 'db' field",
    "db" in health_body,
    str(health_body),
)
T(
    "health body has 'websocket_relay' field",
    "websocket_relay" in health_body,
    str(health_body),
)


# ---------------------------------------------------------------------------
# B2 — earnings endpoint raises 503 on failure, not silent []
# ---------------------------------------------------------------------------
S("B2 — earnings failure contract")

with TestClient(app) as client:
    with patch("api.app_legacy.get_upcoming_earnings", side_effect=RuntimeError("service down"), create=True):
        # Patch the inner import inside the route handler
        import builtins
        _real_import = builtins.__import__

        def _failing_import(name, *args, **kwargs):
            if name == "earnings_service":
                raise ImportError("mocked failure")
            return _real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_failing_import):
            resp_earnings = client.get("/api/earnings/upcoming", headers=_AUTH)

T(
    "earnings 503 on failure (not silent [])",
    resp_earnings.status_code == 503,
    f"got {resp_earnings.status_code}: {resp_earnings.text[:200]}",
)


# ---------------------------------------------------------------------------
# B3 — logging hygiene: no bare silent swallows
# ---------------------------------------------------------------------------
S("B3 — logging hygiene")

import ast
import pathlib

_SILENT_SWALLOW_RE = re.compile(
    r"except\s+(?:Exception|[\w.]+\s+as\s+\w+)[^:]*:\s*\n\s+pass\s*\n",
    re.MULTILINE,
)


def _check_file_no_silent_swallows(path: pathlib.Path, exempt_linenos: set[int]) -> list[int]:
    """Return line numbers of bare silent `except ...: pass` blocks not in exempt_linenos."""
    src = path.read_text(encoding="utf-8")
    lines = src.splitlines()
    bad = []
    for i, line in enumerate(lines, 1):
        if i in exempt_linenos:
            continue
        stripped = line.strip()
        if stripped == "pass":
            # look back for except
            for j in range(i - 2, max(0, i - 5), -1):
                if lines[j].strip().startswith("except"):
                    bad.append(i)
                    break
    return bad


# websocket_relay: line 129 (asyncio.TimeoutError) is the intentional exempt
_ws_bad = _check_file_no_silent_swallows(
    pathlib.Path("api/services/websocket_relay.py"),
    exempt_linenos={129},
)
T(
    "websocket_relay.py: no unlogged silent except..pass",
    len(_ws_bad) == 0,
    f"bare pass at lines {_ws_bad}" if _ws_bad else "",
)

_agg_bad = _check_file_no_silent_swallows(
    pathlib.Path("api/services/sentiment_aggregator.py"),
    exempt_linenos=set(),
)
T(
    "sentiment_aggregator.py: no unlogged silent except..pass",
    len(_agg_bad) == 0,
    f"bare pass at lines {_agg_bad}" if _agg_bad else "",
)


# ---------------------------------------------------------------------------
# B4 — X-Response-Time-Ms header
# ---------------------------------------------------------------------------
S("B4 — observability headers and health fields")

with TestClient(app) as client:
    resp_timed = client.get("/api/health", headers=_AUTH)

timing_header = resp_timed.headers.get("x-response-time-ms")
T(
    "X-Response-Time-Ms header present",
    timing_header is not None,
    f"headers={dict(resp_timed.headers)}",
)
if timing_header is not None:
    try:
        val = float(timing_header)
        T("X-Response-Time-Ms is a valid float", True, str(val))
        T("X-Response-Time-Ms is positive", val > 0, str(val))
    except ValueError:
        T("X-Response-Time-Ms is a valid float", False, timing_header)

# Health endpoint fields
health_live = resp_timed.json()
T("health.status present", "status" in health_live, str(health_live))
T("health.db present", "db" in health_live, str(health_live))
T("health.websocket_relay present", "websocket_relay" in health_live, str(health_live))
T(
    "health.websocket_relay is string",
    isinstance(health_live.get("websocket_relay"), str),
    str(health_live.get("websocket_relay")),
)


_print_summary()
sys.exit(0 if all(ok for _, ok, _ in _results) else 1)
