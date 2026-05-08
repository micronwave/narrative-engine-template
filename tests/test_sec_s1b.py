"""
Security Audit S1-B test suite — LLM Budget Enforcement (C4) + SSE Auth/Limits (C6).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 tests/test_sec_s1b.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone
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
# Setup
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app  # noqa: E402
from api.main import (  # noqa: E402
    limiter,
    _sse_connections,
    _sse_per_user,
    _SSE_MAX_GLOBAL,
    _SSE_MAX_PER_USER,
    _latest_ticker_payload,
)

client = TestClient(app)

# Source code for structural checks
main_py = Path(__file__).parent.parent / "api" / "main.py"
main_source = main_py.read_text(encoding="utf-8")
llm_source = (Path(__file__).parent.parent / "llm_client.py").read_text(encoding="utf-8")
repo_source = (Path(__file__).parent.parent / "repository.py").read_text(encoding="utf-8")
settings_source = (Path(__file__).parent.parent / "settings.py").read_text(encoding="utf-8")


# ===========================================================================
# C4: LLM Budget Enforcement — Structural Checks
# ===========================================================================
S("C4: Settings + exception class")

T(
    "LLM_DAILY_BUDGET_USD in settings.py",
    "LLM_DAILY_BUDGET_USD" in settings_source,
    "field not found in settings.py",
)

T(
    "LLM_DAILY_BUDGET_USD default is 5.0",
    "LLM_DAILY_BUDGET_USD: float = 5.0" in settings_source,
    "expected float = 5.0",
)

T(
    "BudgetExceededError defined in llm_client.py",
    "class BudgetExceededError" in llm_source,
    "exception class not found",
)

try:
    from llm_client import BudgetExceededError  # noqa: E402
    T(
        "BudgetExceededError is importable",
        issubclass(BudgetExceededError, Exception),
        f"type: {type(BudgetExceededError)}",
    )
except ImportError as e:
    T("BudgetExceededError is importable", False, str(e))

# ---------------------------------------------------------------------------
S("C4: Repository get_daily_llm_spend")

T(
    "get_daily_llm_spend in Repository ABC",
    "def get_daily_llm_spend(self)" in repo_source and "@abstractmethod" in repo_source.split("def get_daily_llm_spend")[0].split("def ")[-1],
    "abstract method not found",
)

# Count implementations (ABC + SqliteRepository = at least 2 occurrences)
spend_count = repo_source.count("def get_daily_llm_spend")
T(
    "get_daily_llm_spend implemented in SqliteRepository",
    spend_count >= 2,
    f"found {spend_count} definitions, expected >= 2",
)

# Behavioral: test with a real temp DB
from repository import SqliteRepository  # noqa: E402

_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
_test_repo = SqliteRepository(_tmp.name)
_test_repo.migrate()

T(
    "get_daily_llm_spend returns 0.0 on empty DB",
    _test_repo.get_daily_llm_spend() == 0.0,
    f"got {_test_repo.get_daily_llm_spend()}",
)

# Insert some audit log entries for today
import sqlite3
_tconn = sqlite3.connect(_tmp.name)
_now_iso = datetime.now(timezone.utc).isoformat()
_yesterday_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
import uuid as _uuid

_tconn.execute(
    "INSERT INTO llm_audit_log (call_id, narrative_id, model, task_type, input_tokens, output_tokens, cost_estimate_usd, called_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    (str(_uuid.uuid4()), "nar-test", "haiku", "test", 100, 50, 0.50, _now_iso),
)
_tconn.execute(
    "INSERT INTO llm_audit_log (call_id, narrative_id, model, task_type, input_tokens, output_tokens, cost_estimate_usd, called_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    (str(_uuid.uuid4()), "nar-test", "haiku", "test", 200, 100, 1.25, _now_iso),
)
# Yesterday's entry — should NOT be counted
_tconn.execute(
    "INSERT INTO llm_audit_log (call_id, narrative_id, model, task_type, input_tokens, output_tokens, cost_estimate_usd, called_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    (str(_uuid.uuid4()), "nar-test", "haiku", "test", 500, 500, 10.00, _yesterday_iso),
)
_tconn.commit()
_tconn.close()

_spend = _test_repo.get_daily_llm_spend()
T(
    "get_daily_llm_spend sums today's entries",
    abs(_spend - 1.75) < 0.01,
    f"expected 1.75, got {_spend}",
)

T(
    "get_daily_llm_spend excludes yesterday",
    _spend < 2.0,
    f"got {_spend}, yesterday's $10 should be excluded",
)

# Cleanup temp DB
os.unlink(_tmp.name)

# ---------------------------------------------------------------------------
S("C4: Budget check in call_haiku")

# Find call_haiku method and verify budget check before retry loop
_haiku_start = llm_source.find("def call_haiku(")
_haiku_body = llm_source[_haiku_start:_haiku_start + 1200]

T(
    "budget check in call_haiku",
    "get_daily_llm_spend" in _haiku_body,
    "get_daily_llm_spend not found in call_haiku body",
)

T(
    "BudgetExceededError raised in call_haiku",
    "BudgetExceededError" in _haiku_body,
    "BudgetExceededError not found in call_haiku body",
)

# Verify budget check is BEFORE the retry loop
_budget_pos = _haiku_body.find("get_daily_llm_spend")
_retry_pos = _haiku_body.find("for attempt in range(3)")
T(
    "budget check before retry loop in call_haiku",
    0 < _budget_pos < _retry_pos,
    f"budget at {_budget_pos}, retry at {_retry_pos}",
)

# ---------------------------------------------------------------------------
S("C4: Budget check in call_haiku_chat")

_chat_start = llm_source.find("def call_haiku_chat(")
_chat_body = llm_source[_chat_start:_chat_start + 800]

T(
    "budget check in call_haiku_chat",
    "get_daily_llm_spend" in _chat_body,
    "get_daily_llm_spend not found in call_haiku_chat body",
)

T(
    "BudgetExceededError raised in call_haiku_chat",
    "BudgetExceededError" in _chat_body,
    "BudgetExceededError not found in call_haiku_chat body",
)

# Verify budget check is BEFORE the try block
_budget_pos_chat = _chat_body.find("get_daily_llm_spend")
_try_pos_chat = _chat_body.find("try:")
T(
    "budget check before try block in call_haiku_chat",
    0 < _budget_pos_chat < _try_pos_chat,
    f"budget at {_budget_pos_chat}, try at {_try_pos_chat}",
)

# ---------------------------------------------------------------------------
S("C4: Analyze endpoint exception handling")

# Find the analyze endpoint's LLM call block
_analyze_region = main_source[main_source.find("fallback_json = '{\"thesis\":"):]
_analyze_block = _analyze_region[:750]

T(
    "analyze endpoint imports BudgetExceededError",
    "BudgetExceededError" in _analyze_block,
    "BudgetExceededError not found near analyze LLM call",
)

T(
    "analyze endpoint catches BudgetExceededError",
    "except BudgetExceededError" in _analyze_block,
    "except BudgetExceededError clause not found",
)

T(
    "analyze endpoint returns 429 for budget exceeded",
    "status_code=429" in _analyze_block,
    "429 status code not found in budget handler",
)

# Verify BudgetExceededError is caught BEFORE generic Exception
_budget_except_pos = _analyze_block.find("except BudgetExceededError")
_generic_except_pos = _analyze_block.find("except Exception")
T(
    "BudgetExceededError caught before generic Exception",
    0 < _budget_except_pos < _generic_except_pos,
    f"BudgetExceededError at {_budget_except_pos}, Exception at {_generic_except_pos}",
)


# ===========================================================================
# C6: SSE Stream Auth + Connection Limits — Structural Checks
# ===========================================================================
S("C6: SSE tracking variables")

T(
    "_sse_connections importable",
    "_sse_connections" in dir(sys.modules.get("api.main", None) or __import__("api.main")),
    "variable not found in api.main",
)

T(
    "SSE_MAX_GLOBAL default is 100 in settings.py",
    "SSE_MAX_GLOBAL: int = 100" in settings_source,
    "Expected SSE_MAX_GLOBAL default to be 100",
)

T(
    "SSE_MAX_PER_USER default is 5 in settings.py",
    "SSE_MAX_PER_USER: int = 5" in settings_source,
    "Expected SSE_MAX_PER_USER default to be 5",
)

T(
    "api/main.py wires SSE limits from settings",
    "_SSE_MAX_GLOBAL = int(_API_SETTINGS.SSE_MAX_GLOBAL)" in main_source
    and "_SSE_MAX_PER_USER = int(_API_SETTINGS.SSE_MAX_PER_USER)" in main_source,
    "Expected module-level aliases sourced from API settings",
)

T(
    "runtime SSE limits are positive",
    _SSE_MAX_GLOBAL > 0 and _SSE_MAX_PER_USER > 0,
    f"global={_SSE_MAX_GLOBAL}, per_user={_SSE_MAX_PER_USER}",
)

T(
    "_sse_lock variable in source",
    "_sse_lock" in main_source and "asyncio.Lock" in main_source.split("_sse_lock")[1][:100],
    "_sse_lock not defined with asyncio.Lock type",
)

T(
    "_latest_ticker_payload in source",
    "_latest_ticker_payload" in main_source,
    "shared payload variable not found",
)

# ---------------------------------------------------------------------------
S("C6: Startup hook + broadcast loop")

T(
    "_init_sse startup hook exists",
    "async def _init_sse" in main_source,
    "startup hook not found",
)

T(
    "_sse_broadcast_loop defined",
    "async def _sse_broadcast_loop" in main_source,
    "broadcast loop not found",
)

T(
    "broadcast loop updates _latest_ticker_payload",
    "_latest_ticker_payload" in main_source.split("def _sse_broadcast_loop")[1][:700]
    or "_app_state.latest_ticker_payload" in main_source.split("def _sse_broadcast_loop")[1][:700],
    "shared payload not updated in broadcast loop",
)

# ---------------------------------------------------------------------------
S("C6: Stream endpoint auth + limits")

# Find the stream endpoint
_stream_start = main_source.find('async def stream(')
_stream_body = main_source[_stream_start:_stream_start + 2500]

T(
    "stream endpoint accepts token param",
    "token: str = Query(" in _stream_body or "token: str = Query(None)" in _stream_body,
    "token query parameter not found",
)

T(
    "stream endpoint checks _AUTH_MODE",
    "_AUTH_MODE" in _stream_body,
    "_AUTH_MODE not referenced in stream endpoint",
)

T(
    "stream endpoint calls _decode_jwt",
    "_decode_jwt" in _stream_body,
    "_decode_jwt not called in stream endpoint",
)

T(
    "stream endpoint checks _SSE_MAX_GLOBAL",
    "_SSE_MAX_GLOBAL" in _stream_body,
    "global connection limit not checked",
)

T(
    "stream endpoint checks _SSE_MAX_PER_USER",
    "_SSE_MAX_PER_USER" in _stream_body,
    "per-user connection limit not checked",
)

T(
    "stream endpoint has finally block for cleanup",
    "finally:" in _stream_body,
    "generator cleanup (finally) not found",
)

T(
    "stream endpoint reads _latest_ticker_payload",
    "_latest_ticker_payload" in _stream_body or "_app_state.latest_ticker_payload" in _stream_body,
    "shared payload not used in stream generator",
)

# Verify per-connection _ticker_payload() calls are gone from generator
_gen_start = _stream_body.find("def event_generator")
_gen_body = _stream_body[_gen_start:_gen_start + 500] if _gen_start > 0 else ""
T(
    "generator does NOT call _ticker_payload() directly",
    "_ticker_payload()" not in _gen_body,
    "per-connection _ticker_payload() call still present in generator",
)

T(
    "rate limit decorator preserved on stream endpoint",
    '@limiter.limit("2/minute")' in main_source.split("async def stream")[0].split("\n")[-2]
    or '@limiter.limit("2/minute")' in main_source[main_source.find("async def stream") - 200:main_source.find("async def stream")],
    "2/minute rate limit not found on stream endpoint",
)

# ---------------------------------------------------------------------------
S("C6: Stream endpoint stub mode behavior")

# Verify stub mode doesn't require a token (structural: _AUTH_MODE == "stub" path
# sets user_id = "local" without checking token)
_stub_path = main_source[main_source.find("async def stream("):]
_stub_block = _stub_path[:800]

T(
    "stub mode sets user_id='local' without token",
    'user_id = "local"' in _stub_block,
    "stub mode user_id assignment not found",
)

# Verify JWT mode requires token
T(
    "JWT mode rejects missing token with 403",
    "403" in _stub_block and "Authentication required" in _stub_block,
    "JWT mode 403 rejection not found",
)

# Verify connection decrement uses max(0, ...) to prevent negative counts
T(
    "connection decrement prevents negative counts",
    "max(0, _sse_connections - 1)" in _stub_block
    or "max(0, _sse_connections - 1)" in main_source
    or "max(0, _app_state.sse_connections - 1)" in _stub_block
    or "max(0, _app_state.sse_connections - 1)" in main_source,
    "max(0, ...) guard not found",
)


# ===========================================================================
# Summary
# ===========================================================================
_print_summary()
sys.exit(0 if _fail == 0 else 1)
