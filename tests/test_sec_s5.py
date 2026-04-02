"""
Security Audit S5 Checkpoint B test suite — H8 + M3 + M4.

Tests:
  - H8: Dependency version pinning (requirements.txt, api/requirements.txt)
  - M3: DB file permissions startup hook (api/main.py)
  - M4: Circuit breaker hardening (circuit_breaker.py, data_normalizer.py)

Run with:
    python -X utf8 tests/test_sec_s5.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import sys
import time
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
_API = str(ROOT / "api")
_SERVICES = str(ROOT / "api" / "services")
_ADAPTERS = str(ROOT / "api" / "adapters")
for _p in [str(ROOT), _API, _SERVICES, _ADAPTERS]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Minimal test runner (matches project conventions)
# ---------------------------------------------------------------------------

_results: list[dict] = []
_current_section: str = "Unset"
_pass = 0
_fail = 0


def S(section_name: str) -> None:
    global _current_section
    _current_section = section_name
    print(f"\n{'=' * 60}")
    print(f"  {section_name}")
    print(f"{'=' * 60}")


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


# ===========================================================================
# H8 — Dependency Version Pinning
# ===========================================================================

S("H8 — requirements.txt pinning")

_req_root = (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
_req_api  = (ROOT / "api" / "requirements.txt").read_text(encoding="utf-8").splitlines()


def _pkg_lines(lines: list[str]) -> list[str]:
    """Return non-blank, non-comment requirement lines."""
    return [
        ln.strip() for ln in lines
        if ln.strip() and not ln.strip().startswith("#")
    ]


def _all_pinned(lines: list[str]) -> tuple[bool, list[str]]:
    """Return (all_pinned, unpinned_lines). A line is pinned if it contains ==."""
    unpinned = [ln for ln in _pkg_lines(lines) if "==" not in ln]
    return len(unpinned) == 0, unpinned


root_ok, root_unpinned = _all_pinned(_req_root)
T("root requirements.txt — all packages pinned with ==",
  root_ok,
  f"unpinned: {root_unpinned}" if not root_ok else "")

T("root requirements.txt — scipy pinned",
  any("scipy==" in ln for ln in _req_root))

T("root requirements.txt — websockets pinned",
  any("websockets==" in ln for ln in _req_root))

T("root requirements.txt — yfinance pinned",
  any("yfinance==" in ln for ln in _req_root))

T("root requirements.txt — praw pinned",
  any("praw==" in ln for ln in _req_root))

S("H8 — api/requirements.txt pinning")

api_ok, api_unpinned = _all_pinned(_req_api)
T("api/requirements.txt — all packages pinned with ==",
  api_ok,
  f"unpinned: {api_unpinned}" if not api_ok else "")

T("api/requirements.txt — fastapi pinned",
  any("fastapi==" in ln for ln in _req_api))

T("api/requirements.txt — uvicorn pinned",
  any("uvicorn[standard]==" in ln for ln in _req_api))

T("api/requirements.txt — PyJWT pinned",
  any("PyJWT==" in ln for ln in _req_api))

T("api/requirements.txt — bcrypt pinned",
  any("bcrypt==" in ln for ln in _req_api))

T("api/requirements.txt — slowapi pinned",
  any("slowapi==" in ln for ln in _req_api))

T("api/requirements.txt — no >= operators remain",
  ">=" not in "\n".join(_pkg_lines(_req_api)))

T("root requirements.txt — no >= operators remain",
  ">=" not in "\n".join(_pkg_lines(_req_root)))


# ===========================================================================
# M3 — DB file permissions startup hook (source code inspection)
# ===========================================================================

S("M3 — DB permissions startup hook")

_main_src = (ROOT / "api" / "main.py").read_text(encoding="utf-8")

T("import stat present in api/main.py",
  "import stat" in _main_src)

T("_check_db_permissions startup hook defined",
  "async def _check_db_permissions" in _main_src)

T("@app.on_event startup decorator on _check_db_permissions",
  '@app.on_event("startup")\nasync def _check_db_permissions' in _main_src)

T("Windows guard: skips on os.name == 'nt'",
  'os.name != "nt"' in _main_src or "os.name != 'nt'" in _main_src)

T("checks S_IRGRP bit",
  "S_IRGRP" in _main_src)

T("checks S_IROTH bit",
  "S_IROTH" in _main_src)

T("applies S_IRUSR | S_IWUSR (chmod 600)",
  "S_IRUSR" in _main_src and "S_IWUSR" in _main_src)

T("logs warning when permissions are too open",
  "world/group-readable" in _main_src or "group-readable" in _main_src)


# ===========================================================================
# M4 — CircuitBreaker: import
# ===========================================================================

S("M4 — CircuitBreaker: module structure")

from circuit_breaker import CircuitBreaker, _FAILURE_THRESHOLD, _MIN_FAILURE_SOURCES, _RECOVERY_TIMEOUT

T("_FAILURE_THRESHOLD is 10",
  _FAILURE_THRESHOLD == 10,
  f"got {_FAILURE_THRESHOLD}")

T("_MIN_FAILURE_SOURCES is 2",
  _MIN_FAILURE_SOURCES == 2,
  f"got {_MIN_FAILURE_SOURCES}")

T("_RECOVERY_TIMEOUT is 300",
  _RECOVERY_TIMEOUT == 300,
  f"got {_RECOVERY_TIMEOUT}")

T("CircuitBreaker has _failure_sources set",
  isinstance(CircuitBreaker("test")._failure_sources, set))

T("CircuitBreaker has force_close method",
  callable(getattr(CircuitBreaker("test"), "force_close", None)))


# ===========================================================================
# M4 — CircuitBreaker: initial state
# ===========================================================================

S("M4 — CircuitBreaker: initial state")

cb = CircuitBreaker("test-init")

T("is_open is False initially",
  cb.is_open is False)

T("_consecutive_failures is 0 initially",
  cb._consecutive_failures == 0)

T("_failure_sources is empty initially",
  len(cb._failure_sources) == 0)

T("_open_since is 0.0 initially",
  cb._open_since == 0.0)


# ===========================================================================
# M4 — CircuitBreaker: does NOT open with only 1 source
# ===========================================================================

S("M4 — CircuitBreaker: single-source isolation (spoofing prevention)")

cb_single = CircuitBreaker("test-single-source")

# Drive 15 failures from one source — should never open
for _ in range(15):
    cb_single.record_failure(source="attacker")

T("15 failures from 1 source: is_open is False",
  cb_single.is_open is False,
  f"failures={cb_single._consecutive_failures}, sources={cb_single._failure_sources}")

T("15 failures from 1 source: _consecutive_failures == 15",
  cb_single._consecutive_failures == 15)

T("15 failures from 1 source: only 1 source recorded",
  len(cb_single._failure_sources) == 1)

T("15 failures from 1 source: _open_since stays 0.0",
  cb_single._open_since == 0.0)


# ===========================================================================
# M4 — CircuitBreaker: does NOT open below failure threshold
# ===========================================================================

S("M4 — CircuitBreaker: below failure threshold")

cb_below = CircuitBreaker("test-below-threshold")

# 9 failures from 3 different sources — threshold is 10
for i in range(9):
    cb_below.record_failure(source=f"src{i % 3}")

T("9 failures from 3 sources: is_open is False",
  cb_below.is_open is False)

T("9 failures from 3 sources: _open_since stays 0.0",
  cb_below._open_since == 0.0)


# ===========================================================================
# M4 — CircuitBreaker: opens at exact threshold
# ===========================================================================

S("M4 — CircuitBreaker: opens at exact threshold")

cb_exact = CircuitBreaker("test-exact")

# 9 failures: 5 from src-a, 4 from src-b — still closed
for i in range(5):
    cb_exact.record_failure(source="src-a")
for i in range(4):
    cb_exact.record_failure(source="src-b")

T("9 failures: still closed",
  cb_exact.is_open is False)

# 10th failure from src-b → crosses threshold with 2 sources → opens
t_before = time.time()
cb_exact.record_failure(source="src-b")
t_after = time.time()

T("10th failure (2 sources): is_open is True",
  cb_exact.is_open is True)

T("_open_since set on opening",
  t_before <= cb_exact._open_since <= t_after,
  f"open_since={cb_exact._open_since:.3f}, before={t_before:.3f}, after={t_after:.3f}")


# ===========================================================================
# M4 — CircuitBreaker: _open_since not reset by additional failures (THE BUG FIX)
# ===========================================================================

S("M4 — CircuitBreaker: _open_since fixed (recovery window stable)")

cb_reset = CircuitBreaker("test-reset")

# Open the breaker
for i in range(10):
    cb_reset.record_failure(source="src-a" if i < 5 else "src-b")

T("breaker is open after 10 failures",
  cb_reset.is_open is True)

open_since_first = cb_reset._open_since
T("_open_since is non-zero after opening",
  open_since_first > 0.0)

# Wait a moment, then add more failures — _open_since must NOT change
time.sleep(0.05)
for _ in range(5):
    cb_reset.record_failure(source="src-c")

T("additional failures do not reset _open_since",
  cb_reset._open_since == open_since_first,
  f"first={open_since_first:.6f}, after_extra={cb_reset._open_since:.6f}")

T("is_open still True after extra failures",
  cb_reset.is_open is True)

# Verify log is only emitted once on first opening
import logging
import io

cb_log = CircuitBreaker("test-log")
log_buf = io.StringIO()
handler = logging.StreamHandler(log_buf)
handler.setLevel(logging.WARNING)
logging.getLogger("circuit_breaker").addHandler(handler)

for i in range(15):
    cb_log.record_failure(source="src-a" if i < 5 else "src-b")

logging.getLogger("circuit_breaker").removeHandler(handler)
log_output = log_buf.getvalue()
open_count = log_output.count("circuit OPEN")

T("OPEN warning logged exactly once (not on every extra failure)",
  open_count == 1,
  f"logged {open_count} times")


# ===========================================================================
# M4 — CircuitBreaker: record_success resets all state
# ===========================================================================

S("M4 — CircuitBreaker: record_success")

cb_succ = CircuitBreaker("test-success")
for i in range(10):
    cb_succ.record_failure(source="src-a" if i < 5 else "src-b")

T("open before success", cb_succ.is_open is True)

cb_succ.record_success()

T("is_open False after success",
  cb_succ.is_open is False)

T("_consecutive_failures reset to 0",
  cb_succ._consecutive_failures == 0)

T("_failure_sources cleared",
  len(cb_succ._failure_sources) == 0)

T("_open_since reset to 0.0",
  cb_succ._open_since == 0.0)


# ===========================================================================
# M4 — CircuitBreaker: force_close
# ===========================================================================

S("M4 — CircuitBreaker: force_close")

cb_force = CircuitBreaker("test-force")
for i in range(10):
    cb_force.record_failure(source="src-a" if i < 5 else "src-b")

T("open before force_close", cb_force.is_open is True)

cb_force.force_close()

T("is_open False after force_close",
  cb_force.is_open is False)

T("_consecutive_failures reset",
  cb_force._consecutive_failures == 0)

T("_failure_sources cleared",
  len(cb_force._failure_sources) == 0)

T("_open_since reset to 0.0",
  cb_force._open_since == 0.0)


# ===========================================================================
# M4 — CircuitBreaker: auto-recovery after timeout
# ===========================================================================

S("M4 — CircuitBreaker: auto-recovery via timeout")

cb_rec = CircuitBreaker("test-recovery")
for i in range(10):
    cb_rec.record_failure(source="src-a" if i < 5 else "src-b")

T("is_open True before timeout", cb_rec.is_open is True)

# Simulate time passing beyond recovery window by backdating _open_since
cb_rec._open_since = time.time() - (_RECOVERY_TIMEOUT + 1)

T("is_open False after recovery timeout",
  cb_rec.is_open is False,
  f"_consecutive_failures={cb_rec._consecutive_failures}")

T("_consecutive_failures reset after auto-recovery",
  cb_rec._consecutive_failures == 0)

T("_failure_sources cleared after auto-recovery",
  len(cb_rec._failure_sources) == 0)

T("_open_since reset to 0.0 after auto-recovery",
  cb_rec._open_since == 0.0)

# Can re-open after recovery
for i in range(10):
    cb_rec.record_failure(source="src-a" if i < 5 else "src-b")

T("can re-open after recovery",
  cb_rec.is_open is True)


# ===========================================================================
# M4 — CircuitBreaker: record_success is no-op when already closed
# ===========================================================================

S("M4 — CircuitBreaker: record_success no-op when closed")

cb_noop = CircuitBreaker("test-noop")
cb_noop.record_success()  # should not raise or mutate anything unexpected

T("record_success on fresh breaker: is_open stays False",
  cb_noop.is_open is False)

T("record_success on fresh breaker: failures stay 0",
  cb_noop._consecutive_failures == 0)


# ===========================================================================
# M4 — CircuitBreaker: thread safety
# ===========================================================================

S("M4 — CircuitBreaker: thread safety")

cb_thread = CircuitBreaker("test-thread")
errors_seen: list[Exception] = []

def _worker(src: str, n: int) -> None:
    try:
        for _ in range(n):
            cb_thread.record_failure(source=src)
    except Exception as e:
        errors_seen.append(e)

threads = [threading.Thread(target=_worker, args=(f"src{i}", 20)) for i in range(5)]
for t in threads:
    t.start()
for t in threads:
    t.join()

T("no exceptions under concurrent record_failure",
  len(errors_seen) == 0,
  f"errors: {errors_seen}")

T("_consecutive_failures == 100 after 5×20 concurrent failures",
  cb_thread._consecutive_failures == 100,
  f"got {cb_thread._consecutive_failures}")

T("5 distinct sources recorded",
  len(cb_thread._failure_sources) == 5,
  f"sources: {cb_thread._failure_sources}")

T("is_open True (both thresholds met)",
  cb_thread.is_open is True)


# ===========================================================================
# M4 — DataNormalizer: source parameter passed to record_failure
# ===========================================================================

S("M4 — DataNormalizer: source parameter wiring")

from data_normalizer import DataNormalizer, NormalizedQuote

# Build a mock adapter that always raises
mock_adapter = MagicMock()
mock_adapter.fetch_quote.side_effect = RuntimeError("provider down")
mock_adapter.__class__.__name__ = "FinnhubAdapter"

dn = DataNormalizer([mock_adapter])

# Patch the breaker's record_failure to capture calls
breaker = list(dn._breakers.values())[0]
breaker.record_failure = MagicMock(wraps=breaker.record_failure)

result = dn.get_quote("AAPL", source="192.168.1.1")

T("get_quote returns None when adapter raises",
  result is None)

T("record_failure called on adapter exception",
  breaker.record_failure.called)

T("record_failure called with source='192.168.1.1'",
  breaker.record_failure.call_args == call(source="192.168.1.1"),
  f"actual call: {breaker.record_failure.call_args}")

# Default source when not specified
mock_adapter2 = MagicMock()
mock_adapter2.fetch_quote.side_effect = RuntimeError("down")
mock_adapter2.__class__.__name__ = "FinnhubAdapter"

dn2 = DataNormalizer([mock_adapter2])
breaker2 = list(dn2._breakers.values())[0]
breaker2.record_failure = MagicMock(wraps=breaker2.record_failure)

dn2.get_quote("AAPL")  # no source arg

T("default source is 'unknown' when not provided",
  breaker2.record_failure.call_args == call(source="unknown"),
  f"actual call: {breaker2.record_failure.call_args}")


# ===========================================================================
# M4 — DataNormalizer: skips open circuits
# ===========================================================================

S("M4 — DataNormalizer: open circuit is skipped")

mock_a = MagicMock()
mock_a.fetch_quote.return_value = None  # adapter "works" but has no data
mock_a.__class__.__name__ = "FinnhubAdapter"

mock_b = MagicMock()
from datetime import datetime, timezone
mock_b.fetch_quote.return_value = NormalizedQuote(
    symbol="AAPL", instrument_type="equity", price=150.0,
    timestamp=datetime.now(timezone.utc), source="twelve_data", delay="realtime"
)
mock_b.__class__.__name__ = "TwelveDataAdapter"

dn3 = DataNormalizer([mock_a, mock_b])

# Force-open the first adapter's breaker
breaker_a = dn3._breakers[id(mock_a)]
for i in range(10):
    breaker_a.record_failure(source="src-a" if i < 5 else "src-b")

T("first adapter breaker is open", breaker_a.is_open is True)

quote = dn3.get_quote("AAPL")

T("returns quote from second adapter when first is open",
  quote is not None and quote.source == "twelve_data",
  f"got: {quote}")

T("first adapter fetch_quote NOT called when breaker open",
  not mock_a.fetch_quote.called)


# ===========================================================================
# Summary
# ===========================================================================

print(f"\n{'=' * 60}")
print(f"S5 Checkpoint B — {_pass} passed, {_fail} failed out of {_pass + _fail}")
print(f"{'=' * 60}")

if _fail > 0:
    print("\nFailed tests:")
    for r in _results:
        if not r["passed"]:
            det = f"  ({r['details']})" if r["details"] else ""
            print(f"  [{r['section']}] {r['name']}{det}")

sys.exit(0 if _fail == 0 else 1)
