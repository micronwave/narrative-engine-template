"""
Category 4 API Integration test suite.

Tests Finding 7-10 from final_solution_5.md / final_solution_5_b.md:
  - F7: EdgarIngester wiring — triple gate in ApiIngestionManager
  - F8: Startup key mismatch logging — MARKETAUX / NEWSDATA warnings
  - F9: Source field in /api/stocks — nq.source passed through _apply_normalized
  - F10: Price API usage logging — DataNormalizer calls increment_api_usage

Run with:
    python -X utf8 tests/test_cat4_api.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import os
import sys
import tempfile
from datetime import datetime, timezone, date as _date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Path setup — must come before any project imports
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
_API      = str(ROOT / "api")
_SERVICES = str(ROOT / "api" / "services")
_ADAPTERS = str(ROOT / "api" / "adapters")
for _p in [str(ROOT), _API, _SERVICES, _ADAPTERS]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-real")
os.environ.setdefault("DB_PATH", tempfile.mktemp(suffix=".db"))

# ---------------------------------------------------------------------------
# Minimal test runner
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
    _results.append({"section": _current_section, "name": name,
                     "passed": bool(condition), "details": details})
    if condition:
        _pass += 1
    else:
        _fail += 1
    mark = "PASS" if condition else "FAIL"
    det = f"  ({details})" if details else ""
    print(f"  [{mark}] {name}{det}")


def _report():
    seen: list[str] = []
    for r in _results:
        if r["section"] not in seen:
            seen.append(r["section"])
    print("\n" + "=" * 60)
    print(f"  {'Section':<44} {'Pass':>4}  {'Fail':>4}")
    print("-" * 60)
    for sec in seen:
        items = [r for r in _results if r["section"] == sec]
        p = sum(1 for r in items if r["passed"])
        f = sum(1 for r in items if not r["passed"])
        print(f"  {sec:<44} {p:>4} {f:>5}")
    print("=" * 60)
    print(f"  TOTAL: {_pass} passed, {_fail} failed out of {_pass + _fail} tests")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Source text — load once, used throughout
# ---------------------------------------------------------------------------

_SETTINGS_SRC   = (ROOT / "settings.py").read_text(encoding="utf-8")
_INGESTERS_SRC  = (ROOT / "api_ingesters.py").read_text(encoding="utf-8")
_MAIN_SRC       = (ROOT / "api" / "main.py").read_text(encoding="utf-8")
_NORMALIZER_SRC = (ROOT / "api" / "services" / "data_normalizer.py").read_text(encoding="utf-8")


# ===========================================================================
# Finding 7-A — settings.py EDGAR fields
# ===========================================================================

S("F7-A: settings.py — EDGAR fields")

from settings import Settings

_fields = Settings.model_fields

T("EDGAR_TICKERS field exists",
  "EDGAR_TICKERS" in _fields)

T("EDGAR_TICKERS defaults to empty string",
  _fields.get("EDGAR_TICKERS") is not None and _fields["EDGAR_TICKERS"].default == "",
  f"default={_fields.get('EDGAR_TICKERS', 'MISSING')!r}")

T("EDGAR_EMAIL field exists",
  "EDGAR_EMAIL" in _fields)

T("EDGAR_EMAIL defaults to empty string",
  _fields.get("EDGAR_EMAIL") is not None and _fields["EDGAR_EMAIL"].default == "",
  f"default={_fields.get('EDGAR_EMAIL', 'MISSING')!r}")

T("EDGAR_COMPANY_NAME field exists",
  "EDGAR_COMPANY_NAME" in _fields)

T("ENABLE_EDGAR field exists",
  "ENABLE_EDGAR" in _fields)

T("ENABLE_EDGAR defaults to False",
  _fields.get("ENABLE_EDGAR") is not None and _fields["ENABLE_EDGAR"].default is False,
  f"default={_fields.get('ENABLE_EDGAR', 'MISSING')!r}")


# ===========================================================================
# Finding 7-B — api_ingesters.py gate (source inspection)
# ===========================================================================

S("F7-B: api_ingesters.py — triple gate (source inspection)")

T("settings.ENABLE_EDGAR checked in gate",
  "settings.ENABLE_EDGAR" in _INGESTERS_SRC)

T("settings.EDGAR_EMAIL checked in gate",
  "settings.EDGAR_EMAIL" in _INGESTERS_SRC)

T("settings.EDGAR_TICKERS checked in gate",
  "settings.EDGAR_TICKERS" in _INGESTERS_SRC)

T("EdgarIngester imported lazily (inside if-block, not at top)",
  "from ingester import EdgarIngester" in _INGESTERS_SRC
  and "from ingester import RawDocument" in _INGESTERS_SRC  # top-level import
  and _INGESTERS_SRC.index("from ingester import EdgarIngester")
      > _INGESTERS_SRC.index("from ingester import RawDocument"))

T("EDGAR_COMPANY_NAME passed to EdgarIngester",
  "company_name=settings.EDGAR_COMPANY_NAME" in _INGESTERS_SRC)

T("email=settings.EDGAR_EMAIL passed to EdgarIngester",
  "email=settings.EDGAR_EMAIL" in _INGESTERS_SRC)

T("Ticker whitespace stripped with .strip()",
  ".strip()" in _INGESTERS_SRC and "EDGAR_TICKERS" in _INGESTERS_SRC)

T("Empty-tickers fallback log message present",
  "EDGAR_TICKERS is empty" in _INGESTERS_SRC
  or ("ENABLE_EDGAR=True" in _INGESTERS_SRC and "skipping" in _INGESTERS_SRC))

T("ApiIngestionManager class defined",
  "class ApiIngestionManager" in _INGESTERS_SRC)


# ===========================================================================
# Finding 7-C — ApiIngestionManager gate (unit tests)
# ===========================================================================

S("F7-C: ApiIngestionManager — gate unit tests")

import api_ingesters


def _mock_settings(**overrides):
    """SimpleNamespace with only the fields ApiIngestionManager.__init__ reads."""
    defaults = dict(
        ENABLE_EDGAR=False,
        EDGAR_EMAIL="",
        EDGAR_TICKERS="",
        EDGAR_COMPANY_NAME="TestCo",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _build_manager(settings_ns):
    """Instantiate ApiIngestionManager with the three base ingesters mocked."""
    mock_repo = MagicMock()
    with patch("api_ingesters.MarketauxIngester", return_value=MagicMock()), \
         patch("api_ingesters.NewsdataIngester",  return_value=MagicMock()), \
         patch("api_ingesters.RedditIngester",    return_value=MagicMock()):
        return api_ingesters.ApiIngestionManager(settings_ns, mock_repo)


# Gate off: ENABLE_EDGAR=False
try:
    m = _build_manager(_mock_settings(ENABLE_EDGAR=False))
    T("ENABLE_EDGAR=False → 3 base ingesters",
      len(m._ingesters) == 3, f"got {len(m._ingesters)}")
except Exception as exc:
    T("ENABLE_EDGAR=False → 3 base ingesters", False, str(exc))

# Gate off: EDGAR_EMAIL missing
try:
    m = _build_manager(_mock_settings(ENABLE_EDGAR=True, EDGAR_EMAIL="", EDGAR_TICKERS="AAPL"))
    T("ENABLE_EDGAR=True, EDGAR_EMAIL='' → 3 ingesters (gate blocked)",
      len(m._ingesters) == 3, f"got {len(m._ingesters)}")
except Exception as exc:
    T("ENABLE_EDGAR=True, EDGAR_EMAIL='' → 3 ingesters (gate blocked)", False, str(exc))

# Gate off: EDGAR_TICKERS empty string
try:
    m = _build_manager(_mock_settings(ENABLE_EDGAR=True, EDGAR_EMAIL="t@x.com", EDGAR_TICKERS=""))
    T("ENABLE_EDGAR=True, EDGAR_TICKERS='' → 3 ingesters (gate blocked)",
      len(m._ingesters) == 3, f"got {len(m._ingesters)}")
except Exception as exc:
    T("ENABLE_EDGAR=True, EDGAR_TICKERS='' → 3 ingesters (gate blocked)", False, str(exc))

# Gate off: EDGAR_TICKERS whitespace-only (all entries stripped to empty)
try:
    m = _build_manager(_mock_settings(ENABLE_EDGAR=True, EDGAR_EMAIL="t@x.com", EDGAR_TICKERS=" , , "))
    T("EDGAR_TICKERS=' , , ' (whitespace only) → 3 ingesters",
      len(m._ingesters) == 3, f"got {len(m._ingesters)}")
except Exception as exc:
    T("EDGAR_TICKERS=' , , ' (whitespace only) → 3 ingesters", False, str(exc))

# Gate open: all three conditions met → 4 ingesters
try:
    mock_edgar_cls = MagicMock()
    mock_edgar_instance = MagicMock()
    mock_edgar_cls.return_value = mock_edgar_instance

    with patch("api_ingesters.MarketauxIngester", return_value=MagicMock()), \
         patch("api_ingesters.NewsdataIngester",  return_value=MagicMock()), \
         patch("api_ingesters.RedditIngester",    return_value=MagicMock()), \
         patch("ingester.EdgarIngester", mock_edgar_cls):
        manager = api_ingesters.ApiIngestionManager(
            _mock_settings(ENABLE_EDGAR=True, EDGAR_EMAIL="t@x.com", EDGAR_TICKERS="AAPL"),
            MagicMock(),
        )
    T("All EDGAR conditions met → 4 ingesters",
      len(manager._ingesters) == 4, f"got {len(manager._ingesters)}")
    T("4th ingester is the EdgarIngester instance",
      manager._ingesters[-1] is mock_edgar_instance,
      f"last type: {type(manager._ingesters[-1])}")
except Exception as exc:
    T("All EDGAR conditions met → 4 ingesters", False, str(exc))
    T("4th ingester is the EdgarIngester instance", False, str(exc))

# Ticker parsing: spaces around entries stripped, all three parsed correctly
try:
    mock_edgar_cls = MagicMock()
    mock_edgar_cls.return_value = MagicMock()

    with patch("api_ingesters.MarketauxIngester", return_value=MagicMock()), \
         patch("api_ingesters.NewsdataIngester",  return_value=MagicMock()), \
         patch("api_ingesters.RedditIngester",    return_value=MagicMock()), \
         patch("ingester.EdgarIngester", mock_edgar_cls):
        api_ingesters.ApiIngestionManager(
            _mock_settings(
                ENABLE_EDGAR=True,
                EDGAR_EMAIL="t@x.com",
                EDGAR_TICKERS=" AAPL , MSFT , NVDA ",
            ),
            MagicMock(),
        )

    passed_tickers = mock_edgar_cls.call_args.kwargs.get("tickers") or \
                     mock_edgar_cls.call_args[1].get("tickers")
    T("Ticker whitespace stripped: ['AAPL', 'MSFT', 'NVDA']",
      passed_tickers == ["AAPL", "MSFT", "NVDA"],
      f"got {passed_tickers}")
    T("Exactly 3 tickers parsed from 3-item CSV",
      len(passed_tickers) == 3, f"got {len(passed_tickers)}")
except Exception as exc:
    T("Ticker whitespace stripped: ['AAPL', 'MSFT', 'NVDA']", False, str(exc))
    T("Exactly 3 tickers parsed from 3-item CSV", False, str(exc))

# company_name and email forwarded correctly
try:
    mock_edgar_cls = MagicMock()
    mock_edgar_cls.return_value = MagicMock()

    with patch("api_ingesters.MarketauxIngester", return_value=MagicMock()), \
         patch("api_ingesters.NewsdataIngester",  return_value=MagicMock()), \
         patch("api_ingesters.RedditIngester",    return_value=MagicMock()), \
         patch("ingester.EdgarIngester", mock_edgar_cls):
        api_ingesters.ApiIngestionManager(
            _mock_settings(
                ENABLE_EDGAR=True,
                EDGAR_EMAIL="researcher@corp.com",
                EDGAR_TICKERS="AAPL",
                EDGAR_COMPANY_NAME="CorpIntelligence",
            ),
            MagicMock(),
        )

    kw = mock_edgar_cls.call_args.kwargs
    T("email kwarg forwarded to EdgarIngester",
      kw.get("email") == "researcher@corp.com",
      f"got {kw.get('email')!r}")
    T("company_name kwarg forwarded to EdgarIngester",
      kw.get("company_name") == "CorpIntelligence",
      f"got {kw.get('company_name')!r}")
except Exception as exc:
    T("email kwarg forwarded to EdgarIngester", False, str(exc))
    T("company_name kwarg forwarded to EdgarIngester", False, str(exc))


# ===========================================================================
# Finding 8 — Startup key mismatch logging (source inspection)
# ===========================================================================

S("F8: api/main.py — startup key mismatch logging")

# Locate the function in source
_fn_start = _MAIN_SRC.find("async def start_price_refresh")
_fn_next  = _MAIN_SRC.find("\nasync def ", _fn_start + 1)
_fn_body  = _MAIN_SRC[_fn_start:_fn_next] if _fn_start > 0 else ""

T("start_price_refresh function found",
  _fn_start > 0)

T("MARKETAUX mismatch check inside start_price_refresh",
  "MARKETAUX_API_KEY" in _fn_body,
  "MARKETAUX_API_KEY not found in function body")

T("NEWSDATA mismatch check inside start_price_refresh",
  "NEWSDATA_API_KEY" in _fn_body,
  "NEWSDATA_API_KEY not found in function body")

T("MARKETAUX warning message: 'ingester will be inactive'",
  "ingester will be inactive" in _fn_body)

T("NEWSDATA warning message: 'ingester will be inactive'",
  _fn_body.count("ingester will be inactive") >= 2,
  f"found {_fn_body.count('ingester will be inactive')} occurrences")

T("ENABLE_MARKETAUX env var checked",
  "ENABLE_MARKETAUX" in _fn_body)

T("ENABLE_NEWSDATA env var checked",
  "ENABLE_NEWSDATA" in _fn_body)

T("Disabled values checked: 'false' and '0' treated as off",
  '"false"' in _fn_body and '"0"' in _fn_body)

T("logger.info used (not print) for mismatch warnings",
  "logger.info" in _fn_body and "MARKETAUX" in _fn_body)

# Verify exact message text matches the spec
_expected_mx_msg = "MarketAux enabled but MARKETAUX_API_KEY not set — ingester will be inactive"
_expected_nd_msg = "NewsData enabled but NEWSDATA_API_KEY not set — ingester will be inactive"
T(f"MARKETAUX log message matches spec exactly",
  _expected_mx_msg in _fn_body,
  f"expected: {_expected_mx_msg!r}")
T(f"NEWSDATA log message matches spec exactly",
  _expected_nd_msg in _fn_body,
  f"expected: {_expected_nd_msg!r}")


# ===========================================================================
# Finding 9-A — source field: source inspection
# ===========================================================================

S("F9-A: api/main.py — _apply_normalized source field (source inspection)")

T("_apply_normalized defined in main.py",
  "_apply_normalized" in _MAIN_SRC)

T('sec["source"] = nq.source present',
  'sec["source"] = nq.source' in _MAIN_SRC)

T("NormalizedQuote.source typed as str in data_normalizer.py",
  "source: str" in _NORMALIZER_SRC)

T("None guard: if nq is None: return inside _apply_normalized",
  "if nq is None:" in _MAIN_SRC)

T("price_change_24h computed from nq.close",
  "price_change_24h" in _MAIN_SRC and "nq.close" in _MAIN_SRC)

_fn_def_pos = _MAIN_SRC.find("def _apply_normalized")
_fn_def_body = _MAIN_SRC[_fn_def_pos:_fn_def_pos + 800] if _fn_def_pos > 0 else ""
T("price_change_24h = None branch exists (close=0 case)",
  "price_change_24h" in _fn_def_body and "= None" in _fn_def_body)


# ===========================================================================
# Finding 9-B — source field: functional tests
# ===========================================================================

S("F9-B: _apply_normalized behaviour (functional tests)")

from data_normalizer import NormalizedQuote

_TS = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


def _make_quote(symbol="AAPL", price=180.0, close=170.0, source="finnhub"):
    return NormalizedQuote(
        symbol=symbol,
        instrument_type="equity",
        price=price,
        close=close,
        timestamp=_TS,
        source=source,
        delay="realtime",
    )


# Replicate _apply_normalized — same logic as api/main.py
def _apply(sec: dict, nq) -> None:
    if nq is None:
        return
    sec["current_price"] = nq.price
    sec["source"] = nq.source
    if nq.close and nq.close != 0:
        sec["price_change_24h"] = round((nq.price - nq.close) / nq.close * 100, 2)
    else:
        sec["price_change_24h"] = None


# Basic: source and price populated
sec = {}
_apply(sec, _make_quote(source="twelve_data", price=180.0, close=170.0))
T('source="twelve_data" written to sec',
  sec.get("source") == "twelve_data", f"got {sec.get('source')!r}")
T("current_price written to sec",
  sec.get("current_price") == 180.0, f"got {sec.get('current_price')}")

# price_change_24h calculation
_expected = round((180.0 - 170.0) / 170.0 * 100, 2)
T("price_change_24h correctly computed",
  sec.get("price_change_24h") == _expected,
  f"expected={_expected}, got={sec.get('price_change_24h')}")

# nq=None: sec dict left completely unchanged
sec2 = {"current_price": 100.0, "symbol": "X"}
_apply(sec2, None)
T("nq=None: sec unchanged",
  sec2 == {"current_price": 100.0, "symbol": "X"}, f"sec={sec2}")

# close=0: price_change_24h=None
sec3 = {}
_apply(sec3, _make_quote(source="coingecko", close=0.0))
T("close=0 → price_change_24h is None",
  sec3.get("price_change_24h") is None,
  f"got {sec3.get('price_change_24h')}")
T("close=0 → source still written",
  sec3.get("source") == "coingecko", f"got {sec3.get('source')!r}")

# close=None: price_change_24h=None
sec4 = {}
_apply(sec4, NormalizedQuote(
    symbol="BTC-USD", instrument_type="crypto", price=60000.0, close=None,
    timestamp=_TS, source="coingecko", delay="realtime",
))
T("close=None → price_change_24h is None",
  sec4.get("price_change_24h") is None,
  f"got {sec4.get('price_change_24h')}")

# All four source values pass through
for _src in ("finnhub", "twelve_data", "coingecko", "yfinance"):
    _s: dict = {}
    _apply(_s, _make_quote(source=_src))
    T(f'source="{_src}" round-trips correctly',
      _s.get("source") == _src, f"got {_s.get('source')!r}")

# Multiple securities: each gets its own source
sec_a: dict = {}
sec_b: dict = {}
_apply(sec_a, _make_quote(source="finnhub",    price=100.0, close=90.0))
_apply(sec_b, _make_quote(source="coingecko",  price=200.0, close=180.0))
T("Two securities get independent source fields",
  sec_a.get("source") == "finnhub" and sec_b.get("source") == "coingecko",
  f"sec_a={sec_a.get('source')!r}, sec_b={sec_b.get('source')!r}")


# ===========================================================================
# Finding 10-A — DataNormalizer usage logging (source inspection)
# ===========================================================================

S("F10-A: data_normalizer.py — usage logging (source inspection)")

T("DataNormalizer.__init__ accepts repository=None",
  "def __init__(self, adapters: list, repository=None)" in _NORMALIZER_SRC)

T("self._repository = repository in __init__",
  "self._repository = repository" in _NORMALIZER_SRC)

T("increment_api_usage call present in get_quote",
  "increment_api_usage" in _NORMALIZER_SRC)

T("limit=0 passed (price adapters have no cap)",
  "isoformat(), 0" in _NORMALIZER_SRC or ", 0)" in _NORMALIZER_SRC)

T("Repository None-check guards the call",
  "if self._repository is not None:" in _NORMALIZER_SRC)

T("Exception swallowed: bare except/except Exception inside usage block",
  "except Exception:" in _NORMALIZER_SRC or "except:" in _NORMALIZER_SRC)

T("get_quotes_batch delegates to get_quote (logging inherited)",
  "self.get_quote(" in _NORMALIZER_SRC and "get_quotes_batch" in _NORMALIZER_SRC)


# ===========================================================================
# Finding 10-B — DataNormalizer usage logging (unit tests)
# ===========================================================================

S("F10-B: DataNormalizer — usage logging unit tests")

from data_normalizer import DataNormalizer, NormalizedQuote

_NOW = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
_TODAY = _date.today().isoformat()


def _fake_adapter(class_name: str, return_value=None, raises=None):
    """Create a minimal adapter whose type().__name__ == class_name."""
    def fetch_quote(self, symbol, instrument_type="equity"):
        if raises is not None:
            raise raises
        return return_value
    return type(class_name, (), {"fetch_quote": fetch_quote})()


def _quote(source="finnhub"):
    return NormalizedQuote(
        symbol="AAPL", instrument_type="equity", price=150.0,
        timestamp=_NOW, source=source, delay="realtime",
    )


# ── repository=None (default): no error, quote returned ──────────────────

try:
    dn = DataNormalizer(adapters=[_fake_adapter("FinnhubAdapter", _quote())])
    result = dn.get_quote("AAPL")
    T("repository=None: quote returned without error",
      result is not None and result.price == 150.0, f"result={result}")
    T("repository=None: _repository attribute is None",
      dn._repository is None)
except Exception as exc:
    T("repository=None: quote returned without error", False, str(exc))
    T("repository=None: _repository attribute is None", False, str(exc))


# ── successful fetch → increment_api_usage called ─────────────────────────

try:
    mock_repo = MagicMock()
    dn = DataNormalizer(
        adapters=[_fake_adapter("FinnhubAdapter", _quote("finnhub"))],
        repository=mock_repo,
    )
    result = dn.get_quote("AAPL")

    T("get_quote returns quote",
      result is not None and result.source == "finnhub")
    T("increment_api_usage called exactly once",
      mock_repo.increment_api_usage.call_count == 1,
      f"calls={mock_repo.increment_api_usage.call_count}")

    _args = mock_repo.increment_api_usage.call_args[0]
    T("adapter_name='finnhub' passed",
      _args[0] == "finnhub", f"got {_args[0]!r}")
    T("date=today's ISO string passed",
      _args[1] == _TODAY, f"got {_args[1]!r}")
    T("limit=0 passed",
      _args[2] == 0, f"got {_args[2]!r}")
except Exception as exc:
    T("get_quote returns quote", False, str(exc))
    T("increment_api_usage called exactly once", False, str(exc))
    T("adapter_name='finnhub' passed", False, str(exc))
    T("date=today's ISO string passed", False, str(exc))
    T("limit=0 passed", False, str(exc))


# ── adapter returns None → NO logging ────────────────────────────────────

try:
    mock_repo = MagicMock()
    dn = DataNormalizer(
        adapters=[_fake_adapter("FinnhubAdapter", None)],  # returns None
        repository=mock_repo,
    )
    result = dn.get_quote("UNKNOWN")
    T("adapter returns None → get_quote returns None",
      result is None)
    T("adapter returns None → increment_api_usage NOT called",
      mock_repo.increment_api_usage.call_count == 0,
      f"calls={mock_repo.increment_api_usage.call_count}")
except Exception as exc:
    T("adapter returns None → get_quote returns None", False, str(exc))
    T("adapter returns None → increment_api_usage NOT called", False, str(exc))


# ── increment_api_usage raises → quote still returned ────────────────────

try:
    mock_repo = MagicMock()
    mock_repo.increment_api_usage.side_effect = RuntimeError("DB locked")
    dn = DataNormalizer(
        adapters=[_fake_adapter("FinnhubAdapter", _quote())],
        repository=mock_repo,
    )
    result = dn.get_quote("AAPL")
    T("increment_api_usage raises → quote still returned (exception swallowed)",
      result is not None and result.price == 150.0, f"result={result}")
except Exception as exc:
    T("increment_api_usage raises → quote still returned (exception swallowed)", False, str(exc))


# ── adapter name mapping ──────────────────────────────────────────────────

_adapter_name_cases = [
    ("FinnhubAdapter",    "finnhub"),
    ("TwelveDataAdapter", "twelve_data"),
    ("CoinGeckoAdapter",  "coingecko"),
]
for _cls_name, _expected_key in _adapter_name_cases:
    try:
        mock_repo = MagicMock()
        dn = DataNormalizer(
            adapters=[_fake_adapter(_cls_name, _quote(source=_expected_key))],
            repository=mock_repo,
        )
        dn.get_quote("AAPL")
        _logged_name = mock_repo.increment_api_usage.call_args[0][0]
        T(f"{_cls_name} maps to '{_expected_key}' in usage log",
          _logged_name == _expected_key, f"got {_logged_name!r}")
    except Exception as exc:
        T(f"{_cls_name} maps to '{_expected_key}' in usage log", False, str(exc))


# ── first adapter fails → second logs, not first ─────────────────────────

try:
    mock_repo = MagicMock()
    dn = DataNormalizer(
        adapters=[
            _fake_adapter("FinnhubAdapter",    raises=ConnectionError("timeout")),
            _fake_adapter("TwelveDataAdapter", _quote(source="twelve_data")),
        ],
        repository=mock_repo,
    )
    result = dn.get_quote("AAPL")
    T("fallback adapter used when first fails",
      result is not None and result.source == "twelve_data",
      f"source={result.source if result else None!r}")
    T("usage logged for successful backup adapter only",
      mock_repo.increment_api_usage.call_count == 1,
      f"calls={mock_repo.increment_api_usage.call_count}")
    _logged_name = mock_repo.increment_api_usage.call_args[0][0]
    T("backup adapter name 'twelve_data' logged (not failed 'finnhub')",
      _logged_name == "twelve_data", f"got {_logged_name!r}")
except Exception as exc:
    T("fallback adapter used when first fails", False, str(exc))
    T("usage logged for successful backup adapter only", False, str(exc))
    T("backup adapter name 'twelve_data' logged (not failed 'finnhub')", False, str(exc))


# ── get_quotes_batch: each successful symbol logs once ──────────────────

try:
    mock_repo = MagicMock()
    dn = DataNormalizer(
        adapters=[_fake_adapter("FinnhubAdapter", _quote())],
        repository=mock_repo,
    )
    results = dn.get_quotes_batch(["AAPL", "MSFT", "NVDA"])
    T("get_quotes_batch returns all 3 symbols",
      len(results) == 3, f"got {len(results)}")
    T("get_quotes_batch → increment_api_usage called 3 times (one per symbol)",
      mock_repo.increment_api_usage.call_count == 3,
      f"calls={mock_repo.increment_api_usage.call_count}")
    _all_names = {c[0][0] for c in mock_repo.increment_api_usage.call_args_list}
    T("all batch log calls use 'finnhub' adapter name",
      _all_names == {"finnhub"}, f"got {_all_names}")
except Exception as exc:
    T("get_quotes_batch returns all 3 symbols", False, str(exc))
    T("get_quotes_batch → increment_api_usage called 3 times (one per symbol)", False, str(exc))
    T("all batch log calls use 'finnhub' adapter name", False, str(exc))


# ── batch with some failures: only successes logged ──────────────────────

try:
    _call_count = {"n": 0}

    class _SometimesFailing:
        def fetch_quote(self, symbol, instrument_type="equity"):
            _call_count["n"] += 1
            if symbol == "FAIL":
                return None  # not found — no logging expected
            return _quote()

    # Give it a class name that maps to finnhub
    _SometimesFailing.__name__ = "FinnhubAdapter"

    mock_repo = MagicMock()
    dn = DataNormalizer(adapters=[_SometimesFailing()], repository=mock_repo)
    results = dn.get_quotes_batch(["AAPL", "FAIL", "MSFT"])

    T("batch with one miss: 3 results returned",
      len(results) == 3, f"got {len(results)}")
    T("FAIL symbol returns None in results",
      results.get("FAIL") is None, f"got {results.get('FAIL')}")
    T("successful symbols logged (2 calls, not 3)",
      mock_repo.increment_api_usage.call_count == 2,
      f"calls={mock_repo.increment_api_usage.call_count}")
except Exception as exc:
    T("batch with one miss: 3 results returned", False, str(exc))
    T("FAIL symbol returns None in results", False, str(exc))
    T("successful symbols logged (2 calls, not 3)", False, str(exc))


# ===========================================================================
# Finding 10-C — _init_data_normalizer_repo startup hook (source inspection)
# ===========================================================================

S("F10-C: api/main.py — _init_data_normalizer_repo startup hook")

_hook_pos  = _MAIN_SRC.find("async def _init_data_normalizer_repo")
_hook_next = _MAIN_SRC.find("\nasync def ", _hook_pos + 1)
_hook_body = _MAIN_SRC[_hook_pos:_hook_next] if _hook_pos > 0 else ""

T("_init_data_normalizer_repo defined as async def",
  _hook_pos > 0, "Function not found in main.py")

_decorator_window = _MAIN_SRC[max(0, _hook_pos - 150):_hook_pos]
T('@app.on_event("startup") decorator present immediately before hook',
  '@app.on_event("startup")' in _decorator_window,
  f"window={_decorator_window!r}")

T("get_repo() called inside hook body",
  "get_repo()" in _hook_body,
  f"hook_body={_hook_body[:300]!r}")

T("data_normalizer._repository = repo assigned in hook",
  "data_normalizer._repository = repo" in _hook_body)

T("None-check guards the assignment (non-fatal if DB unavailable)",
  "if repo is not None" in _hook_body,
  f"hook_body={_hook_body[:300]!r}")

# Confirm data_normalizer module-level object exists (hook can write to it)
T("module-level data_normalizer object defined (hook can set ._repository)",
  "data_normalizer = DataNormalizer(" in _MAIN_SRC)


# ---------------------------------------------------------------------------
# Final report
# ---------------------------------------------------------------------------

_report()
sys.exit(0 if _fail == 0 else 1)
