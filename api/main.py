"""
Narrative Engine — FastAPI public API (customer-facing).

Run from project root:
    python -m uvicorn api.main:app --port 8000 --reload

The Flask ops dashboard continues on port 5000 (dashboard/app.py) and is
completely independent. Both read from the same SQLite DB.
"""

import asyncio
import hashlib
import json
import os
import random
import re
import secrets
import stat
import sys
import threading
import time as _time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Optional

import functools
import inspect
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

from fastapi import Depends, FastAPI, File, HTTPException, Header, Path as FPath, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from starlette.responses import RedirectResponse

try:
    from sse_starlette.sse import EventSourceResponse
    _SSE_AVAILABLE = True
except ImportError:
    _SSE_AVAILABLE = False
    from starlette.responses import StreamingResponse

# Make project root importable (repository.py, settings.py, etc.)
_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Make api/ directory importable (finnhub_service.py, etc.)
_API_DIR = str(Path(__file__).parent)
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

# Phase 2: make api/services/ and api/adapters/ importable
_SERVICES_DIR = str(Path(__file__).parent / "services")
if _SERVICES_DIR not in sys.path:
    sys.path.insert(0, _SERVICES_DIR)

_ADAPTERS_DIR = str(Path(__file__).parent / "adapters")
if _ADAPTERS_DIR not in sys.path:
    sys.path.insert(0, _ADAPTERS_DIR)

from finnhub_service import FinnhubService  # noqa: E402
from data_normalizer import DataNormalizer, NormalizedQuote  # noqa: E402
from finnhub_adapter import FinnhubAdapter  # noqa: E402
from twelve_data_adapter import TwelveDataAdapter  # noqa: E402
from coingecko_adapter import CoinGeckoAdapter  # noqa: E402
from websocket_relay import FinnhubWebSocketRelay  # noqa: E402
from notifications import NotificationManager, RULE_TYPES  # noqa: E402
from sector_map import SECTOR_MAP  # noqa: E402
from stocktwits_adapter import StockTwitsAdapter  # noqa: E402
from sentiment_aggregator import SentimentAggregator  # noqa: E402

# Load .env into os.environ before reading env vars at module level
from dotenv import load_dotenv as _load_dotenv
_load_dotenv(Path(__file__).parent.parent / ".env")

finnhub = FinnhubService(
    api_key=os.environ.get("FINNHUB_API_KEY", ""),
    cache_ttl=int(os.environ.get("FINNHUB_CACHE_TTL_SECONDS", "60")),
)

# Phase 2: Data normalization layer — adapter chain
# Note: we parse ENABLE flags with the same truthiness rules as Pydantic bool
# coercion (1/true/yes/on) to stay in sync with settings.py definitions,
# but we read os.environ directly because importing Settings requires
# ANTHROPIC_API_KEY which would break the test suite.
_finnhub_adapter = FinnhubAdapter(finnhub)
_data_adapters = [_finnhub_adapter]
if os.environ.get("ENABLE_TWELVE_DATA", "").lower() in ("1", "true", "yes", "on"):
    _data_adapters.append(TwelveDataAdapter(
        api_key=os.environ.get("TWELVE_DATA_API_KEY", "")
    ))
if os.environ.get("ENABLE_COINGECKO", "").lower() in ("1", "true", "yes", "on"):
    _data_adapters.append(CoinGeckoAdapter(
        api_key=os.environ.get("COINGECKO_API_KEY", "")
    ))

data_normalizer = DataNormalizer(adapters=_data_adapters)

# Part C: Social sentiment instances (SentimentAggregator initialized at startup
# once repo is available — set to None until then)
_stocktwits_adapter = StockTwitsAdapter()
_sentiment_aggregator: "SentimentAggregator | None" = None

# Phase 2 Batch 4: WebSocket relay instance (None until startup if API key set)
_ws_relay: FinnhubWebSocketRelay | None = None

# DB path resolved at module level — never import settings here (it requires
# ANTHROPIC_API_KEY at import time and would break the test suite).
DB_PATH = str(Path(__file__).parent.parent / "data" / "narrative_engine.db")

# Auth mode: "stub" (single-user MVP) or "jwt" (multi-user)
_AUTH_MODE = os.environ.get("AUTH_MODE", "stub")

# Environment: "development" or "production" — controls HSTS + HTTPS redirect
_ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")

# ---------------------------------------------------------------------------
# Audit: shared-state lock, input validators, CSV sanitizer
# ---------------------------------------------------------------------------
_securities_lock = threading.Lock()

# C6: SSE connection tracking (asyncio.Lock since SSE endpoint is async def)
_sse_connections: int = 0
_sse_per_user: dict[str, int] = {}
_sse_lock: asyncio.Lock | None = None  # initialized in startup hook
_SSE_MAX_GLOBAL = 100
_SSE_MAX_PER_USER = 5
_latest_ticker_payload: dict = {"type": "ticker-update", "items": []}

# M10: Brute-force protection for login
_login_attempts: dict[str, list[float]] = {}  # email -> [timestamps]
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_WINDOW_SECONDS = 900  # 15 minutes

_SYMBOL_RE = re.compile(r'^(?=.*[A-Z0-9])[A-Z0-9.\-]{1,12}$')

# H7: Cookie security — Secure flag only in production (HTTPS)
_IS_SECURE_ENV = os.environ.get("ENVIRONMENT", "").lower() == "production"

# M9 / H7: IP extraction helper (respects X-Forwarded-For behind proxy)
def _extract_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# H7: CSRF token generation helper
def _generate_csrf_token() -> str:
    """Generate a random CSRF token."""
    return secrets.token_hex(32)


def _validate_symbol(symbol: str) -> str:
    """Validate and normalize a ticker symbol. Raises 422 on bad input."""
    s = symbol.upper().strip()
    if not _SYMBOL_RE.match(s):
        raise HTTPException(status_code=422, detail="Invalid ticker symbol")
    return s


def _csv_safe(val: str) -> str:
    """Prevent CSV formula injection (OWASP)."""
    if val and val[0] in ('=', '+', '-', '@', '\t', '\r', '\n'):
        return "'" + val
    return val


def _safe_json_list(raw) -> list:
    """Safely parse a JSON string that should be a list."""
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    try:
        result = json.loads(raw)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _get_jwt_secret() -> str:
    """Return JWT secret or raise 500 if not properly configured."""
    secret = os.environ.get("JWT_SECRET_KEY", "")
    if not secret or len(secret) < 32:
        raise HTTPException(status_code=500, detail="JWT_SECRET_KEY must be at least 32 characters")
    return secret

# ---------------------------------------------------------------------------
# D1 — Asset Class Association Model (in-memory stubs)
# ---------------------------------------------------------------------------
# Example stub data — replace with your own
ASSET_CLASSES = [
    {"id": "ac-001", "name": "Semiconductors", "type": "sector",
     "description": "Companies involved in chip design, fabrication, and equipment"},
    {"id": "ac-002", "name": "Energy", "type": "sector",
     "description": "Oil, gas, and renewable energy producers and distributors"},
    {"id": "ac-003", "name": "Treasury Bonds", "type": "index",
     "description": "US government debt securities across maturities"},
    {"id": "ac-004", "name": "Gold", "type": "commodity",
     "description": "Physical gold and gold-backed instruments"},
    {"id": "ac-005", "name": "Currencies", "type": "currency",
     "description": "Foreign exchange instruments and currency ETFs"},
    {"id": "ac-006", "name": "Crypto", "type": "crypto",
     "description": "Digital assets and cryptocurrency instruments"},
    {"id": "ac-007", "name": "Industrials", "type": "sector",
     "description": "Heavy industry, manufacturing, aerospace, and defense contractors"},
]

# Example stub data — replace with your own
TRACKED_SECURITIES = [
    # Semiconductors (ac-001)
    {"id": "ts-001", "symbol": "TSM", "name": "Taiwan Semiconductor Manufacturing",
     "asset_class_id": "ac-001", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-002", "symbol": "NVDA", "name": "NVIDIA Corporation",
     "asset_class_id": "ac-001", "exchange": "NASDAQ",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-003", "symbol": "INTC", "name": "Intel Corporation",
     "asset_class_id": "ac-001", "exchange": "NASDAQ",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-004", "symbol": "ASML", "name": "ASML Holding N.V.",
     "asset_class_id": "ac-001", "exchange": "NASDAQ",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    # Energy (ac-002)
    {"id": "ts-005", "symbol": "XOM", "name": "Exxon Mobil Corporation",
     "asset_class_id": "ac-002", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-006", "symbol": "CVX", "name": "Chevron Corporation",
     "asset_class_id": "ac-002", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-007", "symbol": "OXY", "name": "Occidental Petroleum Corporation",
     "asset_class_id": "ac-002", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    # Treasury Bonds (ac-003)
    {"id": "ts-008", "symbol": "TLT", "name": "iShares 20+ Year Treasury Bond ETF",
     "asset_class_id": "ac-003", "exchange": "NASDAQ",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-009", "symbol": "IEF", "name": "iShares 7-10 Year Treasury Bond ETF",
     "asset_class_id": "ac-003", "exchange": "NASDAQ",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    # Gold (ac-004)
    {"id": "ts-010", "symbol": "GLD", "name": "SPDR Gold Shares",
     "asset_class_id": "ac-004", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-011", "symbol": "IAU", "name": "iShares Gold Trust",
     "asset_class_id": "ac-004", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    # Currencies (ac-005)
    {"id": "ts-012", "symbol": "UUP", "name": "Invesco DB US Dollar Index Bullish Fund",
     "asset_class_id": "ac-005", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    # Crypto (ac-006)
    {"id": "ts-013", "symbol": "IBIT", "name": "iShares Bitcoin Trust ETF",
     "asset_class_id": "ac-006", "exchange": "NASDAQ",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-014", "symbol": "ETHA", "name": "iShares Ethereum Trust ETF",
     "asset_class_id": "ac-006", "exchange": "NASDAQ",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    # Industrials (ac-007)
    {"id": "ts-015", "symbol": "GE", "name": "GE Aerospace",
     "asset_class_id": "ac-007", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-016", "symbol": "CAT", "name": "Caterpillar Inc.",
     "asset_class_id": "ac-007", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
    {"id": "ts-017", "symbol": "LMT", "name": "Lockheed Martin Corporation",
     "asset_class_id": "ac-007", "exchange": "NYSE",
     "current_price": None, "price_change_24h": None, "narrative_impact_score": 0},
]

# Example stub data — replace with your own
# NarrativeAssets: placeholder narrative IDs replaced at startup with real DB IDs.
NARRATIVE_ASSETS = [
    {"id": "na-001", "narrative_id": "nar-001", "asset_class_id": "ac-001",
     "exposure_score": 0.92, "direction": "bullish",
     "rationale": "CHIPS Act funding directly benefits domestic semiconductor fabrication capacity"},
    {"id": "na-002", "narrative_id": "nar-001", "asset_class_id": "ac-007",
     "exposure_score": 0.65, "direction": "bullish",
     "rationale": "Reshoring and defense spending growth benefit industrial and aerospace sectors"},
    {"id": "na-003", "narrative_id": "nar-002", "asset_class_id": "ac-002",
     "exposure_score": 0.78, "direction": "bearish",
     "rationale": "Clean energy transition narrative creates structural pressure on fossil fuel valuations"},
    {"id": "na-004", "narrative_id": "nar-002", "asset_class_id": "ac-006",
     "exposure_score": 0.45, "direction": "mixed",
     "rationale": "Energy regulatory uncertainty spills into crypto mining profitability outlook"},
    {"id": "na-005", "narrative_id": "nar-003", "asset_class_id": "ac-003",
     "exposure_score": 0.88, "direction": "bearish",
     "rationale": "Persistent inflation signals reduce real returns on long-duration Treasury bonds"},
    {"id": "na-006", "narrative_id": "nar-003", "asset_class_id": "ac-004",
     "exposure_score": 0.82, "direction": "bullish",
     "rationale": "Inflation hedge demand and safe-haven flows drive gold accumulation narratives"},
    {"id": "na-007", "narrative_id": "nar-003", "asset_class_id": "ac-005",
     "exposure_score": 0.55, "direction": "uncertain",
     "rationale": "Dollar strength vs inflation backdrop creates competing currency signals"},
    {"id": "na-008", "narrative_id": "nar-001", "asset_class_id": "ac-006",
     "exposure_score": 0.38, "direction": "bullish",
     "rationale": "Institutional adoption of crypto accelerates alongside tech sector momentum"},
]


def _derive_direction(securities: list[dict]) -> str:
    """Derive bullish/bearish/mixed from actual price changes of securities."""
    changes = [
        s.get("price_change_24h") for s in securities
        if s.get("price_change_24h") is not None
    ]
    if not changes:
        return "uncertain"
    avg = sum(changes) / len(changes)
    if avg > 0.3:
        return "bullish"
    if avg < -0.3:
        return "bearish"
    return "mixed"


def _build_narrative_assets(narrative_id: str) -> list:
    """Returns NarrativeAsset list with nested asset_class and securities for a narrative."""
    matching = [na for na in NARRATIVE_ASSETS if na["narrative_id"] == narrative_id]
    result = []
    for na in matching:
        ac = next((a for a in ASSET_CLASSES if a["id"] == na["asset_class_id"]), None)
        if ac is None:
            continue
        securities = [s for s in TRACKED_SECURITIES if s["asset_class_id"] == na["asset_class_id"]]
        direction = _derive_direction(securities)
        result.append({**na, "direction": direction, "asset_class": ac, "securities": securities})
    return result


# ---------------------------------------------------------------------------
# D4 — Manipulation/Coordination Detection Model (in-memory stubs)
# ---------------------------------------------------------------------------
# Example stub data — replace with your own
MANIPULATION_INDICATORS = [
    {
        "id": "mi-001",
        "narrative_id": "nar-002",
        "indicator_type": "coordinated_amplification",
        "confidence": 0.78,
        "detected_at": "2026-03-15T10:30:00Z",
        "evidence_summary": "87% of signal volume originated from 3 source clusters within a 2-hour window",
        "flagged_signals": ["sig-003", "sig-007", "sig-012"],
        "status": "active",
    },
    {
        "id": "mi-002",
        "narrative_id": "nar-002",
        "indicator_type": "bot_network",
        "confidence": 0.91,
        "detected_at": "2026-03-15T14:00:00Z",
        "evidence_summary": "Uniform publishing cadence and near-identical phrasing across 12 sources suggests bot network activity",
        "flagged_signals": ["sig-003", "sig-008"],
        "status": "confirmed",
    },
    {
        "id": "mi-003",
        "narrative_id": "nar-001",
        "indicator_type": "temporal_spike",
        "confidence": 0.65,
        "detected_at": "2026-03-14T09:15:00Z",
        "evidence_summary": "Signal volume increased 340% in a 90-minute window with no corresponding news catalyst",
        "flagged_signals": ["sig-001", "sig-004", "sig-009"],
        "status": "under_review",
    },
    {
        "id": "mi-004",
        "narrative_id": "nar-003",
        "indicator_type": "astroturfing",
        "confidence": 0.43,
        "detected_at": "2026-03-13T16:45:00Z",
        "evidence_summary": "Multiple newly-created accounts amplifying the same narrative within hours of account creation",
        "flagged_signals": ["sig-010", "sig-011"],
        "status": "active",
    },
    {
        "id": "mi-005",
        "narrative_id": "nar-003",
        "indicator_type": "sockpuppet_cluster",
        "confidence": 0.82,
        "detected_at": "2026-03-13T17:30:00Z",
        "evidence_summary": "Network analysis reveals 7 accounts sharing IP ranges and publishing patterns consistent with single-operator control",
        "flagged_signals": ["sig-010"],
        "status": "confirmed",
    },
    {
        "id": "mi-006",
        "narrative_id": "nar-001",
        "indicator_type": "source_concentration",
        "confidence": 0.58,
        "detected_at": "2026-03-14T11:00:00Z",
        "evidence_summary": "2 source domains account for 73% of all signal volume, significantly exceeding baseline concentration",
        "flagged_signals": ["sig-001", "sig-002", "sig-004", "sig-005"],
        "status": "under_review",
    },
]

# ---------------------------------------------------------------------------
# In-memory credit store — resets on server restart (C4 will persist to DB).
# ---------------------------------------------------------------------------
_user_credits: dict = {
    "user_id": "user-001",
    "balance": 5,
    "total_purchased": 20,
    "total_used": 15,
}

# ---------------------------------------------------------------------------
# In-memory subscription store (C4 — toggleable stub).
# ---------------------------------------------------------------------------
_subscription: dict = {
    "user_id": "user-001",
    "subscribed": False,
}

STUB_AUTH_TOKEN = "stub-auth-token"

app = FastAPI(title="Narrative Intelligence API", version="0.2.0")

_cors_raw = os.environ.get("CORS_ORIGINS", "http://localhost:3000")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_raw.split(",")],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type", "x-auth-token", "Authorization"],  # FUTURE: drop x-auth-token after cookie migration (H7)
)


@app.middleware("http")
async def _security_headers(request: Request, call_next):
    """M1: HSTS header + HTTP→HTTPS redirect in production."""
    if _ENVIRONMENT == "production":
        if request.headers.get("x-forwarded-proto") == "http":
            https_url = request.url.replace(scheme="https")
            return RedirectResponse(url=str(https_url), status_code=301)
    response = await call_next(request)
    if _ENVIRONMENT == "production":
        response.headers["Strict-Transport-Security"] = (
            "max-age=3600; includeSubDomains"
        )
    return response


# ---------------------------------------------------------------------------
# Shared thread-pool executors (C3 — avoid per-request overhead / thread explosion)
# ---------------------------------------------------------------------------
_BG_EXECUTOR = ThreadPoolExecutor(max_workers=5)
_REQUEST_EXECUTOR = ThreadPoolExecutor(max_workers=10)

_REQUEST_TIMEOUT_SECONDS = 30.0


def _timeout(timeout: float = _REQUEST_TIMEOUT_SECONDS):
    """Decorator: wraps a sync endpoint in run_in_executor with a wall-clock timeout.

    Place BELOW @app.get and @limiter.limit decorators. The endpoint body runs
    in the default executor (not _REQUEST_EXECUTOR) to avoid deadlocks when it
    internally submits to _REQUEST_EXECUTOR. Returns 504 on timeout.
    """
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            loop = asyncio.get_running_loop()
            try:
                return await asyncio.wait_for(
                    loop.run_in_executor(
                        None, functools.partial(fn, *args, **kwargs)
                    ),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                raise HTTPException(
                    status_code=504, detail="Computation timed out"
                )
        # Preserve original signature so FastAPI resolves query params + slowapi
        # finds the Request parameter correctly.
        wrapper.__signature__ = inspect.signature(fn)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Rate limiter (C2 — tiered IP-based rate limiting via slowapi)
# ---------------------------------------------------------------------------
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["100/minute"],
    enabled=os.environ.get("RATE_LIMIT_ENABLED", "1") != "0",
)
app.state.limiter = limiter


async def _custom_rate_limit_handler(request: Request, exc: RateLimitExceeded):
    """Return 429 with Retry-After header."""
    from starlette.responses import JSONResponse
    # All current limits are per-minute (60s window). If hourly/daily limits
    # are added, extract dynamically from request.state.view_rate_limit.
    retry_after = 60
    return JSONResponse(
        status_code=429,
        content={"detail": f"Rate limit exceeded: {exc.detail}"},
        headers={"Retry-After": str(retry_after)},
    )


app.add_exception_handler(RateLimitExceeded, _custom_rate_limit_handler)


# ---------------------------------------------------------------------------
# Startup: replace placeholder narrative IDs in NARRATIVE_ASSETS with real DB IDs
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def _init_narrative_asset_ids():
    """Replace 'nar-001/002/003' placeholders with real narrative IDs from DB."""
    repo = get_repo()
    if repo is None:
        return
    try:
        rows = repo.get_all_active_narratives()
        rows.sort(key=lambda r: float(r.get("ns_score") or 0.0), reverse=True)
        real_ids = [r["narrative_id"] for r in rows[:3]]
        id_map = {}
        for i, placeholder in enumerate(["nar-001", "nar-002", "nar-003"]):
            if i < len(real_ids):
                id_map[placeholder] = real_ids[i]
        for na in NARRATIVE_ASSETS:
            if na["narrative_id"] in id_map:
                na["narrative_id"] = id_map[na["narrative_id"]]
    except Exception as e:
        print(f"[D1] Warning: could not initialize narrative IDs from DB: {e}")


_DYNAMIC_SECURITIES: dict[str, dict] = {}


@app.on_event("startup")
async def start_price_refresh():
    """Start background Finnhub price refresh loop if API key is configured.

    Eagerly discovers dynamic tickers so they appear on first page load.
    The refresh loop handles price fetching immediately on its first iteration.
    """
    global _DYNAMIC_SECURITIES
    _DYNAMIC_SECURITIES = _discover_linked_tickers()
    if _DYNAMIC_SECURITIES:
        print(f"[Startup] Discovered {len(_DYNAMIC_SECURITIES)} dynamic tickers: "
              f"{list(_DYNAMIC_SECURITIES.keys())[:10]}...")

    if finnhub.is_enabled():
        asyncio.create_task(_price_refresh_loop())
    else:
        print("[Finnhub] API key not set — price refresh disabled")

    # Finding 8: warn when ingester flags are on but keys are missing
    if os.environ.get("ENABLE_MARKETAUX", "true").lower() not in ("false", "0") \
            and not os.environ.get("MARKETAUX_API_KEY", ""):
        logger.info("MarketAux enabled but MARKETAUX_API_KEY not set — ingester will be inactive")
    if os.environ.get("ENABLE_NEWSDATA", "true").lower() not in ("false", "0") \
            and not os.environ.get("NEWSDATA_API_KEY", ""):
        logger.info("NewsData enabled but NEWSDATA_API_KEY not set — ingester will be inactive")

# Crypto symbol mapping for Finnhub (moved to module level to avoid per-cycle recreation)
_CRYPTO_MAP = {
    "BTC-USD": "BINANCE:BTCUSDT",
    "ETH-USD": "BINANCE:ETHUSDT",
    "AVAX-USD": "BINANCE:AVAXUSDT",
    "SOL-USD": "BINANCE:SOLUSDT",
    "MATIC-USD": "BINANCE:MATICUSDT",
    "ADA-USD": "BINANCE:ADAUSDT",
    "DOT-USD": "BINANCE:DOTUSDT",
    "LINK-USD": "BINANCE:LINKUSDT",
    "ATOM-USD": "BINANCE:ATOMUSDT",
    "UNI-USD": "BINANCE:UNIUSDT",
}


def _discover_linked_tickers() -> dict[str, dict]:
    """Scan all active narratives for linked_assets tickers, return as securities
    with impact scores computed from narrative count × similarity."""
    repo = get_repo()
    if repo is None:
        return {}
    narratives = repo.get_all_active_narratives()
    static_symbols = {s["symbol"] for s in TRACKED_SECURITIES}

    # Accumulate per-ticker: narrative count, max similarity, sum of similarities, entropy
    ticker_data: dict[str, dict] = {}
    nar_entropy: dict[str, float] = {
        n.get("narrative_id", ""): float(n.get("entropy") or 0.5)
        for n in narratives
    }

    for n in narratives:
        la_raw = n.get("linked_assets")
        if not la_raw:
            continue
        try:
            assets = json.loads(la_raw) if isinstance(la_raw, str) else la_raw
        except Exception:
            continue
        nid = n.get("narrative_id", "")
        for a in assets:
            ticker = a.get("ticker", "")
            if not ticker or ticker.startswith("TOPIC:") or ticker in static_symbols:
                continue
            sim = float(a.get("similarity_score", 0))
            if ticker not in ticker_data:
                ticker_data[ticker] = {
                    "name": a.get("asset_name", ticker),
                    "max_sim": sim,
                    "sim_sum": sim,
                    "narrative_count": 1,
                    "entropy_sum": nar_entropy.get(nid, 0.5),
                }
            else:
                td = ticker_data[ticker]
                td["max_sim"] = max(td["max_sim"], sim)
                td["sim_sum"] += sim
                td["narrative_count"] += 1
                td["entropy_sum"] += nar_entropy.get(nid, 0.5)

    if not ticker_data:
        return {}

    # Compute impact scores using same formula as static:
    # (narrative_count_norm × 0.40) + (avg_similarity × 0.35) + (avg_entropy × 0.25)
    max_nc = max(td["narrative_count"] for td in ticker_data.values()) or 1
    raw_scores: dict[str, float] = {}
    for ticker, td in ticker_data.items():
        nc_norm = td["narrative_count"] / max_nc
        avg_sim = td["sim_sum"] / td["narrative_count"]
        avg_ent = td["entropy_sum"] / td["narrative_count"]
        raw_scores[ticker] = nc_norm * 0.40 + avg_sim * 0.35 + avg_ent * 0.25

    min_raw = min(raw_scores.values())
    max_raw = max(raw_scores.values())
    span = max(max_raw - min_raw, 0.001)

    result: dict[str, dict] = {}
    for ticker, td in ticker_data.items():
        score = round(1 + (raw_scores[ticker] - min_raw) / span * 99)
        result[ticker] = {
            "id": f"dyn-{ticker}",
            "symbol": ticker,
            "name": td["name"],
            "asset_class_id": "ac-dynamic",
            "exchange": "—",
            "current_price": None,
            "price_change_24h": None,
            "narrative_impact_score": score,
            "similarity_score": round(td["max_sim"], 3),
            "dynamic": True,
        }
    return result


async def _price_refresh_loop():
    """
    Background task: fetch Finnhub quotes for TRACKED_SECURITIES + dynamic tickers.
    Runs every FINNHUB_REFRESH_INTERVAL_SECONDS.
    """
    global _DYNAMIC_SECURITIES
    interval = int(os.environ.get("FINNHUB_REFRESH_INTERVAL_SECONDS", "300"))
    while True:
        try:
            # Discover dynamic tickers from linked_assets, preserving
            # existing prices so a 429 or timeout doesn't wipe them out
            fresh = _discover_linked_tickers()
            with _securities_lock:
                for sym, sec in fresh.items():
                    existing = _DYNAMIC_SECURITIES.get(sym)
                    if existing is not None:
                        sec["current_price"] = existing.get("current_price")
                        sec["price_change_24h"] = existing.get("price_change_24h")
                        sec["_previous_close"] = existing.get("_previous_close")
                _DYNAMIC_SECURITIES = fresh

            # Update WS relay subscriptions with latest symbol set
            if _ws_relay is not None:
                all_syms = [s["symbol"] for s in TRACKED_SECURITIES] + list(
                    _DYNAMIC_SECURITIES.keys()
                )
                _ws_relay.update_symbols(all_syms)

            # Skip symbols already covered by WebSocket relay
            ws_active = (
                _ws_relay.get_active_symbols() if _ws_relay is not None else set()
            )

            # Build fetch list with Finnhub-compatible symbols
            fetch_symbols: list[str] = []
            symbol_remap: dict[str, str] = {}  # finnhub_sym → original_sym
            for s in list(TRACKED_SECURITIES) + list(_DYNAMIC_SECURITIES.values()):
                sym = s["symbol"]
                if sym in ws_active:
                    continue  # Real-time via WebSocket — skip REST poll
                fh_sym = _CRYPTO_MAP.get(sym, sym)
                if sym.endswith("-USD") and sym not in _CRYPTO_MAP:
                    # Auto-generate Binance mapping for unmapped crypto
                    base = sym.replace("-USD", "")
                    fh_sym = f"BINANCE:{base}USDT"
                fetch_symbols.append(fh_sym)
                symbol_remap[fh_sym] = sym

            quotes = await asyncio.get_running_loop().run_in_executor(
                None, finnhub.fetch_quotes_batch, fetch_symbols
            )

            # Reverse-map quotes back to original symbols
            mapped_quotes: dict[str, dict] = {}
            for fh_sym, q in quotes.items():
                orig = symbol_remap.get(fh_sym, fh_sym)
                mapped_quotes[orig] = q

            # price_change_24h is a PERCENTAGE (e.g., 2.5 means +2.5%), not a dollar amount.
            def _apply_quote(sec: dict, quote: dict | None) -> None:
                if quote is None:
                    return
                with _securities_lock:
                    price = quote.get("c")
                    prev_close = quote.get("pc")
                    sec["current_price"] = float(price) if price and price != 0 else None
                    if prev_close:
                        sec["_previous_close"] = float(prev_close)
                    if price and prev_close and prev_close != 0:
                        sec["price_change_24h"] = round((float(price) - float(prev_close)) / float(prev_close) * 100, 2)
                    else:
                        change = quote.get("d")
                        if change is not None and price and price != 0:
                            sec["price_change_24h"] = round(float(change) / float(price) * 100, 2)
                        else:
                            sec["price_change_24h"] = None

            # Update static securities
            for sec in TRACKED_SECURITIES:
                _apply_quote(sec, mapped_quotes.get(sec["symbol"]))

            # Update dynamic securities
            for symbol, sec in _DYNAMIC_SECURITIES.items():
                _apply_quote(sec, mapped_quotes.get(symbol))

            # Phase 2: fallback through DataNormalizer for symbols Finnhub missed
            if os.environ.get("ENABLE_TWELVE_DATA", "").lower() == "true" or \
               os.environ.get("ENABLE_COINGECKO", "").lower() == "true":
                all_symbols = (
                    {sec["symbol"] for sec in TRACKED_SECURITIES}
                    | set(_DYNAMIC_SECURITIES.keys())
                )
                successful = {
                    sym for sym, q in mapped_quotes.items()
                    if q is not None and q.get("c") and q.get("c") != 0
                }
                missed_symbols = list(all_symbols - successful)
                if missed_symbols:
                    try:
                        fallback_quotes = await asyncio.get_running_loop().run_in_executor(
                            None, data_normalizer.get_quotes_batch, missed_symbols
                        )
                        def _apply_normalized(sec: dict, nq: NormalizedQuote | None) -> None:
                            if nq is None:
                                return
                            sec["current_price"] = nq.price
                            sec["source"] = nq.source
                            if nq.close and nq.close != 0:
                                sec["price_change_24h"] = round(
                                    (nq.price - nq.close) / nq.close * 100, 2
                                )
                            else:
                                sec["price_change_24h"] = None

                        for sec in TRACKED_SECURITIES:
                            nq = fallback_quotes.get(sec["symbol"])
                            if nq:
                                _apply_normalized(sec, nq)
                        for symbol, sec in _DYNAMIC_SECURITIES.items():
                            nq = fallback_quotes.get(symbol)
                            if nq:
                                _apply_normalized(sec, nq)
                    except Exception as e:
                        print(f"[DataNormalizer] Fallback refresh error: {e}")
        except Exception as e:
            print(f"[Finnhub] Price refresh error: {e}")
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# D3 — Narrative Impact Score calculation
# ---------------------------------------------------------------------------

def calculate_narrative_impact_scores(
    securities: list[dict],
    narrative_assets: list[dict],
    narratives: list[dict],
) -> dict[str, int]:
    """
    Returns dict mapping security id → impact score (1–100).

    Per-security scoring uses asset-class-level narrative signals PLUS
    per-security differentiators (direct ticker mentions in linked_assets,
    price volatility) to break ties within the same sector.
    """
    # Build narrative id → entropy lookup
    nar_entropy: dict[str, float] = {}
    for n in narratives:
        nid = n.get("narrative_id") or n.get("id") or ""
        nar_entropy[nid] = float(n.get("entropy") or 0.5)

    # Build per-ticker direct mention counts from linked_assets
    ticker_mention_count: dict[str, int] = {}
    ticker_max_sim: dict[str, float] = {}
    for n in narratives:
        la_raw = n.get("linked_assets")
        if not la_raw:
            continue
        try:
            assets = json.loads(la_raw) if isinstance(la_raw, str) else la_raw
        except Exception:
            continue
        for a in assets:
            t = a.get("ticker", "")
            if t and not t.startswith("TOPIC:"):
                ticker_mention_count[t] = ticker_mention_count.get(t, 0) + 1
                sim = float(a.get("similarity_score", 0))
                ticker_max_sim[t] = max(ticker_max_sim.get(t, 0), sim)

    # Per-asset-class aggregates
    ac_data: dict[str, dict] = {}
    for na in narrative_assets:
        ac_id = na["asset_class_id"]
        nar_id = na["narrative_id"]
        if ac_id not in ac_data:
            ac_data[ac_id] = {"narrative_ids": set(), "exposure_scores": [], "entropies": []}
        ac_data[ac_id]["narrative_ids"].add(nar_id)
        ac_data[ac_id]["exposure_scores"].append(na["exposure_score"])
        ac_data[ac_id]["entropies"].append(nar_entropy.get(nar_id, 0.5))

    max_nc = max((len(d["narrative_ids"]) for d in ac_data.values()), default=1) or 1
    max_mentions = max(ticker_mention_count.values(), default=1) or 1

    # Compute raw scores per security with per-ticker differentiation
    raw_scores: dict[str, float] = {}
    for sec in securities:
        sec_id = sec["id"]
        ac_id = sec["asset_class_id"]
        symbol = sec["symbol"]

        # Base score from asset class
        if ac_id in ac_data and ac_data[ac_id]["narrative_ids"]:
            d = ac_data[ac_id]
            nc_norm = len(d["narrative_ids"]) / max_nc
            avg_exp = sum(d["exposure_scores"]) / len(d["exposure_scores"])
            avg_ent = sum(d["entropies"]) / len(d["entropies"])
            base = nc_norm * 0.30 + avg_exp * 0.25 + avg_ent * 0.15
        else:
            base = 0.0

        # Per-security bonus: direct ticker mentions in linked_assets
        mentions = ticker_mention_count.get(symbol, 0)
        mention_norm = mentions / max_mentions
        sim_bonus = ticker_max_sim.get(symbol, 0)

        # Per-security bonus: price volatility (more volatile = more narrative impact)
        change = abs(float(sec.get("price_change_24h") or 0))
        vol_bonus = min(change / 10.0, 1.0)  # normalize: 10% change = max bonus

        raw_scores[sec_id] = base + mention_norm * 0.15 + sim_bonus * 0.10 + vol_bonus * 0.05

    if not raw_scores:
        return {}

    min_raw = min(raw_scores.values())
    max_raw = max(raw_scores.values())
    span = max(max_raw - min_raw, 0.001)

    return {
        sec_id: round(1 + (score - min_raw) / span * 99)
        for sec_id, score in raw_scores.items()
    }


@app.on_event("startup")
async def start_impact_score_refresh():
    """Start background narrative impact score computation loop."""
    asyncio.create_task(_impact_score_loop())


async def _impact_score_loop():
    """
    Background task: compute narrative impact scores for TRACKED_SECURITIES.
    Updates narrative_impact_score in-place. Runs immediately at startup then
    every IMPACT_SCORE_REFRESH_SECONDS.
    """
    interval = int(os.environ.get("IMPACT_SCORE_REFRESH_SECONDS", "600"))
    while True:
        try:
            narratives_for_scoring: list[dict] = []
            repo = get_repo()
            if repo is not None:
                try:
                    narratives_for_scoring = repo.get_all_active_narratives()
                except Exception:
                    pass
            if not narratives_for_scoring:
                # Fallback: stub narratives with default entropy
                stub_ids = list({na["narrative_id"] for na in NARRATIVE_ASSETS})
                narratives_for_scoring = [
                    {"narrative_id": nid, "entropy": 0.5} for nid in stub_ids
                ]
            scores = calculate_narrative_impact_scores(
                TRACKED_SECURITIES, NARRATIVE_ASSETS, narratives_for_scoring
            )
            for sec in TRACKED_SECURITIES:
                sec["narrative_impact_score"] = scores.get(sec["id"], 0)
        except Exception as e:
            print(f"[D3] Impact score refresh error: {e}")
        await asyncio.sleep(interval)


@app.on_event("startup")
async def start_analytics_bg_refresh():
    """Start background analytics pre-computation loop (lead-time + contrarian)."""
    asyncio.create_task(_analytics_bg_loop())


# ---------------------------------------------------------------------------
# Phase 2 Batch 4 — WebSocket relay + tick storage
# ---------------------------------------------------------------------------

def _ws_price_update_callback(symbol: str, price: float, volume, ts_iso: str):
    """Called by the WS relay on each incoming trade. Updates in-memory prices."""
    with _securities_lock:
        for sec in TRACKED_SECURITIES:
            if sec["symbol"] == symbol:
                sec["current_price"] = price
                prev_close = sec.get("_previous_close")
                if prev_close and prev_close != 0:
                    sec["price_change_24h"] = round((price - prev_close) / prev_close * 100, 2)
                return
        dyn = _DYNAMIC_SECURITIES.get(symbol)
        if dyn:
            dyn["current_price"] = price
            prev_close = dyn.get("_previous_close")
            if prev_close and prev_close != 0:
                dyn["price_change_24h"] = round((price - prev_close) / prev_close * 100, 2)


@app.on_event("startup")
async def start_websocket_relay():
    """Start Finnhub WebSocket relay if API key is configured."""
    global _ws_relay
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        print("[WS Relay] API key not set — WebSocket relay disabled")
        return

    symbols_limit = int(os.environ.get("WEBSOCKET_SYMBOLS_LIMIT", "50"))
    flush_interval = int(os.environ.get("WEBSOCKET_FLUSH_INTERVAL_SECONDS", "5"))
    reconnect_max = int(os.environ.get("WEBSOCKET_RECONNECT_MAX_DELAY_SECONDS", "300"))

    _ws_relay = FinnhubWebSocketRelay(
        api_key=api_key,
        symbols_limit=symbols_limit,
        flush_interval=flush_interval,
        reconnect_max_delay=reconnect_max,
    )

    # Build initial symbol list from TRACKED_SECURITIES
    initial_symbols = [s["symbol"] for s in TRACKED_SECURITIES]
    _ws_relay.update_symbols(initial_symbols)

    asyncio.create_task(_ws_relay.start(update_callback=_ws_price_update_callback))
    asyncio.create_task(_tick_flush_loop())
    asyncio.create_task(_tick_retention_loop())
    print(f"[WS Relay] Started — subscribing to up to {symbols_limit} symbols")


@app.on_event("startup")
async def _init_sse():
    global _sse_lock
    _sse_lock = asyncio.Lock()
    asyncio.create_task(_sse_broadcast_loop())


async def _sse_broadcast_loop():
    """Single background task updates shared ticker payload every 8s."""
    global _latest_ticker_payload
    while True:
        try:
            _latest_ticker_payload = {"type": "ticker-update", "items": _ticker_payload()}
        except Exception:
            pass  # stale payload is better than crash
        await asyncio.sleep(8)


@app.on_event("startup")
async def _init_login_cleanup():
    asyncio.create_task(_cleanup_login_attempts())


async def _cleanup_login_attempts():
    """Periodically purge stale login attempt records every 5 minutes."""
    while True:
        await asyncio.sleep(300)
        now = _time.time()
        stale = [e for e, ts in _login_attempts.items()
                 if all(now - t >= _LOGIN_WINDOW_SECONDS for t in ts)]
        for e in stale:
            _login_attempts.pop(e, None)


@app.on_event("startup")
async def start_sentiment_refresh():
    """Start background sentiment aggregation loop (Part C)."""
    global _sentiment_aggregator
    repo = get_repo()
    if repo is not None:
        _sentiment_aggregator = SentimentAggregator(repo, _stocktwits_adapter, finnhub)
    asyncio.create_task(_sentiment_refresh_loop())


@app.on_event("startup")
async def start_alert_engine():
    """Run alert rule evaluation every 30 seconds."""
    async def _alert_loop():
        while True:
            try:
                repo = get_repo()
                if repo is not None:
                    securities = {s["symbol"]: s for s in TRACKED_SECURITIES}
                    nm = NotificationManager(repo)
                    nm.check_rules(securities=securities)
            except Exception as e:
                print(f"[AlertEngine] Error: {e}")
            await asyncio.sleep(30)
    asyncio.create_task(_alert_loop())


@app.on_event("startup")
async def _check_db_permissions():
    db_path = Path(DB_PATH)
    if db_path.exists() and os.name != "nt":  # Skip on Windows
        current = stat.S_IMODE(db_path.stat().st_mode)
        if current & (stat.S_IRGRP | stat.S_IROTH):
            logger.warning(
                "Database file %s is world/group-readable (mode %o). Setting to 600.",
                DB_PATH,
                current,
            )
            os.chmod(str(db_path), stat.S_IRUSR | stat.S_IWUSR)


@app.on_event("startup")
async def _init_data_normalizer_repo():
    """Wire repository into DataNormalizer for price API usage tracking."""
    repo = get_repo()
    if repo is not None:
        data_normalizer._repository = repo


async def _sentiment_refresh_loop():
    """
    Background task: fetch social sentiment and store composite scores.

    - Every 15 min: fetch StockTwits data for tracked securities, store social mentions
    - Every 60 min: compute and store composite sentiment for all tracked tickers
    """
    global _sentiment_aggregator
    social_interval = int(os.environ.get("SENTIMENT_SOCIAL_REFRESH_SECONDS", "900"))   # 15 min
    composite_interval = int(os.environ.get("SENTIMENT_COMPOSITE_REFRESH_SECONDS", "3600"))  # 60 min
    last_composite = 0.0

    while True:
        repo = get_repo()
        if repo is not None and _sentiment_aggregator is None:
            _sentiment_aggregator = SentimentAggregator(repo, _stocktwits_adapter, finnhub)

        if repo is not None and _sentiment_aggregator is not None:
            tickers = [s["symbol"] for s in TRACKED_SECURITIES]
            try:
                # Social mentions pass
                for ticker in tickers:
                    data = _stocktwits_adapter.get_sentiment(ticker)
                    if data:
                        repo.insert_social_mention(ticker, "stocktwits", {
                            "mention_count": data.get("total_messages", 0),
                            "bullish_count": data.get("bullish_count", 0),
                            "bearish_count": data.get("bearish_count", 0),
                        })
            except Exception as e:
                print(f"[Sentiment] Social mentions refresh error: {e}")

            now_ts = _time.time()
            if now_ts - last_composite >= composite_interval:
                try:
                    for ticker in tickers:
                        result = _sentiment_aggregator.compute_ticker_sentiment(ticker)
                        repo.insert_sentiment_record(ticker, result)
                    last_composite = _time.time()
                except Exception as e:
                    print(f"[Sentiment] Composite refresh error: {e}")

        await asyncio.sleep(social_interval)


@app.on_event("shutdown")
async def _shutdown_executors():
    """Cleanly shut down shared thread-pool executors."""
    _BG_EXECUTOR.shutdown(wait=False)
    _REQUEST_EXECUTOR.shutdown(wait=False)


async def _tick_flush_loop():
    """Flush buffered WebSocket ticks to price_ticks table periodically."""
    flush_interval = int(os.environ.get("WEBSOCKET_FLUSH_INTERVAL_SECONDS", "5"))
    await asyncio.sleep(5)  # Let relay connect first

    while True:
        try:
            if _ws_relay is not None:
                ticks = _ws_relay.drain_tick_buffer()
                if ticks:
                    repo = get_repo()
                    if repo is not None:
                        count = repo.insert_ticks_batch(ticks)
                        if count > 0:
                            print(f"[Tick Flush] Wrote {count} ticks to DB")
        except Exception as e:
            print(f"[Tick Flush] Error: {e}")
        await asyncio.sleep(flush_interval)


async def _tick_retention_loop():
    """Aggregate old ticks into 1-min candles, then prune. Runs every hour."""
    await asyncio.sleep(120)  # Let system stabilize first

    while True:
        try:
            retention_hours = int(os.environ.get("PRICE_TICK_RETENTION_HOURS", "48"))
            from datetime import datetime as _dt, timezone as _tz, timedelta as _td
            cutoff = (_dt.now(_tz.utc) - _td(hours=retention_hours)).isoformat()

            repo = get_repo()
            if repo is not None:
                candles = repo.aggregate_candles_1m(cutoff)
                pruned = repo.prune_old_ticks(cutoff)
                if candles > 0 or pruned > 0:
                    print(
                        f"[Tick Retention] Aggregated {candles} candles, "
                        f"pruned {pruned} ticks (cutoff: {retention_hours}h)"
                    )
        except Exception as e:
            print(f"[Tick Retention] Error: {e}")
        await asyncio.sleep(3600)  # Every hour


async def _analytics_bg_loop():
    """
    Background task: pre-compute lead-time distribution and contrarian signals.
    Both require yfinance price lookups. Single loop avoids double-fetching.
    Runs every ANALYTICS_BG_REFRESH_SECONDS (default 4h).
    """
    interval = int(os.environ.get("ANALYTICS_BG_REFRESH_SECONDS", "14400"))
    await asyncio.sleep(60)  # Let price refresh populate first

    while True:
        try:
            repo = get_repo()
            if repo is None:
                await asyncio.sleep(interval)
                continue

            from stock_data import get_price_history as _yf_price_history

            all_narratives = repo.get_all_active_narratives()
            first_dates = repo.get_first_snapshot_dates()

            # Collect all unique tickers from linked_assets
            all_tickers: set[str] = set()
            for n in all_narratives:
                for a in _parse_linked_assets(n):
                    t = a.get("ticker", "") if isinstance(a, dict) else ""
                    if t and not t.startswith("TOPIC:"):
                        all_tickers.add(t.upper())

            # Fetch price histories in parallel (shared BG executor, 15s timeout)
            price_data: dict[str, list] = {}
            futures = {
                _BG_EXECUTOR.submit(_yf_price_history, t, 90): t
                for t in all_tickers
            }
            for future in futures:
                ticker = futures[future]
                try:
                    data = future.result(timeout=15)
                    if data:
                        price_data[ticker] = data
                except Exception:
                    pass

            _compute_lead_time_cache(all_narratives, first_dates, price_data)
            _compute_contrarian_cache(repo, all_narratives, price_data)
            print(
                f"[Analytics BG] Refreshed — {len(price_data)} tickers, "
                f"{len(_lead_time_cache)} thresholds, "
                f"{len(_contrarian_cache.get('signals', []))} contrarian signals"
            )
        except Exception as e:
            print(f"[Analytics BG] Error: {e}")
        await asyncio.sleep(interval)


def _compute_lead_time_cache(
    all_narratives: list[dict],
    first_dates: dict[str, str],
    price_data: dict[str, list],
) -> None:
    """Compute lead-time distribution for multiple thresholds and cache."""
    import time as _time
    import statistics

    global _lead_time_cache, _lead_time_cache_at

    for threshold in [1.0, 2.0, 5.0]:
        data_points: list[dict] = []

        for n in all_narratives:
            stage = n.get("stage", "Emerging")
            if stage == "Emerging":
                continue
            nid = n.get("narrative_id", "")
            detection_date = first_dates.get(nid)
            if not detection_date:
                continue

            for a in _parse_linked_assets(n):
                ticker = (
                    a.get("ticker", "") if isinstance(a, dict) else ""
                ).upper()
                if (
                    not ticker
                    or ticker.startswith("TOPIC:")
                    or ticker not in price_data
                ):
                    continue

                prices = price_data[ticker]
                detection_close = None
                lead_days = None

                for p in prices:
                    if p["date"] < detection_date:
                        detection_close = p["close"]
                        continue
                    if detection_close is None:
                        detection_close = p["close"]
                        continue
                    if detection_close == 0:
                        continue
                    cum_change = abs(
                        (p["close"] - detection_close) / detection_close * 100
                    )
                    if cum_change >= threshold:
                        try:
                            d_det = date.fromisoformat(detection_date[:10])
                            d_move = date.fromisoformat(p["date"][:10])
                            lead_days = (d_move - d_det).days
                        except Exception:
                            lead_days = 0
                        break

                price_change_pct = None
                if detection_close and detection_close != 0 and prices:
                    last_close = prices[-1]["close"]
                    price_change_pct = round(
                        (last_close - detection_close) / detection_close * 100, 2
                    )

                data_points.append(
                    {
                        "narrative_id": nid,
                        "ticker": ticker,
                        "lead_days": lead_days,
                        "price_change_pct": price_change_pct,
                    }
                )

        # Bucket into histogram
        buckets = [
            {"range": "0-1 days", "min": 0, "max": 1, "count": 0},
            {"range": "2-3 days", "min": 2, "max": 3, "count": 0},
            {"range": "4-7 days", "min": 4, "max": 7, "count": 0},
            {"range": "8-14 days", "min": 8, "max": 14, "count": 0},
            {"range": "15-30 days", "min": 15, "max": 30, "count": 0},
            {"range": "No move", "min": None, "max": None, "count": 0},
        ]
        valid_leads: list[int] = []
        for dp in data_points:
            ld = dp["lead_days"]
            if ld is None:
                buckets[-1]["count"] += 1
            else:
                valid_leads.append(ld)
                for b in buckets[:-1]:
                    if b["min"] <= ld <= b["max"]:
                        b["count"] += 1
                        break

        histogram = [{"range": b["range"], "count": b["count"]} for b in buckets]
        total = len(data_points)
        hit_count = len(valid_leads)
        median_lead = (
            statistics.median(valid_leads) if valid_leads else 0
        )
        mean_lead = (
            round(sum(valid_leads) / len(valid_leads), 1) if valid_leads else 0
        )
        hit_rate = round(hit_count / total, 2) if total > 0 else 0.0

        _lead_time_cache[str(threshold)] = {
            "data_points": data_points,
            "histogram_buckets": histogram,
            "median_lead_days": median_lead,
            "mean_lead_days": mean_lead,
            "hit_rate": hit_rate,
        }

    _lead_time_cache_at = _time.time()


def _compute_contrarian_cache(
    repo, all_narratives: list[dict], price_data: dict[str, list]
) -> None:
    """Compute contrarian signals from coordination-flagged narratives."""
    import time as _time

    global _contrarian_cache, _contrarian_cache_at

    signals: list[dict] = []
    coordinated = [
        n
        for n in all_narratives
        if n.get("is_coordinated")
        or int(n.get("coordination_flag_count") or 0) > 0
    ]

    for n in coordinated:
        nid = n.get("narrative_id", "")
        events = repo.get_adversarial_events(narrative_id=nid, limit=10)
        if not events:
            continue

        coord_events: list[dict] = []
        for e in events:
            domains_raw = e.get("source_domains", "")
            try:
                domains = (
                    json.loads(domains_raw)
                    if isinstance(domains_raw, str)
                    else domains_raw
                )
            except Exception:
                domains = [domains_raw] if domains_raw else []
            coord_events.append(
                {
                    "detected_at": e.get("detected_at"),
                    "source_domains": (
                        domains if isinstance(domains, list) else [str(domains)]
                    ),
                    "similarity_score": round(
                        float(e.get("similarity_score") or 0), 4
                    ),
                }
            )

        # Velocity: oldest snapshot vs current
        snapshots = repo.get_snapshot_history(nid, 90)
        velocity_now = float(
            n.get("velocity_windowed") or n.get("velocity") or 0
        )
        velocity_at_det = velocity_now
        if snapshots and len(snapshots) > 1:
            velocity_at_det = float(snapshots[-1].get("velocity") or 0)

        # Price enrichment for linked assets
        earliest_det = None
        if coord_events:
            try:
                earliest_det = min(
                    e["detected_at"]
                    for e in coord_events
                    if e["detected_at"]
                )
            except Exception:
                pass

        enriched_assets: list[dict] = []
        for a in _parse_linked_assets(n):
            ticker = (
                a.get("ticker", "") if isinstance(a, dict) else ""
            ).upper()
            sim = float(
                a.get("similarity_score", 0) if isinstance(a, dict) else 0
            )
            if (
                not ticker
                or ticker.startswith("TOPIC:")
                or ticker not in price_data
            ):
                continue

            prices = price_data[ticker]
            price_at_det = None
            price_now = prices[-1]["close"] if prices else None

            if earliest_det and prices:
                det_date = earliest_det[:10]
                for p in prices:
                    if p["date"] >= det_date:
                        price_at_det = p["close"]
                        break

            change_pct = None
            if price_at_det and price_now and price_at_det != 0:
                change_pct = round(
                    (price_now - price_at_det) / price_at_det * 100, 2
                )

            enriched_assets.append(
                {
                    "ticker": ticker,
                    "price_at_detection": price_at_det,
                    "price_now": price_now,
                    "price_change_pct": change_pct,
                    "similarity_score": round(sim, 4),
                }
            )

        signals.append(
            {
                "narrative_id": nid,
                "name": n.get("name", ""),
                "stage": n.get("stage", ""),
                "ns_score": round(float(n.get("ns_score") or 0), 4),
                "coordination_events": coord_events,
                "linked_assets": enriched_assets,
                "velocity_at_detection": round(velocity_at_det, 4),
                "velocity_now": round(velocity_now, 4),
                "velocity_sustained": abs(velocity_now)
                >= abs(velocity_at_det) * 0.5,
            }
        )

    _contrarian_cache["signals"] = signals
    _contrarian_cache_at = _time.time()


# ---------------------------------------------------------------------------
# Cached repo instance — avoids repeated schema migrations on every request.
# ---------------------------------------------------------------------------
_repo_instance = None


def get_repo():
    global _repo_instance
    if _repo_instance is not None:
        return _repo_instance
    if not Path(DB_PATH).exists():
        return None
    from repository import SqliteRepository
    repo = SqliteRepository(DB_PATH)
    repo.migrate()
    _repo_instance = repo
    return repo


# ---------------------------------------------------------------------------
# Auth dependency — every user-specific endpoint uses Depends(get_current_user)
# ---------------------------------------------------------------------------
def _decode_jwt(token: str) -> dict:
    """Decode and validate a JWT token. Returns payload or raises HTTPException."""
    import jwt  # PyJWT — lazy import so stub mode has no dependency
    secret = _get_jwt_secret()
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=403, detail="Invalid authentication token")

    # Blacklist check — FAIL-CLOSED: if DB is unavailable, reject the token
    jti = payload.get("jti")
    if jti:
        repo = get_repo()
        if repo is None:
            raise HTTPException(status_code=503, detail="Database unavailable — cannot verify token")
        if repo.is_token_blacklisted(jti):
            raise HTTPException(status_code=401, detail="Token has been revoked")

    return payload


def get_current_user(request: Request, x_auth_token: Optional[str] = Header(None)) -> dict:
    """FastAPI dependency returning a user dict.
    AUTH_MODE=stub: validates STUB_AUTH_TOKEN, returns local user.
    AUTH_MODE=jwt: decodes JWT, returns user dict with real user_id.
    H7: Falls back to auth_token cookie if no header present."""
    # H7: Try header first, then cookie
    token = x_auth_token or request.cookies.get("auth_token")
    if token is None:
        raise HTTPException(status_code=403, detail="Authentication required")
    if _AUTH_MODE == "stub":
        if token != STUB_AUTH_TOKEN:
            raise HTTPException(status_code=403, detail="Invalid authentication token")
        return {"user_id": "local", "role": "user"}
    # JWT mode — M9: log token validation failures from caller
    try:
        payload = _decode_jwt(token)
    except HTTPException as e:
        try:
            repo = get_repo()
            if repo:
                repo.log_auth_event({
                    "event_type": "token_validation_failure",
                    "ip_address": _extract_ip(request),
                    "user_agent": request.headers.get("user-agent"),
                    "success": False,
                    "details": e.detail,
                })
        except Exception:
            pass
        raise
    return {
        "user_id": payload["sub"],
        "role": payload.get("role", "user"),
        "jti": payload.get("jti"),
        "exp": payload.get("exp"),
    }


def get_optional_user(request: Request, x_auth_token: Optional[str] = Header(None)) -> dict:
    """FastAPI dependency for endpoints that work without auth in single-user mode.
    AUTH_MODE=stub: no token = local user, bad token = 403.
    AUTH_MODE=jwt: token required, no anonymous access.
    H7: Falls back to auth_token cookie if no header present."""
    # H7: Try header first, then cookie
    token = x_auth_token or request.cookies.get("auth_token")
    if _AUTH_MODE == "stub":
        if token and token != STUB_AUTH_TOKEN:
            raise HTTPException(status_code=403, detail="Invalid authentication token")
        return {"user_id": "local", "role": "user"}
    # JWT mode — token is required
    if token is None:
        raise HTTPException(status_code=403, detail="Authentication required")
    # M9: log token validation failures from caller
    try:
        payload = _decode_jwt(token)
    except HTTPException as e:
        try:
            repo = get_repo()
            if repo:
                repo.log_auth_event({
                    "event_type": "token_validation_failure",
                    "ip_address": _extract_ip(request),
                    "user_agent": request.headers.get("user-agent"),
                    "success": False,
                    "details": e.detail,
                })
        except Exception:
            pass
        raise
    return {
        "user_id": payload["sub"],
        "role": payload.get("role", "user"),
        "jti": payload.get("jti"),
        "exp": payload.get("exp"),
    }


# L3: RBAC — factory that returns a FastAPI dependency checking user role
def require_role(required_role: str):
    """Factory: returns a FastAPI dependency that checks user role."""
    def _check(user: dict = Depends(get_current_user)) -> dict:
        if user.get("role") != required_role:
            raise HTTPException(
                status_code=403,
                detail=f"Requires role: {required_role}",
            )
        return user
    return _check


# ---------------------------------------------------------------------------
# Auth endpoints — only functional when AUTH_MODE=jwt
# ---------------------------------------------------------------------------
class SignupRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


@app.post("/api/auth/signup")
@limiter.limit("5/minute")
def auth_signup(request: Request, body: SignupRequest):
    """Create a new user account. Only active in JWT mode."""
    if _AUTH_MODE != "jwt":
        raise HTTPException(status_code=404, detail="Auth endpoints require AUTH_MODE=jwt")

    import bcrypt
    import jwt as pyjwt

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    import re
    _EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    email = body.email.lower().strip()
    if not email or not _EMAIL_RE.match(email):
        raise HTTPException(status_code=422, detail="Invalid email format")
    if not body.password or len(body.password) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters")

    # L1: Pre-hash with SHA-256 to handle passwords > 72 bytes (bcrypt truncates at 72)
    pw_bytes = hashlib.sha256(body.password.encode("utf-8")).digest()
    password_hash = bcrypt.hashpw(pw_bytes, bcrypt.gensalt()).decode("utf-8")

    # L2: Generate email verification token
    verification_token = secrets.token_urlsafe(32)

    user_id = str(uuid.uuid4())
    now_ts = datetime.now(timezone.utc)
    try:
        repo.create_user({
            "id": user_id,
            "email": email,
            "password_hash": password_hash,
            "email_verified": 0,
            "verification_token": verification_token,
            "created_at": now_ts.isoformat(),
        })
    except Exception as e:
        if "UNIQUE" in str(e).upper():
            raise HTTPException(status_code=409, detail="Email already registered")
        raise

    # L2: Log verification URL (email sending deferred — no SMTP/SES configured yet)
    logger.info("[Auth] Verification token for %s: /api/auth/verify?token=%s", email, verification_token)

    # M9: Log signup event
    try:
        repo.log_auth_event({
            "event_type": "signup",
            "email": email,
            "user_id": user_id,
            "ip_address": _extract_ip(request),
            "user_agent": request.headers.get("user-agent"),
            "success": True,
        })
    except Exception:
        pass  # Non-fatal — never block signup for audit logging failure

    secret = _get_jwt_secret()
    expiry_hours = int(os.environ.get("JWT_EXPIRY_HOURS", "2"))
    payload = {
        "jti": str(uuid.uuid4()),
        "sub": user_id,
        "email": email,
        "role": "user",
        "iat": now_ts,
        "exp": now_ts + timedelta(hours=expiry_hours),
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    # M2: Generate refresh token
    refresh_jti = str(uuid.uuid4())
    refresh_payload = {
        "jti": refresh_jti,
        "sub": user_id,
        "type": "refresh",
        "iat": now_ts,
        "exp": now_ts + timedelta(days=7),
    }
    refresh_token = pyjwt.encode(refresh_payload, secret, algorithm="HS256")
    repo.store_refresh_token(refresh_jti, user_id, (now_ts + timedelta(days=7)).isoformat())

    # H7: Set HttpOnly cookie + CSRF cookie
    from starlette.responses import JSONResponse
    response = JSONResponse({"user_id": user_id, "email": email, "token": token, "refresh_token": refresh_token})
    response.set_cookie(
        key="auth_token",
        value=token,
        httponly=True,
        secure=_IS_SECURE_ENV,
        samesite="strict",
        path="/api",
        max_age=expiry_hours * 3600,
    )
    csrf_token = _generate_csrf_token()
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=False,
        secure=_IS_SECURE_ENV,
        samesite="strict",
        path="/api",
        max_age=expiry_hours * 3600,
    )
    return response


@app.post("/api/auth/login")
@limiter.limit("5/minute")
def auth_login(request: Request, body: LoginRequest):
    """Authenticate and receive a JWT. Only active in JWT mode."""
    if _AUTH_MODE != "jwt":
        raise HTTPException(status_code=404, detail="Auth endpoints require AUTH_MODE=jwt")

    import bcrypt
    import jwt as pyjwt

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    email = body.email.lower().strip()

    # M10: Brute-force check
    now = _time.time()
    attempts = _login_attempts.get(email, [])
    attempts = [t for t in attempts if now - t < _LOGIN_WINDOW_SECONDS]
    _login_attempts[email] = attempts

    if len(attempts) >= _LOGIN_MAX_ATTEMPTS:
        raise HTTPException(
            status_code=429,
            detail="Too many login attempts. Try again in 15 minutes.",
            headers={"Retry-After": str(_LOGIN_WINDOW_SECONDS)},
        )

    # Progressive delay after 3 failures
    if len(attempts) >= 3:
        delay = min(2 ** (len(attempts) - 3), 30)
        _time.sleep(delay)

    user = repo.get_user_by_email(email)
    if not user:
        _login_attempts.setdefault(email, []).append(now)
        # M9: Log login failure
        try:
            repo.log_auth_event({
                "event_type": "login_failure",
                "email": email,
                "ip_address": _extract_ip(request),
                "user_agent": request.headers.get("user-agent"),
                "success": False,
                "details": "Invalid email or password",
            })
        except Exception:
            pass
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # L1: Dual-check — try new SHA-256 pre-hash first, fall back to old style (and rehash)
    pw_sha = hashlib.sha256(body.password.encode("utf-8")).digest()
    if bcrypt.checkpw(pw_sha, user["password_hash"].encode("utf-8")):
        pass  # New-style hash matches
    elif bcrypt.checkpw(body.password.encode("utf-8"), user["password_hash"].encode("utf-8")):
        # Old-style hash matches — migrate to new pre-hashed format
        new_hash = bcrypt.hashpw(pw_sha, bcrypt.gensalt()).decode("utf-8")
        try:
            repo.update_user_password_hash(user["id"], new_hash)
        except Exception:
            pass  # Non-fatal — migration will retry on next login
    else:
        _login_attempts.setdefault(email, []).append(now)
        # M9: Log login failure
        try:
            repo.log_auth_event({
                "event_type": "login_failure",
                "email": email,
                "ip_address": _extract_ip(request),
                "user_agent": request.headers.get("user-agent"),
                "success": False,
                "details": "Invalid email or password",
            })
        except Exception:
            pass
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # Clear attempts on successful login
    _login_attempts.pop(email, None)

    # M9: Log login success
    try:
        repo.log_auth_event({
            "event_type": "login_success",
            "email": user["email"],
            "user_id": user["id"],
            "ip_address": _extract_ip(request),
            "user_agent": request.headers.get("user-agent"),
            "success": True,
        })
    except Exception:
        pass

    secret = _get_jwt_secret()
    expiry_hours = int(os.environ.get("JWT_EXPIRY_HOURS", "2"))
    payload = {
        "jti": str(uuid.uuid4()),
        "sub": user["id"],
        "email": user["email"],
        "role": "user",
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    # M2: Generate refresh token
    now_ts = datetime.now(timezone.utc)
    refresh_jti = str(uuid.uuid4())
    refresh_payload = {
        "jti": refresh_jti,
        "sub": user["id"],
        "type": "refresh",
        "iat": now_ts,
        "exp": now_ts + timedelta(days=7),
    }
    refresh_token = pyjwt.encode(refresh_payload, secret, algorithm="HS256")
    repo.store_refresh_token(refresh_jti, user["id"], (now_ts + timedelta(days=7)).isoformat())

    # H7: Set HttpOnly cookie + CSRF cookie
    from starlette.responses import JSONResponse
    response = JSONResponse({"user_id": user["id"], "email": user["email"], "token": token, "refresh_token": refresh_token})
    response.set_cookie(
        key="auth_token",
        value=token,
        httponly=True,
        secure=_IS_SECURE_ENV,
        samesite="strict",
        path="/api",
        max_age=expiry_hours * 3600,
    )
    csrf_token = _generate_csrf_token()
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=False,
        secure=_IS_SECURE_ENV,
        samesite="strict",
        path="/api",
        max_age=expiry_hours * 3600,
    )
    return response


@app.get("/api/auth/verify")
def auth_verify_email(token: str = Query(...)):
    """Verify email address using the token sent during signup. Only active in JWT mode."""
    if _AUTH_MODE != "jwt":
        raise HTTPException(status_code=404, detail="Auth endpoints require AUTH_MODE=jwt")
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    user = repo.get_user_by_verification_token(token)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid verification token")
    repo.mark_email_verified(user["id"])
    return {"detail": "Email verified successfully"}


@app.get("/api/auth/me")
def auth_me(user: dict = Depends(get_current_user)):
    """Return current user info. Works in both stub and JWT mode."""
    if _AUTH_MODE == "stub":
        return {"user_id": "local", "role": "user", "auth_mode": "stub"}
    repo = get_repo()
    if repo is None:
        return {"user_id": user["user_id"], "role": user["role"], "auth_mode": "jwt"}
    db_user = repo.get_user_by_id(user["user_id"])
    return {
        "user_id": user["user_id"],
        "email": db_user["email"] if db_user else None,
        "role": user["role"],
        "auth_mode": "jwt",
        "created_at": db_user["created_at"] if db_user else None,
    }


@app.post("/api/auth/logout")
@limiter.limit("5/minute")
def auth_logout(request: Request, user: dict = Depends(get_current_user)):
    """Revoke the current JWT token by adding its jti to the blacklist."""
    if _AUTH_MODE != "jwt":
        # H7: Clear cookies even in stub mode
        from starlette.responses import JSONResponse
        response = JSONResponse({"detail": "Logged out (stub mode)"})
        response.delete_cookie(key="auth_token", path="/api")
        response.delete_cookie(key="csrf_token", path="/api")
        return response
    jti = user.get("jti")
    if not jti:
        return {"detail": "Token has no jti — issued before revocation support"}
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    repo.blacklist_token(
        jti=jti,
        user_id=user["user_id"],
        expires_at=datetime.fromtimestamp(user["exp"], tz=timezone.utc).isoformat(),
    )
    # M9: Log logout event
    try:
        repo.log_auth_event({
            "event_type": "logout",
            "user_id": user["user_id"],
            "ip_address": _extract_ip(request),
            "user_agent": request.headers.get("user-agent"),
            "success": True,
        })
    except Exception:
        pass
    # H7: Clear auth and CSRF cookies
    from starlette.responses import JSONResponse
    response = JSONResponse({"detail": "Logged out successfully"})
    response.delete_cookie(key="auth_token", path="/api")
    response.delete_cookie(key="csrf_token", path="/api")
    return response


@app.post("/api/auth/refresh")
@limiter.limit("10/minute")
def auth_refresh(request: Request, body: RefreshRequest):
    """Exchange a valid refresh token for a new access token + refresh token pair.
    Implements rotation: the old refresh token is revoked on use."""
    if _AUTH_MODE != "jwt":
        raise HTTPException(status_code=404, detail="Auth endpoints require AUTH_MODE=jwt")

    import jwt as pyjwt
    secret = _get_jwt_secret()
    try:
        payload = pyjwt.decode(body.refresh_token, secret, algorithms=["HS256"])
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Refresh token expired")
    except pyjwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Not a refresh token")

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    # Check if revoked
    stored = repo.get_refresh_token(payload["jti"])
    if not stored or stored["revoked"]:
        raise HTTPException(status_code=401, detail="Refresh token revoked")

    # Rotate: revoke old, issue new pair
    repo.revoke_refresh_token(payload["jti"])

    user_id = payload["sub"]
    now_ts = datetime.now(timezone.utc)
    expiry_hours = int(os.environ.get("JWT_EXPIRY_HOURS", "2"))

    # New access token
    new_access_payload = {
        "jti": str(uuid.uuid4()),
        "sub": user_id,
        "role": "user",
        "iat": now_ts,
        "exp": now_ts + timedelta(hours=expiry_hours),
    }
    access_token = pyjwt.encode(new_access_payload, secret, algorithm="HS256")

    # New refresh token
    new_refresh_jti = str(uuid.uuid4())
    new_refresh_payload = {
        "jti": new_refresh_jti,
        "sub": user_id,
        "type": "refresh",
        "iat": now_ts,
        "exp": now_ts + timedelta(days=7),
    }
    new_refresh_token = pyjwt.encode(new_refresh_payload, secret, algorithm="HS256")
    repo.store_refresh_token(new_refresh_jti, user_id, (now_ts + timedelta(days=7)).isoformat())

    # M9: Log token refresh
    try:
        repo.log_auth_event({
            "event_type": "token_refresh",
            "user_id": user_id,
            "ip_address": _extract_ip(request),
            "user_agent": request.headers.get("user-agent"),
            "success": True,
        })
    except Exception:
        pass

    # H7: Set new cookies
    from starlette.responses import JSONResponse
    response = JSONResponse({"token": access_token, "refresh_token": new_refresh_token})
    response.set_cookie(
        key="auth_token",
        value=access_token,
        httponly=True,
        secure=_IS_SECURE_ENV,
        samesite="strict",
        path="/api",
        max_age=expiry_hours * 3600,
    )
    csrf_token = _generate_csrf_token()
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=False,
        secure=_IS_SECURE_ENV,
        samesite="strict",
        path="/api",
        max_age=expiry_hours * 3600,
    )
    return response


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------
def _format_velocity_summary(velocity_windowed) -> str:
    try:
        val = float(velocity_windowed or 0.0)
    except (TypeError, ValueError):
        val = 0.0
    pct = round(val * 100, 1)
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct}% signal velocity over 7d"


def _make_descriptor(n: dict) -> str:
    desc = (n.get("description") or "").strip()
    if desc:
        return desc
    # Synthesize a readable fallback from name + topic tags + stats
    name = (n.get("name") or "").strip()
    stage = n.get("stage") or "Emerging"
    doc_count = int(n.get("document_count") or 0)
    tags_raw = n.get("topic_tags") or "[]"
    if isinstance(tags_raw, str):
        try:
            tags = json.loads(tags_raw)
        except Exception:
            tags = []
    else:
        tags = list(tags_raw)
    tag_text = ", ".join(tags) if tags else "general"
    return (
        f"The narrative describes a {stage.lower()}-stage development across {tag_text} themes"
        f"{f', tracking {doc_count} sources' if doc_count > 1 else ''}. "
        f"A detailed summary will be generated on the next analysis cycle."
    )


def _parse_linked_assets(n: dict) -> list:
    la = n.get("linked_assets")
    if not la:
        return []
    if isinstance(la, str):
        try:
            return json.loads(la)
        except Exception:
            return []
    return list(la)


def _normalize_snapshot_date(d: str) -> str:
    """Convert YYYYMMDD to YYYY-MM-DD."""
    d = str(d).strip()
    if len(d) == 8 and "-" not in d:
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def _build_velocity_timeseries(repo, narrative_id: str, current_velocity: float) -> list:
    """
    Returns a 7-point velocity timeseries from narrative_snapshots.
    Pads synthetically if fewer than 7 snapshots exist.
    """
    today = date.today()
    start = today - timedelta(days=6)

    snapshots = []
    try:
        snapshots = repo.get_snapshots_range(
            narrative_id,
            start.isoformat(),
            today.isoformat(),
        )
    except Exception:
        snapshots = []

    # Build a date→velocity map
    snap_map = {}
    for s in snapshots:
        d = _normalize_snapshot_date(s.get("snapshot_date", ""))
        v = s.get("velocity")
        if d and v is not None:
            snap_map[d] = round(float(v), 4)

    # Fill all 7 days, using synthetic fallback for missing days
    base_val = round(float(current_velocity or 0.0), 4)
    result = []
    for i in range(7):
        day = (start + timedelta(days=i)).isoformat()
        if day in snap_map:
            result.append({"date": day, "value": snap_map[day]})
        else:
            # Synthetic: slight variation around current velocity
            variation = round(base_val * (0.9 + 0.02 * i), 4)
            result.append({"date": day, "value": variation})

    return result


def _build_signal(doc: dict) -> dict:
    domain = doc.get("source_domain") or "unknown"
    src_id = domain.replace(".", "-").replace("www-", "")
    return {
        "id": doc["doc_id"],
        "narrative_id": doc.get("narrative_id", ""),
        "headline": (doc.get("excerpt") or "")[:150],
        "source": {
            "id": src_id,
            "name": domain,
            "type": "news",
            "url": doc.get("source_url") or "",
            "credibility_score": 0.85,
        },
        "timestamp": doc.get("published_at") or "",
        "sentiment": 0.5,
        "coordination_flag": False,
    }


def _build_catalyst(mut: dict) -> dict:
    mag = mut.get("magnitude") or 0.0
    impact = round(min(float(mag), 1.0), 4)
    explanation = (mut.get("haiku_explanation") or "").strip()
    if not explanation:
        explanation = (
            f"{mut.get('mutation_type','change').replace('_',' ').title()}: "
            f"{mut.get('previous_value','?')} → {mut.get('new_value','?')}"
        )
    return {
        "id": mut["id"],
        "narrative_id": mut.get("narrative_id", ""),
        "description": explanation,
        "timestamp": mut.get("detected_at") or "",
        "impact_score": impact,
    }


def _build_mutation(mut: dict) -> dict:
    explanation = (mut.get("haiku_explanation") or "").strip()
    if not explanation:
        explanation = (
            f"{mut.get('mutation_type','change').replace('_',' ').title()}: "
            f"{mut.get('previous_value','?')} → {mut.get('new_value','?')}"
        )
    return {
        "id": mut.get("id", ""),
        "narrative_id": mut.get("narrative_id", ""),
        "from_state": mut.get("previous_value") or "",
        "to_state": mut.get("new_value") or "",
        "timestamp": mut.get("detected_at") or "",
        "trigger": mut.get("id", ""),
        "description": explanation,
        "mutation_type": (mut.get("mutation_type") or "change").replace("_", " ").title(),
        "magnitude": float(mut.get("magnitude") or 0),
    }


def _build_entropy_detail(n: dict, evidence: list) -> dict:
    narrative_id = n.get("narrative_id") or n.get("id") or ""
    score = n.get("entropy")

    # temporal_spread: (date range of evidence) / 7 days, normalized 0-1
    temporal_spread = 0.5
    if evidence:
        dates = [e.get("published_at") for e in evidence if e.get("published_at")]
        if len(dates) >= 2:
            dates_sorted = sorted(dates)
            try:
                from datetime import datetime
                d0 = datetime.fromisoformat(dates_sorted[0].replace("Z", "+00:00"))
                d1 = datetime.fromisoformat(dates_sorted[-1].replace("Z", "+00:00"))
                span_days = abs((d1 - d0).total_seconds()) / 86400
                temporal_spread = round(min(span_days / 7.0, 1.0), 4)
            except Exception:
                temporal_spread = 0.5

    return {
        "narrative_id": narrative_id,
        "score": score,
        "components": {
            "source_diversity": round(float(n.get("cross_source_score") or 0.0), 4),
            "temporal_spread": temporal_spread,
            "sentiment_variance": round(float(n.get("polarization") or 0.0), 4),
        },
    }


_NEWS_DOMAINS = {"reuters", "cnbc", "bloomberg", "ft.com", "bbc", "guardian",
                  "washingtonpost", "nytimes", "aljazeera", "decrypt", "coindesk",
                  "investing.com", "marketwatch", "cnn", "foxbusiness", "theguardian"}
_RESEARCH_DOMAINS = {"seekingalpha", "siliconangle", "bitcoinmagazine", "theblock",
                     "analyst", "research", "morningstar"}
_FILING_DOMAINS = {"sec.gov", "prnewswire", "businesswire", "globenewswire", "edgar"}


def _categorize_domain(domain: str) -> str:
    domain_parts = domain.lower().split(".")
    for kw in _NEWS_DOMAINS:
        if kw in domain_parts:
            return "news"
    for kw in _RESEARCH_DOMAINS:
        if kw in domain_parts:
            return "research"
    for kw in _FILING_DOMAINS:
        if kw in domain_parts:
            return "filings"
    return "other"


def _build_source_stats(docs: list[dict]) -> dict:
    counts = {"total": len(docs), "news": 0, "research": 0, "filings": 0, "other": 0}
    for d in docs:
        cat = _categorize_domain(d.get("source_domain") or "")
        counts[cat] += 1
    return counts


# Precompiled regex patterns for entity extraction (L1)
_RE_PROPER_NOUNS = re.compile(r'\b([A-Z][a-z]{3,}(?:\s+[A-Z][a-z]+){1,2})\b')
_RE_ACRONYMS = re.compile(r'\b([A-Z]{2,6})\b')
_RE_GEO_TERMS = re.compile(
    r'\b((?:Strait of \w+|Kharg Island|Wall Street|Hong Kong|Saudi Arabia'
    r'|Middle East|South China Sea|European Union|Federal Reserve'
    r'|United States|United Kingdom))\b',
    re.IGNORECASE,
)

_ENTITY_SKIP_FIRST_WORD = {"The", "This", "These", "Stage", "That", "With"}
_ENTITY_BLOCKLIST = {
    "THE", "AND", "FOR", "NOT", "ARE", "BUT", "HAS", "ITS",
    "ALL", "CAN", "HAD", "HER", "WAS", "ONE", "OUR", "OUT",
    "NEW", "NOW", "OLD", "SEE", "WAY", "WHO", "DID", "GET",
    "HIS", "HOW", "MAN", "LET", "SAY", "SHE", "TOO", "USE",
    "WITH", "FROM", "THIS", "THAT", "WILL", "HAVE", "BEEN",
    "ALSO", "INTO", "THAN", "THEM", "THEY", "EACH", "MAKE",
    "LIKE", "LONG", "LOOK", "MANY", "SOME", "SUCH", "TAKE",
    "COME", "COULD", "WOULD", "ABOUT", "AFTER", "THESE",
    "OTHER", "WHICH", "THEIR", "THERE", "STAGE", "DOCS",
    "DEVELOPMENT", "SUMMARY", "EMERGING", "GROWING", "MATURE",
    "DECLINING", "DORMANT", "ANALYSIS", "CYCLE", "DETAILED",
    "REPORT",
}


def _extract_entities(name: str, descriptor: str) -> list[str]:
    """Extract key entities from narrative name + descriptor for display."""
    text = f"{name}. {descriptor}"
    entities: list[str] = []
    # 2-3 word capitalized proper nouns (first word must be 4+ chars)
    for match in _RE_PROPER_NOUNS.finditer(descriptor):
        phrase = match.group(0)
        if phrase.split()[0] not in _ENTITY_SKIP_FIRST_WORD:
            entities.append(phrase)
    # All-caps words 2+ chars (acronyms, tickers)
    for match in _RE_ACRONYMS.finditer(text):
        word = match.group(1)
        if word not in _ENTITY_BLOCKLIST:
            entities.append(word)
    # Key financial/geopolitical terms
    for t in _RE_GEO_TERMS.findall(text):
        entities.append(t)
    # Deduplicate preserving order, limit to 6
    seen: set[str] = set()
    result: list[str] = []
    for e in entities:
        key = e.strip()
        if key and key not in seen:
            seen.add(key)
            result.append(key)
        if len(result) >= 6:
            break
    return result


def _safe_json_loads_list(val) -> list:
    """Parse a JSON string to list, or return [] on any failure."""
    if isinstance(val, list):
        return val
    if not isinstance(val, str):
        return []
    try:
        import json as _json
        parsed = _json.loads(val)
        return parsed if isinstance(parsed, list) else []
    except (ValueError, TypeError):
        return []


def _build_signal_lookup(repo) -> dict:
    """Bulk fetch all narrative signals into a lookup dict. Non-fatal."""
    try:
        rows = repo.get_all_narrative_signals()
        lookup = {}
        for r in rows:
            try:
                lookup[r["narrative_id"]] = {
                    **r,
                    "key_actors": _safe_json_loads_list(r.get("key_actors", "[]")),
                    "affected_sectors": _safe_json_loads_list(r.get("affected_sectors", "[]")),
                }
            except (KeyError, TypeError):
                continue
        return lookup
    except Exception:
        return {}


def _build_visible_narrative(n: dict, repo, include_full: bool = False, signal_lookup: dict | None = None) -> dict:
    """Build a full visible narrative object with timeseries + ID arrays."""
    narrative_id = n["narrative_id"]
    velocity_ts = _build_velocity_timeseries(repo, narrative_id, n.get("velocity_windowed"))

    sig = (signal_lookup or {}).get(narrative_id)

    # For list endpoint: just IDs. For detail endpoint: full objects (handled separately).
    evidence_ids = []
    mutation_ids = []
    catalyst_ids = []
    docs: list[dict] = []
    try:
        docs = repo.get_document_evidence(narrative_id)
        evidence_ids = [d["doc_id"] for d in docs[:20]]
    except Exception:
        pass
    try:
        muts = repo.get_mutations_for_narrative(narrative_id)
        for m in muts:
            mutation_ids.append(m["id"])
            if m.get("mutation_type") in ("stage_change", "score_spike"):
                catalyst_ids.append(m["id"])
    except Exception:
        pass

    return {
        "id": narrative_id,
        "name": n.get("name") or "",
        "descriptor": _make_descriptor(n),
        "velocity_summary": _format_velocity_summary(n.get("velocity_windowed")),
        "entropy": n.get("entropy"),
        "saturation": round(float(n.get("cohesion") or 0.0), 4),
        "velocity_timeseries": velocity_ts,
        "signals": evidence_ids,
        "catalysts": catalyst_ids,
        "mutations": mutation_ids,
        "stage": n.get("stage") or "Emerging",
        "burst_velocity": (lambda br: {
            "rate": 0, "baseline": 0, "ratio": br, "is_burst": br >= 3.0,
        } if br and br > 0 else None)(float(n.get("burst_ratio") or 0)),
        "topic_tags": _safe_json_list(n.get("topic_tags")),
        "blurred": False,
        "entity_tags": _extract_entities(n.get("name") or "", _make_descriptor(n)),
        "source_stats": _build_source_stats(docs),
        "last_evidence_at": docs[0].get("published_at", "") if docs else "",
        "signal_direction": sig.get("direction") if sig else None,
        "signal_confidence": round(float(sig.get("confidence") or 0.0), 4) if sig else None,
        "signal_certainty": sig.get("certainty") if sig else None,
        "signal_catalyst_type": sig.get("catalyst_type") if sig else None,
    }


# ===========================================================================
# Endpoints
# ===========================================================================

@app.get("/api/health")
@limiter.exempt
def health():
    return {"status": "ok"}


@app.get("/api/websocket/status")
def websocket_status():
    """WebSocket relay connection status and diagnostics."""
    if _ws_relay is None:
        return {
            "enabled": False,
            "connected": False,
            "subscribed_symbols": [],
            "tick_buffer_size": 0,
            "uptime_seconds": 0.0,
        }
    return {
        "enabled": True,
        "connected": _ws_relay.is_connected,
        "subscribed_symbols": sorted(_ws_relay.get_active_symbols()),
        "tick_buffer_size": _ws_relay.get_tick_buffer_size(),
        "uptime_seconds": round(_ws_relay.get_uptime_seconds(), 1),
    }


@app.get("/api/narratives")
@limiter.limit("30/minute")
def get_narratives(
    request: Request,
    topic: Optional[str] = None,
    stage: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: dict = Depends(get_optional_user),
):
    """
    Returns all active narratives as full visible objects, sorted by ns_score desc.
    Supports ?limit=N&offset=N for pagination. Topic/stage filtering applied at SQL level.
    """
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    # H5: DB-level pagination with stage/topic filters and ORDER BY ns_score DESC
    rows = repo.get_all_active_narratives(
        limit=limit, offset=offset, stage=stage, topic=topic,
    )

    signal_lookup = _build_signal_lookup(repo)

    result = []
    for n in rows:
        result.append(_build_visible_narrative(n, repo, signal_lookup=signal_lookup))

    return result


@app.get("/api/ticker")
@limiter.limit("30/minute")
def get_ticker(request: Request, user: dict = Depends(get_optional_user)):
    """Returns up to 10 ticker items with narrative id for click navigation. Minimum 5 guaranteed."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    rows = repo.get_all_active_narratives(limit=10)

    items = [
        {
            "id": n["narrative_id"],
            "name": n.get("name") or f"Narrative {n['narrative_id'][:8]}",
            "velocity_summary": _format_velocity_summary(n.get("velocity_windowed")),
        }
        for n in rows
    ]

    while len(items) < 5:
        items.append({
            "id": None,
            "name": f"Signal {len(items) + 1}",
            "velocity_summary": "+0.0% signal velocity over 7d",
        })

    return items


@app.get("/api/narratives/{narrative_id}")
@limiter.limit("30/minute")
def get_narrative_detail(request: Request, narrative_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """
    Returns the full Narrative payload with nested Signal, Catalyst, Mutation
    objects and entropy_detail. Used by the Investigate drawer and detail page.
    """
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")

    # Evidence / signals
    evidence = []
    try:
        evidence = repo.get_document_evidence(narrative_id)
    except Exception:
        pass
    signals = [_build_signal(d) for d in evidence[:20]]

    # Mutations + catalysts
    raw_muts = []
    try:
        raw_muts = repo.get_mutations_for_narrative(narrative_id)
    except Exception:
        pass
    mutations = [_build_mutation(m) for m in raw_muts]
    catalysts = [
        _build_catalyst(m) for m in raw_muts
        if m.get("mutation_type") in ("stage_change", "score_spike")
    ]

    velocity_ts = _build_velocity_timeseries(repo, narrative_id, n.get("velocity_windowed"))
    entropy_detail = _build_entropy_detail(n, evidence)

    # 1.1: Sonnet deep analysis from narrative_snapshots
    sonnet_analysis = None
    try:
        snapshots = repo.get_snapshot_history(narrative_id, days=30)
        for snap in snapshots:
            if snap.get("sonnet_analysis"):
                sonnet_analysis = snap["sonnet_analysis"]
                break
    except Exception:
        pass

    # 1.6: Per-narrative sentiment from evidence texts
    sentiment_data = None
    try:
        texts = [d.get("excerpt") or d.get("title") or "" for d in evidence if d.get("excerpt") or d.get("title")]
        if texts:
            from signals import compute_sentiment_scores
            sentiment_data = compute_sentiment_scores(texts)
    except Exception:
        pass

    # Phase 1: Structured signal data from narrative_signals table
    signal_data = None
    try:
        raw_sig = repo.get_narrative_signal(narrative_id)
        if raw_sig:
            signal_data = {
                "direction": raw_sig.get("direction", "neutral"),
                "confidence": round(float(raw_sig.get("confidence") or 0.0), 4),
                "timeframe": raw_sig.get("timeframe", "unknown"),
                "magnitude": raw_sig.get("magnitude", "incremental"),
                "certainty": raw_sig.get("certainty", "speculative"),
                "key_actors": _safe_json_loads_list(raw_sig.get("key_actors", "[]")),
                "affected_sectors": _safe_json_loads_list(raw_sig.get("affected_sectors", "[]")),
                "catalyst_type": raw_sig.get("catalyst_type", "unknown"),
            }
    except Exception:
        pass

    # 1.2: Coordination events from adversarial_log
    coordination = {"flags": int(n.get("coordination_flag_count") or 0),
                     "is_coordinated": bool(n.get("is_coordinated")),
                     "events": []}
    try:
        coordination["events"] = repo.get_adversarial_events(narrative_id=narrative_id, limit=10)
    except Exception:
        pass

    return {
        "id": narrative_id,
        "name": n.get("name") or "",
        "descriptor": _make_descriptor(n),
        "velocity_summary": _format_velocity_summary(n.get("velocity_windowed")),
        "entropy": n.get("entropy"),
        "saturation": round(float(n.get("cohesion") or 0.0), 4),
        "velocity_timeseries": velocity_ts,
        "signals": signals,
        "catalysts": catalysts,
        "mutations": mutations,
        "stage": n.get("stage") or "Emerging",
        "entropy_detail": entropy_detail,
        "blurred": False,
        "assets": _build_narrative_assets(narrative_id),
        "entity_tags": _extract_entities(n.get("name") or "", _make_descriptor(n)),
        "source_stats": _build_source_stats(evidence),
        "last_evidence_at": evidence[0].get("published_at", "") if evidence else "",
        "sonnet_analysis": sonnet_analysis,
        "sentiment": sentiment_data,
        "signal": signal_data,
        "coordination": coordination,
        "ns_score": round(float(n.get("ns_score") or 0.0), 4),
        "document_count": int(n.get("document_count") or 0),
        "cross_source_score": round(float(n.get("cross_source_score") or 0.0), 4),
        "polarization": round(float(n.get("polarization") or 0.0), 4),
        "topic_tags": _safe_json_list(n.get("topic_tags")),
        "burst_velocity": (lambda br: {
            "ratio": round(br, 2), "is_burst": br >= 3.0, "label": "SURGE" if br >= 3.0 else "normal",
        } if br and br > 0 else None)(float(n.get("burst_ratio") or 0)),
    }


@app.get("/api/constellation")
@_timeout()
def get_constellation(user: dict = Depends(get_optional_user)):
    """
    Returns a force-directed graph for the constellation map.
    Nodes: narrative + catalyst nodes. Edges: shared assets (related) + mutations (triggered).
    """
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    rows = repo.get_all_active_narratives()
    rows.sort(key=lambda r: float(r.get("ns_score") or 0.0), reverse=True)

    nodes = []
    edges = []
    catalyst_nodes_added = set()

    for n in rows:
        nid = n["narrative_id"]
        nodes.append({
            "id": nid,
            "name": n.get("name") or nid[:12],
            "type": "narrative",
            "entropy": n.get("entropy"),
        })
        # Catalyst narratives also appear as separate catalyst nodes
        if n.get("is_catalyst"):
            cat_id = f"cat-{nid}"
            if cat_id not in catalyst_nodes_added:
                nodes.append({
                    "id": cat_id,
                    "name": n.get("name") or nid[:12],
                    "type": "catalyst",
                    "description": (
                        n.get("haiku_description")
                        or f"Catalyst event driving '{n.get('name') or nid[:12]}'"
                    ),
                    "impact_score": round(float(n.get("ns_score") or 0.5), 4),
                })
                catalyst_nodes_added.add(cat_id)
                # Triggered edge from catalyst node to the narrative
                edges.append({
                    "source": cat_id,
                    "target": nid,
                    "weight": round(float(n.get("ns_score") or 0.5), 4),
                    "label": "triggered",
                })

    # Related edges from shared linked_assets tickers
    # Only compute between top 20 narratives to keep O(n²) manageable
    top_rows = rows[:20]
    asset_map = {}
    for n in top_rows:
        assets = _parse_linked_assets(n)
        tickers = {a.get("ticker") for a in assets if isinstance(a, dict) and a.get("ticker")}
        asset_map[n["narrative_id"]] = tickers

    processed = set()
    for i, n1 in enumerate(top_rows):
        id1 = n1["narrative_id"]
        for n2 in top_rows[i + 1:]:
            id2 = n2["narrative_id"]
            pair = (id1, id2)
            if pair in processed:
                continue
            processed.add(pair)
            shared = asset_map.get(id1, set()) & asset_map.get(id2, set())
            if len(shared) >= 2:
                weight = round(len(shared) / max(len(asset_map[id1]), len(asset_map[id2]), 1), 4)
                edges.append({
                    "source": id1,
                    "target": id2,
                    "weight": min(weight, 1.0),
                    "label": "related",
                })

    # Guarantee minimum 3 nodes and 2 edges (pad if DB is sparse)
    while len(nodes) < 3:
        idx = len(nodes)
        nodes.append({"id": f"stub-{idx}", "name": f"Signal {idx+1}", "type": "narrative", "entropy": None})
    if len(edges) < 2 and len(nodes) >= 2:
        edges.append({"source": nodes[0]["id"], "target": nodes[1]["id"], "weight": 0.3, "label": "related"})
    if len(edges) < 2 and len(nodes) >= 3:
        edges.append({"source": nodes[1]["id"], "target": nodes[2]["id"], "weight": 0.2, "label": "related"})

    return {"nodes": nodes, "edges": edges}


@app.get("/api/credits")
def get_credits(user: dict = Depends(get_current_user)):
    """Returns credit balance for the signed-in user. Requires auth."""
    return _user_credits


class TopupRequest(BaseModel):
    amount: int


@app.post("/api/credits/topup")
def topup_credits(body: TopupRequest, user: dict = Depends(get_current_user)):
    """Increments credit balance. Requires auth."""
    if body.amount <= 0:
        raise HTTPException(status_code=422, detail="amount must be positive")
    _user_credits["balance"] += body.amount
    _user_credits["total_purchased"] += body.amount
    return _user_credits


# ===========================================================================
# D1 Endpoints — Asset Class Association Model
# ===========================================================================

@app.get("/api/asset-classes")
def get_asset_classes(user: dict = Depends(get_optional_user)):
    """Returns all AssetClass objects."""
    return ASSET_CLASSES


@app.get("/api/securities")
@limiter.limit("30/minute")
def get_securities(request: Request, user: dict = Depends(get_optional_user)):
    """Returns all TrackedSecurity objects (static + dynamically discovered from narratives)."""
    combined = list(TRACKED_SECURITIES) + list(_DYNAMIC_SECURITIES.values())
    return combined


@app.get("/api/narratives/{narrative_id}/assets")
@limiter.limit("30/minute")
def get_narrative_assets(request: Request, narrative_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """
    Returns NarrativeAsset objects for a narrative, each with nested asset_class
    and securities list. Returns [] if no associations found.
    """
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")
    return _build_narrative_assets(narrative_id)


@app.get("/api/narratives/{narrative_id}/signal")
@limiter.limit("30/minute")
def get_narrative_signal_endpoint(request: Request, narrative_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Return structured signal data for a single narrative."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    signal = repo.get_narrative_signal(narrative_id)
    if not signal:
        return {"narrative_id": narrative_id, "signal": None}
    signal_dict = dict(signal)
    signal_dict["key_actors"] = _safe_json_loads_list(signal_dict.get("key_actors") or "[]")
    signal_dict["affected_sectors"] = _safe_json_loads_list(signal_dict.get("affected_sectors") or "[]")
    return {"narrative_id": narrative_id, "signal": signal_dict}


@app.get("/api/signals/leaderboard")
@limiter.limit("30/minute")
def get_signal_leaderboard(request: Request, limit: int = Query(50, ge=1, le=500), user: dict = Depends(get_optional_user)):
    """Top narratives ranked by confidence * direction strength."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    signals = repo.get_all_narrative_signals()
    enriched = []
    for sig in signals:
        try:
            nar = repo.get_narrative(sig["narrative_id"])
        except Exception:
            continue
        if nar and not nar.get("suppressed"):
            enriched.append({
                "narrative_id": sig["narrative_id"],
                "name": nar["name"],
                "stage": nar.get("stage"),
                "direction": sig["direction"],
                "confidence": sig["confidence"],
                "magnitude": sig.get("magnitude"),
                "certainty": sig.get("certainty"),
                "catalyst_type": sig.get("catalyst_type"),
                "signal_strength": float(sig.get("confidence") or 0.0) * (1.0 if sig.get("direction") != "neutral" else 0.0),
            })
    enriched.sort(key=lambda x: x["signal_strength"], reverse=True)
    return enriched[:limit]


# ===========================================================================
# D2 Endpoints — Finnhub Quote
# ===========================================================================

@app.get("/api/securities/{symbol}/quote")
@limiter.limit("30/minute")
def get_security_quote(request: Request, symbol: str = FPath(..., max_length=12), user: dict = Depends(get_optional_user)):
    """
    Returns the latest cached Finnhub quote dict for the given symbol.
    Returns {"symbol": symbol, "available": false} if unavailable or key not set.
    """
    symbol = _validate_symbol(symbol)
    if not finnhub.is_enabled():
        return {"symbol": symbol, "available": False}
    # fetch_quote returns cached data if within TTL; None if disabled/failed
    quote = finnhub.fetch_quote(symbol)
    if quote is None:
        return {"symbol": symbol, "available": False}
    return {**quote, "symbol": symbol, "available": True}


# ===========================================================================
# C4 Endpoints — Subscription, Export, Signals
# ===========================================================================

@app.get("/api/subscription")
def get_subscription(user: dict = Depends(get_current_user)):
    """Returns subscription status for the signed-in user. Requires auth."""
    return _subscription


@app.post("/api/subscription/toggle")
def toggle_subscription(user: dict = Depends(get_current_user)):
    """Toggles subscription status. Requires auth. Returns updated status."""
    _subscription["subscribed"] = not _subscription["subscribed"]
    return _subscription


@app.post("/api/narratives/{narrative_id}/export")
def export_narrative(narrative_id: str = FPath(..., max_length=50), user: dict = Depends(get_current_user)):
    """
    Generates a CSV export of the narrative's signals, catalysts, and mutations.
    Requires auth + active subscription (returns 403 otherwise).
    """
    if not _subscription["subscribed"]:
        raise HTTPException(status_code=403, detail="Subscription required for export")

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")

    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow(["type", "id", "headline_or_description", "source_or_states", "timestamp", "score"])
    writer.writerow(["NARRATIVE", narrative_id, _csv_safe(n.get("name", "")), _csv_safe(_make_descriptor(n)), "", ""])

    # Signals
    try:
        docs = repo.get_document_evidence(narrative_id)
        for d in docs[:50]:
            writer.writerow([
                "SIGNAL", d.get("doc_id", ""),
                _csv_safe((d.get("excerpt") or "")[:200]),
                _csv_safe(d.get("source_domain", "")),
                d.get("published_at", ""),
                "",
            ])
    except Exception:
        pass

    # Mutations
    try:
        muts = repo.get_mutations_for_narrative(narrative_id)
        for m in muts:
            writer.writerow([
                "MUTATION", m.get("id", ""),
                _csv_safe((m.get("haiku_explanation") or "")[:200]),
                _csv_safe(f"{m.get('previous_value','?')} → {m.get('new_value','?')}"),
                m.get("detected_at", ""),
                m.get("magnitude", ""),
            ])
    except Exception:
        pass

    csv_bytes = output.getvalue().encode("utf-8")

    from fastapi.responses import Response as FastAPIResponse
    return FastAPIResponse(
        content=csv_bytes,
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="narrative-{narrative_id}.csv"',
            "Content-Length": str(len(csv_bytes)),
        },
    )


@app.get("/api/signals")
@limiter.limit("30/minute")
def get_signals(request: Request, user: dict = Depends(get_optional_user)):
    """
    Returns a flat list of Signal objects from the top active narratives.
    Sorted newest first. Includes coordination_flag.
    """
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    rows = repo.get_all_active_narratives(limit=5)

    signals = []
    seen_ids = set()
    for n in rows:
        nid = n["narrative_id"]
        try:
            docs = repo.get_document_evidence(nid)
        except Exception:
            continue
        for d in docs[:10]:
            if d["doc_id"] in seen_ids:
                continue
            seen_ids.add(d["doc_id"])
            # Flag documents from adversarial/high-velocity bursts
            coordination_flag = bool(n.get("adversarial_flag") or n.get("coordination_score", 0) > 0.5)
            sig = _build_signal(d)
            sig["coordination_flag"] = coordination_flag
            signals.append(sig)

    # Sort newest first (empty timestamps sort to end)
    signals.sort(key=lambda s: s.get("timestamp") or "", reverse=True)

    return signals[:50]


@app.post("/api/credits/use")
def use_credit(user: dict = Depends(get_current_user)):
    """Decrements credit balance by 1. Requires auth. Returns 402 if balance is 0."""
    if _user_credits["balance"] <= 0:
        raise HTTPException(status_code=402, detail="Insufficient credits")
    _user_credits["balance"] -= 1
    _user_credits["total_used"] += 1
    return _user_credits


def _ticker_payload() -> list:
    """Build current ticker data with a small random velocity nudge (±5%)."""
    repo = get_repo()
    if repo is None:
        return []
    rows = repo.get_all_active_narratives(limit=10)
    items = []
    for n in rows:
        vel = float(n.get("velocity_windowed") or 0.0)
        nudged = vel * (1 + random.uniform(-0.05, 0.05))
        pct = round(nudged * 100, 1)
        sign = "+" if pct >= 0 else ""
        items.append({
            "name": n.get("name") or f"Narrative {n['narrative_id'][:8]}",
            "velocity_summary": f"{sign}{pct}% signal velocity over 7d",
        })
    while len(items) < 5:
        items.append({
            "name": f"Signal {len(items) + 1}",
            "velocity_summary": "+0.0% signal velocity over 7d",
        })
    return items


@app.get("/api/stream")
@limiter.limit("2/minute")
async def stream(request: Request, token: str = Query(None)):
    """
    SSE endpoint — emits ticker-update events every 8 seconds.
    Auth: pass JWT as ?token= query parameter (EventSource cannot set headers).
    Stub mode: no token required, user_id = "local".
    """
    global _sse_connections

    # --- Auth ---
    if _AUTH_MODE == "stub":
        user_id = "local"
    else:
        if not token:
            raise HTTPException(status_code=403, detail="Authentication required")
        payload = _decode_jwt(token)
        user_id = payload.get("sub", "unknown")

    # --- Connection limits ---
    lock = _sse_lock or asyncio.Lock()

    async with lock:
        if _sse_connections >= _SSE_MAX_GLOBAL:
            raise HTTPException(status_code=503, detail="SSE connection limit reached")
        user_count = _sse_per_user.get(user_id, 0)
        if user_count >= _SSE_MAX_PER_USER:
            raise HTTPException(status_code=429, detail="Per-user SSE connection limit reached")
        _sse_connections += 1
        _sse_per_user[user_id] = user_count + 1

    if _SSE_AVAILABLE:
        async def event_generator():
            global _sse_connections
            try:
                yield {"data": json.dumps({"type": "connected"})}
                while True:
                    await asyncio.sleep(8)
                    yield {"data": json.dumps(_latest_ticker_payload)}
            finally:
                async with lock:
                    _sse_connections = max(0, _sse_connections - 1)
                    cur = _sse_per_user.get(user_id, 1)
                    if cur <= 1:
                        _sse_per_user.pop(user_id, None)
                    else:
                        _sse_per_user[user_id] = cur - 1
        return EventSourceResponse(event_generator())
    else:
        async def manual_generator():
            global _sse_connections
            try:
                yield "data: {\"type\": \"connected\"}\n\n"
                while True:
                    await asyncio.sleep(8)
                    payload = json.dumps(_latest_ticker_payload)
                    yield f"data: {payload}\n\n"
            finally:
                async with lock:
                    _sse_connections = max(0, _sse_connections - 1)
                    cur = _sse_per_user.get(user_id, 1)
                    if cur <= 1:
                        _sse_per_user.pop(user_id, None)
                    else:
                        _sse_per_user[user_id] = cur - 1
        return StreamingResponse(manual_generator(), media_type="text/event-stream")


# ===========================================================================
# D3 Endpoints — Stocks with Narrative Impact Scores
# ===========================================================================

@app.get("/api/stocks")
@limiter.limit("30/minute")
def get_stocks(
    request: Request,
    sort_by: str = "impact",
    sort_order: str = "desc",
    asset_class: Optional[str] = None,
    min_impact: Optional[int] = None,
    user: dict = Depends(get_optional_user),
):
    """
    Returns filtered and sorted list of TrackedSecurity objects.
    sort_by: "impact" | "price" | "change" | "symbol"
    sort_order: "desc" | "asc"
    asset_class: filter by asset_class_id (optional)
    min_impact: filter by min narrative_impact_score inclusive (optional)
    """
    result = list(TRACKED_SECURITIES) + list(_DYNAMIC_SECURITIES.values())

    if asset_class:
        result = [s for s in result if s["asset_class_id"] == asset_class]
    if min_impact is not None:
        result = [s for s in result if s["narrative_impact_score"] >= min_impact]

    reverse = sort_order == "desc"
    if sort_by == "impact":
        result.sort(key=lambda s: s["narrative_impact_score"], reverse=reverse)
    elif sort_by == "price":
        result.sort(key=lambda s: s["current_price"] or 0.0, reverse=reverse)
    elif sort_by == "change":
        result.sort(key=lambda s: s["price_change_24h"] or 0.0, reverse=reverse)
    elif sort_by == "symbol":
        result.sort(key=lambda s: s["symbol"], reverse=reverse)

    return result


@app.get("/api/stocks/{symbol}")
@limiter.limit("30/minute")
def get_stock_detail(request: Request, symbol: str = FPath(..., max_length=12), user: dict = Depends(get_optional_user)):
    """
    Returns a single TrackedSecurity extended with a "narratives" field listing
    which narratives affect this security and how.
    Returns 404 if symbol not found.
    """
    symbol = _validate_symbol(symbol)
    all_secs = list(TRACKED_SECURITIES) + list(_DYNAMIC_SECURITIES.values())
    sec = next(
        (s for s in all_secs if s["symbol"].upper() == symbol.upper()),
        None,
    )
    if sec is None:
        raise HTTPException(status_code=404, detail=f"Security '{symbol}' not found")

    ac_id = sec["asset_class_id"]
    is_dynamic = sec.get("dynamic", False)

    narratives_field: list[dict] = []
    repo = get_repo()

    if is_dynamic and repo is not None:
        # Dynamic securities: find narratives that link to this ticker via linked_assets
        all_narratives = repo.get_all_active_narratives()
        for n in all_narratives:
            la_raw = n.get("linked_assets")
            if not la_raw:
                continue
            try:
                assets = json.loads(la_raw) if isinstance(la_raw, str) else la_raw
            except Exception:
                continue
            for a in assets:
                if a.get("ticker", "").upper() == symbol.upper():
                    sim = float(a.get("similarity_score", 0))
                    narratives_field.append({
                        "narrative_id": n["narrative_id"],
                        "narrative_name": n.get("name") or n["narrative_id"][:8],
                        "exposure_score": round(sim, 3),
                        "direction": _derive_direction([sec]),
                    })
                    break
    else:
        # Static securities: use NARRATIVE_ASSETS mapping
        matching_nas = [na for na in NARRATIVE_ASSETS if na["asset_class_id"] == ac_id]
        nar_names: dict[str, str] = {}
        if repo is not None:
            for na in matching_nas:
                nar_id = na["narrative_id"]
                if nar_id in nar_names:
                    continue
                try:
                    n = repo.get_narrative(nar_id)
                    if n:
                        nar_names[nar_id] = n.get("name") or nar_id
                except Exception:
                    pass
        narratives_field = [
            {
                "narrative_id": na["narrative_id"],
                "narrative_name": nar_names.get(na["narrative_id"], f"Narrative {na['narrative_id'][:8]}"),
                "exposure_score": na["exposure_score"],
                "direction": _derive_direction([sec]),
            }
            for na in matching_nas
        ]

    return {**sec, "narratives": narratives_field}


# ===========================================================================
# Activity Feed, Watchlist, Alert Rules — Inbox Tab
# ===========================================================================

@app.get("/api/activity")
def get_activity(limit: int = 100, user: dict = Depends(get_optional_user)):
    """Merged activity feed: mutations + notifications + pipeline events."""
    limit = max(1, min(limit, 500))
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    items: list[dict] = []

    # Mutations (richest data)
    try:
        for rd in repo.get_recent_mutations(limit):
            items.append({
                "type": "mutation",
                "subtype": rd.get("mutation_type", ""),
                "timestamp": rd.get("detected_at", ""),
                "title": f"{(rd.get('mutation_type') or 'change').replace('_', ' ').title()}: {rd.get('narrative_name') or 'Unknown'}",
                "message": (rd.get("haiku_explanation") or "")[:200],
                "link": f"/narrative/{rd.get('narrative_id', '')}",
                "metadata": {
                    "narrative_id": rd.get("narrative_id", ""),
                    "previous_value": rd.get("previous_value", ""),
                    "new_value": rd.get("new_value", ""),
                    "magnitude": rd.get("magnitude"),
                },
            })
    except Exception:
        pass

    # Notifications (from alert rules)
    try:
        notifs = repo.get_notifications(user["user_id"])
        for n in notifs:
            items.append({
                "type": "alert",
                "subtype": "notification",
                "timestamp": n.get("created_at", ""),
                "title": n.get("title", ""),
                "message": n.get("message", ""),
                "link": n.get("link", ""),
                "metadata": {"is_read": bool(n.get("is_read")), "id": n.get("id", "")},
            })
    except Exception:
        pass

    # Pipeline events (last 10)
    try:
        for rd in repo.get_recent_pipeline_events(10):
            if rd["step_name"] == "cleanup":
                items.append({
                    "type": "system",
                    "subtype": "pipeline_complete",
                    "timestamp": rd.get("run_at", ""),
                    "title": "Pipeline cycle completed",
                    "message": rd.get("error_message") or f"Duration: {rd.get('duration_ms', 0)}ms",
                    "link": "",
                    "metadata": {"status": rd.get("status", "")},
                })
    except Exception:
        pass

    items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return items[:limit]


# --- Watchlist ---

@app.get("/api/watchlist")
def get_watchlist(user: dict = Depends(get_optional_user)):
    """Get user's watchlist with current stats for each item."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    watchlists = repo.list_watchlists(user["user_id"])
    if not watchlists:
        return {"items": [], "watchlist_id": None}

    wl = watchlists[0]
    raw_items = repo.get_watchlist_items(wl["id"])

    enriched: list[dict] = []
    for item in raw_items:
        entry = {**item}
        if item["item_type"] == "narrative":
            n = repo.get_narrative(item["item_id"])
            if n:
                entry["name"] = n.get("name", "")
                entry["stage"] = n.get("stage", "")
                entry["velocity"] = float(n.get("velocity_windowed") or 0)
                entry["ns_score"] = float(n.get("ns_score") or 0)
        elif item["item_type"] == "ticker":
            all_secs = list(TRACKED_SECURITIES) + list(_DYNAMIC_SECURITIES.values())
            sec = next((s for s in all_secs if s["symbol"].upper() == item["item_id"].upper()), None)
            if sec:
                entry["name"] = sec.get("name", "")
                entry["current_price"] = sec.get("current_price")
                entry["price_change_24h"] = sec.get("price_change_24h")
        enriched.append(entry)

    return {"items": enriched, "watchlist_id": wl["id"]}


class WatchlistAddRequest(BaseModel):
    item_type: Literal["narrative", "ticker"]
    item_id: str


@app.post("/api/watchlist/add")
def add_to_watchlist(req: WatchlistAddRequest, user: dict = Depends(get_optional_user)):
    """Add item to watchlist. Creates default watchlist if needed."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    watchlists = repo.list_watchlists(user["user_id"])
    if not watchlists:

        wl_id = str(uuid.uuid4())
        repo.create_watchlist({
            "id": wl_id,
            "user_id": user["user_id"],
            "name": "My Watchlist",
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    else:
        wl_id = watchlists[0]["id"]

    # Check for duplicates
    existing = repo.get_watchlist_items(wl_id)
    if any(i["item_type"] == req.item_type and i["item_id"] == req.item_id for i in existing):
        return {"status": "already_exists"}


    repo.add_watchlist_item({
        "id": str(uuid.uuid4()),
        "watchlist_id": wl_id,
        "item_type": req.item_type,
        "item_id": req.item_id,
        "added_at": datetime.now(timezone.utc).isoformat(),
    })
    return {"status": "added"}


@app.delete("/api/watchlist/remove/{item_id}")
def remove_from_watchlist(item_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Remove item from watchlist."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    item = repo.get_watchlist_item(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Watchlist item not found")
    if item.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized to delete this item")
    repo.delete_watchlist_item(item_id)
    return {"status": "removed"}


# --- Alert Rules ---

class AlertRuleRequest(BaseModel):
    rule_type: Literal[
        "ns_above", "ns_below", "new_narrative", "mutation", "stage_change", "catalyst",
        "price_above", "price_below", "pct_change",
        "rsi_overbought", "rsi_oversold", "macd_crossover",
        "sentiment_spike", "portfolio_drawdown",
    ]
    target_type: Literal["narrative", "ticker", "portfolio"]
    target_id: str = ""
    threshold: float = 0.0


@app.get("/api/alerts/types")
def get_alert_types(user: dict = Depends(get_optional_user)):
    """Returns available alert rule types."""
    return RULE_TYPES


@app.get("/api/alerts/rules")
def get_alert_rules(user: dict = Depends(get_optional_user)):
    """List user's alert rules."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    mgr = NotificationManager(repo)
    return mgr.list_rules(user["user_id"])


@app.post("/api/alerts/rules")
def create_alert_rule(req: AlertRuleRequest, user: dict = Depends(get_optional_user)):
    """Create a new alert rule."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    mgr = NotificationManager(repo)
    rule_id = mgr.create_rule(user["user_id"], req.rule_type, req.target_type, req.target_id, req.threshold)
    return {"status": "created", "rule_id": rule_id}


@app.delete("/api/alerts/rules/{rule_id}")
def delete_alert_rule(rule_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Delete an alert rule."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    rule = repo.get_notification_rule(rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    if rule.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized to delete this rule")
    mgr = NotificationManager(repo)
    mgr.delete_rule(rule_id)
    return {"status": "deleted"}


@app.post("/api/alerts/rules/{rule_id}/toggle")
def toggle_alert_rule(rule_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Toggle an alert rule enabled/disabled."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    mgr = NotificationManager(repo)
    # Read current state, flip it
    rules = mgr.list_rules(user["user_id"])
    rule = next((r for r in rules if r["id"] == rule_id), None)
    if rule is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    new_state = not bool(rule.get("enabled", 1))
    mgr.toggle_rule(rule_id, new_state)
    return {"status": "toggled", "enabled": new_state}


@app.get("/api/alerts")
def get_alerts(unread_only: bool = False, user: dict = Depends(get_optional_user)):
    """Get user's notifications."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    mgr = NotificationManager(repo)
    return mgr.get_notifications(user["user_id"], unread_only)


@app.post("/api/alerts/read/{notification_id}")
def mark_alert_read(notification_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Mark a notification as read."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    mgr = NotificationManager(repo)
    mgr.mark_read(notification_id)
    return {"status": "read"}


@app.post("/api/alerts/read-all")
def mark_all_alerts_read(user: dict = Depends(get_optional_user)):
    """Mark all notifications as read."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    mgr = NotificationManager(repo)
    mgr.mark_all_read(user["user_id"])
    return {"status": "all_read"}


# ===========================================================================
# D4 Endpoints — Manipulation/Coordination Detection
# ===========================================================================

def _adversarial_to_manipulation(event: dict) -> dict:
    """Map adversarial_log row to ManipulationIndicator shape."""
    return {
        "id": event.get("event_id", ""),
        "narrative_id": event.get("narrative_id", ""),
        "indicator_type": "coordination_burst",
        "confidence": float(event.get("similarity_score") or 0.5),
        "detected_at": event.get("detected_at", ""),
        "evidence_summary": f"Coordinated activity from domains: {event.get('source_domains', 'unknown')}",
        "flagged_signals": [],
        "status": event.get("action_taken") or "active",
    }


@app.get("/api/manipulation")
def get_manipulation(
    indicator_type: Optional[str] = None,
    min_confidence: Optional[float] = None,
    status: Optional[str] = None,
    user: dict = Depends(get_optional_user),
):
    """
    Returns narratives augmented with their ManipulationIndicators.
    Only includes narratives with at least one indicator after filtering.
    """
    # Try real adversarial_log data first, fall back to stubs
    repo = get_repo()
    real_events = []
    if repo is not None:
        try:
            raw = repo.get_adversarial_events(limit=100)
            real_events = [_adversarial_to_manipulation(e) for e in raw]
        except Exception:
            pass
    filtered = real_events if real_events else list(MANIPULATION_INDICATORS)
    if indicator_type:
        filtered = [mi for mi in filtered if mi["indicator_type"] == indicator_type]
    if min_confidence is not None:
        filtered = [mi for mi in filtered if mi["confidence"] >= min_confidence]
    if status:
        filtered = [mi for mi in filtered if mi["status"] == status]

    # Group by narrative_id
    groups: dict[str, list] = {}
    for mi in filtered:
        nid = mi["narrative_id"]
        groups.setdefault(nid, []).append(mi)

    result = []
    for nid, indicators in groups.items():
        nar_info = {
            "id": nid,
            "name": f"Narrative {nid[:8]}",
            "descriptor": "",
            "entropy": None,
            "velocity_summary": "",
        }
        repo = get_repo()
        if repo is not None:
            try:
                n = repo.get_narrative(nid)
                if n:
                    nar_info["name"] = n.get("name") or nid
                    nar_info["descriptor"] = _make_descriptor(n)
                    nar_info["entropy"] = n.get("entropy")
                    nar_info["velocity_summary"] = _format_velocity_summary(
                        n.get("velocity_windowed")
                    )
            except Exception:
                pass
        result.append({**nar_info, "manipulation_indicators": indicators})

    return result


@app.get("/api/narratives/{narrative_id}/manipulation")
def get_narrative_manipulation(narrative_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Returns ManipulationIndicator objects for the given narrative_id. Returns [] if none."""
    repo = get_repo()
    if repo is not None:
        try:
            raw = repo.get_adversarial_events(narrative_id=narrative_id, limit=50)
            if raw:
                return [_adversarial_to_manipulation(e) for e in raw]
        except Exception:
            pass
    return [mi for mi in MANIPULATION_INDICATORS if mi["narrative_id"] == narrative_id]


# ===========================================================================
# F3 — Pre-Earnings Intelligence Brief
# ===========================================================================

def _interpret_entropy(entropy: float | None) -> str:
    """Human-readable entropy interpretation."""
    if entropy is None:
        return "Insufficient data"
    if entropy < 0.5:
        return "Narrow sourcing — potential echo chamber"
    if entropy < 1.0:
        return "Limited diversity — monitor for astroturfing"
    if entropy < 2.0:
        return "Multi-source coverage — diverse perspectives"
    return "Broad coverage — approaching consensus"


@app.get("/api/brief/{ticker}")
def get_brief(ticker: str = FPath(..., max_length=12), user: dict = Depends(get_optional_user)):
    """Pre-earnings intelligence brief for a specific ticker."""
    ticker = _validate_symbol(ticker)
    # Find security
    sec = next((s for s in TRACKED_SECURITIES if s["symbol"].upper() == ticker.upper()), None)
    if sec is None:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found")

    # Find narrative_ids linked to this ticker's asset class
    asset_class_id = sec["asset_class_id"]
    linked_nas = [na for na in NARRATIVE_ASSETS if na["asset_class_id"] == asset_class_id]
    narrative_ids = list(set(na["narrative_id"] for na in linked_nas))

    repo = get_repo()
    brief_narratives = []

    for nid in narrative_ids:
        nar_info = None
        if repo:
            try:
                nar_info = repo.get_narrative(nid)
            except Exception:
                pass

        if not nar_info:
            continue

        # Find exposure from NARRATIVE_ASSETS; direction from live price
        na_entry = next((na for na in linked_nas if na["narrative_id"] == nid), None)
        exposure_score = na_entry["exposure_score"] if na_entry else 0.0
        direction = _derive_direction([sec])

        # Count coordination flags from MANIPULATION_INDICATORS
        coord_flags = sum(1 for mi in MANIPULATION_INDICATORS if mi["narrative_id"] == nid)

        # Get evidence for signal count and top signals
        evidence = []
        try:
            evidence = repo.get_document_evidence(nid) if repo else []
        except Exception:
            pass

        top_signals = [
            {
                "headline": e.get("excerpt", "")[:120],
                "source": e.get("source_domain", ""),
                "timestamp": e.get("published_at", ""),
            }
            for e in evidence[:3]
        ]

        entropy_val = nar_info.get("entropy")
        velocity_w = float(nar_info.get("velocity_windowed") or 0.0)

        # Calculate days active
        from signals import get_narrative_age_days
        days_active = get_narrative_age_days(nar_info.get("created_at") or "")

        brief_narratives.append({
            "id": nid,
            "name": nar_info.get("name") or nid,
            "stage": nar_info.get("stage") or "Emerging",
            "velocity_windowed": round(velocity_w, 4),
            "entropy": entropy_val,
            "entropy_interpretation": _interpret_entropy(entropy_val),
            "burst_velocity": None,
            "coordination_flags": coord_flags,
            "exposure_score": exposure_score,
            "direction": direction,
            "days_active": days_active,
            "signal_count": len(evidence),
            "top_signals": top_signals,
        })

    # Build risk summary
    entropies = [n["entropy"] for n in brief_narratives if n["entropy"] is not None]
    avg_entropy = sum(entropies) / len(entropies) if entropies else 0.0
    directions = [n["direction"] for n in brief_narratives]
    direction_counts = {}
    for d in directions:
        direction_counts[d] = direction_counts.get(d, 0) + 1
    dominant_direction = max(direction_counts, key=direction_counts.get) if direction_counts else "uncertain"
    coord_detected = any(n["coordination_flags"] > 0 for n in brief_narratives)
    burst_ratios = [n["burst_velocity"]["ratio"] for n in brief_narratives if n["burst_velocity"]]
    highest_burst = max(burst_ratios) if burst_ratios else 0.0

    from datetime import datetime, timezone
    return {
        "ticker": ticker.upper(),
        "security": sec,
        "narratives": brief_narratives,
        "risk_summary": {
            "coordination_detected": coord_detected,
            "highest_burst_ratio": highest_burst,
            "dominant_direction": dominant_direction,
            "narrative_count": len(brief_narratives),
            "avg_entropy": round(avg_entropy, 2),
            "entropy_assessment": _interpret_entropy(avg_entropy if entropies else None),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ===========================================================================
# F5 — Historical Snapshot API + Price Data
# ===========================================================================

@app.get("/api/narratives/{narrative_id}/history")
@limiter.limit("30/minute")
def get_narrative_history(request: Request, narrative_id: str = FPath(..., max_length=50), days: int = 30, user: dict = Depends(get_optional_user)):
    """Returns daily snapshots for the narrative over the specified period."""
    days = max(1, min(days, 365))
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")
    snapshots = repo.get_snapshot_history(narrative_id, days)
    return [
        {
            "date": s.get("snapshot_date"),
            "velocity": s.get("velocity"),
            "entropy": s.get("entropy"),
            "cohesion": s.get("cohesion"),
            "ns_score": s.get("ns_score"),
            "document_count": s.get("doc_count"),
            "lifecycle_stage": s.get("lifecycle_stage"),
            "linked_assets": json.loads(s.get("linked_assets") or "[]"),
            "burst_ratio": s.get("burst_ratio"),
        }
        for s in snapshots
    ]


@app.get("/api/ticker/{symbol}/price-history")
@limiter.limit("30/minute")
def get_price_history_endpoint(
    request: Request,
    symbol: str = FPath(..., max_length=12),
    days: int = 30,
    interval: str = "1d",
    period: str = "",
    user: dict = Depends(get_optional_user),
):
    """Returns OHLCV price history from yfinance, cached."""
    symbol = _validate_symbol(symbol)
    # Period shortcuts override days
    from datetime import date as _date
    _PERIOD_MAP = {"1D": 1, "5D": 5, "1M": 30, "3M": 90, "6M": 180, "1Y": 365}
    if period == "YTD":
        today = _date.today()
        days = (today - _date(today.year, 1, 1)).days or 1
    elif period in _PERIOD_MAP:
        days = _PERIOD_MAP[period]
    days = max(1, min(days, 365))
    from stock_data import get_price_history
    data = get_price_history(symbol, days, interval)
    if not data:
        return {"symbol": symbol, "data": [], "available": False}
    return {"symbol": symbol, "data": data, "available": True}


# ===========================================================================
# F6 — Velocity-Price Correlation
# ===========================================================================

@app.get("/api/correlations/{narrative_id}/{ticker}")
@limiter.limit("10/minute")
def get_correlation(request: Request, narrative_id: str = FPath(..., max_length=50), ticker: str = FPath(..., max_length=12), lead_days: int = Query(1, ge=0, le=90), user: dict = Depends(get_optional_user)):
    """Returns correlation analysis between narrative velocity and ticker price."""
    ticker = _validate_symbol(ticker)
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    # Get velocity history from snapshots
    snapshots = repo.get_snapshot_history(narrative_id, 90)
    velocity_history = [
        {"date": s.get("snapshot_date"), "velocity": s.get("velocity")}
        for s in reversed(snapshots)  # oldest first
        if s.get("snapshot_date")
    ]

    # Get price history from yfinance
    from stock_data import get_price_history
    price_history = get_price_history(ticker, 90)

    from correlation_service import compute_velocity_price_correlation
    result = compute_velocity_price_correlation(velocity_history, price_history, lead_days)
    result["narrative_id"] = narrative_id
    result["ticker"] = ticker.upper()
    return result


# ===========================================================================
# V3 Phase 1 — New Endpoints
# ===========================================================================

# --- 1.2: Coordination endpoints ---

@app.get("/api/narratives/{narrative_id}/coordination")
def get_narrative_coordination(narrative_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Returns adversarial coordination events for a narrative."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")
    events = repo.get_adversarial_events(narrative_id=narrative_id, limit=50)
    return {
        "narrative_id": narrative_id,
        "is_coordinated": bool(n.get("is_coordinated")),
        "flag_count": int(n.get("coordination_flag_count") or 0),
        "events": events,
    }


@app.get("/api/coordination/summary")
def get_coordination_summary(user: dict = Depends(get_optional_user)):
    """Aggregate coordination stats across all narratives."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    all_events = repo.get_adversarial_events(limit=200)
    by_type: dict[str, int] = {}
    for e in all_events:
        t = e.get("event_type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1
    return {
        "total_events": len(all_events),
        "events_by_type": by_type,
        "most_recent": all_events[0] if all_events else None,
    }


# --- 1.3: Correlation batch endpoints ---

import threading as _threading
_correlation_cache: dict = {}
_correlation_cache_at: float = 0.0
_correlation_cache_lock = _threading.Lock()
_CORRELATION_CACHE_TTL = 900  # 15 minutes

_overlap_cache: dict[str, dict] = {}  # cache_key -> {"result": dict, "cached_at": float}
_OVERLAP_CACHE_TTL = 14400  # 4 hours

_lead_time_cache: dict = {}
_lead_time_cache_at: float = 0.0
_LEAD_TIME_CACHE_TTL = 14400  # 4 hours

_contrarian_cache: dict = {}
_contrarian_cache_at: float = 0.0
_CONTRARIAN_CACHE_TTL = 14400  # 4 hours


@app.get("/api/correlations/top")
@limiter.limit("10/minute")
@_timeout()
def get_top_correlations(request: Request, limit: int = 20, lead_days: int = Query(1, ge=0, le=90), user: dict = Depends(get_optional_user)):
    """Returns top narrative-ticker correlations by absolute strength. Cached 15min."""
    import time as _time
    global _correlation_cache, _correlation_cache_at

    limit = min(limit, 100)  # cap to prevent abuse

    cache_key = f"{lead_days}"
    with _correlation_cache_lock:
        if cache_key in _correlation_cache and (_time.time() - _correlation_cache_at) < _CORRELATION_CACHE_TTL:
            cached = _correlation_cache[cache_key]
            return {"pairs": cached[:limit], "generated_at": _correlation_cache_at, "cached": True}

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    from stock_data import get_price_history
    from correlation_service import compute_velocity_price_correlation

    rows = repo.get_all_active_narratives(limit=30)
    pairs = []

    # Collect all (narrative, ticker) work items
    work_items = []
    for n in rows:
        nid = n["narrative_id"]
        linked = json.loads(n.get("linked_assets") or "[]")
        tickers = [a.get("ticker") or a.get("symbol") for a in linked if isinstance(a, dict) and (a.get("ticker") or a.get("symbol"))]
        if not tickers:
            continue
        snapshots = repo.get_snapshot_history(nid, 90)
        vel_history = [{"date": s.get("snapshot_date"), "velocity": s.get("velocity")} for s in reversed(snapshots) if s.get("snapshot_date")]
        if len(vel_history) < 2:
            continue
        for ticker in tickers[:3]:
            work_items.append((nid, n, ticker.upper(), vel_history))

    futures = {}
    for nid, n, ticker, vel_history in work_items:
        future = _REQUEST_EXECUTOR.submit(get_price_history, ticker, 90)
        futures[future] = (nid, n, ticker, vel_history)

    for future in futures:
        nid, n, ticker, vel_history = futures[future]
        try:
            price_data = future.result(timeout=30)
            if not price_data:
                continue
            result = compute_velocity_price_correlation(vel_history, price_data, lead_days)
            pairs.append({
                "narrative_id": nid,
                "narrative_name": n.get("name") or "",
                "ticker": ticker,
                "correlation": result["correlation"],
                "p_value": result["p_value"],
                "n_observations": result["n_observations"],
                "is_significant": bool(result["is_significant"]),
                "interpretation": result["interpretation"],
                "lead_days": lead_days,
            })
        except (FuturesTimeout, Exception):
            continue

    pairs.sort(key=lambda p: abs(p["correlation"]), reverse=True)
    with _correlation_cache_lock:
        if len(_correlation_cache) > 50:
            _correlation_cache.clear()
        _correlation_cache[cache_key] = pairs
        _correlation_cache_at = _time.time()
    return {"pairs": pairs[:limit], "generated_at": _correlation_cache_at, "cached": False}


@app.get("/api/narratives/{narrative_id}/correlations")
@limiter.limit("10/minute")
@_timeout()
def get_narrative_correlations(request: Request, narrative_id: str = FPath(..., max_length=50), lead_days: int = Query(1, ge=0, le=90), user: dict = Depends(get_optional_user)):
    """All ticker correlations for one narrative."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")

    from stock_data import get_price_history
    from correlation_service import compute_velocity_price_correlation

    linked = json.loads(n.get("linked_assets") or "[]")
    tickers = [a.get("ticker") or a.get("symbol") for a in linked if isinstance(a, dict) and (a.get("ticker") or a.get("symbol"))]

    snapshots = repo.get_snapshot_history(narrative_id, 90)
    vel_history = [{"date": s.get("snapshot_date"), "velocity": s.get("velocity")} for s in reversed(snapshots) if s.get("snapshot_date")]

    results = []
    futures = {
        _REQUEST_EXECUTOR.submit(get_price_history, ticker.upper(), 90): ticker.upper()
        for ticker in tickers
    }
    for future in futures:
        ticker = futures[future]
        try:
            price_data = future.result(timeout=30)
            if not price_data:
                continue
            result = compute_velocity_price_correlation(vel_history, price_data, lead_days)
            result["ticker"] = ticker
            result["narrative_id"] = narrative_id
            results.append(result)
        except (FuturesTimeout, Exception):
            continue

    results.sort(key=lambda r: abs(r["correlation"]), reverse=True)
    return results


# --- Phase 0: Multi-signal analysis ---

@app.get("/api/analytics/signal-ranking")
@limiter.limit("10/minute")
@_timeout()
def get_signal_ranking(request: Request, days: int = 90, threshold: float = 2.0, user: dict = Depends(get_optional_user)):
    """
    Runs generalized correlation across ALL snapshot metrics for ALL qualifying
    narratives at multiple lead_days values. Returns metrics ranked by predictive
    power (hit_rate) plus a redundancy matrix showing inter-metric correlation.

    This endpoint is slow (potentially 30-60 seconds) because it runs hundreds of
    correlation computations. It is an analysis endpoint for manual/infrequent use.
    Do NOT cache or pre-compute.

    threshold is unused in ranking logic but reserved for future filtering.
    """
    days = max(7, min(days, 365))
    import math
    import warnings
    from scipy.stats import pearsonr
    from correlation_service import (
        compute_metric_price_correlation,
        SNAPSHOT_METRICS,
    )
    from stock_data import get_price_history

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    # Qualifying narratives: Growing+ stage, not suppressed, has real tickers
    all_narratives = repo.get_all_active_narratives()
    qualifying_stages = {"Growing", "Mature", "Declining"}
    qualifying = []
    for n in all_narratives:
        if n.get("stage") not in qualifying_stages:
            continue
        try:
            linked = json.loads(n.get("linked_assets") or "[]")
        except (json.JSONDecodeError, TypeError):
            continue
        tickers = []
        for a in linked:
            if isinstance(a, dict):
                t = a.get("ticker") or a.get("symbol") or ""
            elif isinstance(a, str):
                t = a
            else:
                continue
            if t and not t.startswith("TOPIC:"):
                tickers.append(t.upper())
        if tickers:
            qualifying.append({"narrative": n, "tickers": list(set(tickers))})

    if not qualifying:
        return {
            "metrics_tested": SNAPSHOT_METRICS,
            "rankings": [],
            "redundancy_matrix": {},
            "sample_size": 0,
            "days_analyzed": days,
        }

    lead_days_values = [0, 1, 2, 3, 5, 7]

    # Collect unique tickers and fetch price histories once
    all_tickers = set()
    for q in qualifying:
        all_tickers.update(q["tickers"])

    price_cache: dict[str, list[dict]] = {}
    futures = {
        _REQUEST_EXECUTOR.submit(get_price_history, t, days): t
        for t in all_tickers
    }
    for future in futures:
        ticker = futures[future]
        try:
            data = future.result(timeout=30)
            if data:
                price_cache[ticker] = data
        except Exception:
                continue

    # For each narrative-ticker pair, get snapshot history and run correlations
    # Structure: results[metric][lead_days] = list of {r, p, n, is_significant}
    results: dict[str, dict[int, list[dict]]] = {
        m: {ld: [] for ld in lead_days_values} for m in SNAPSHOT_METRICS
    }

    # Collect aligned metric rows for redundancy matrix (same snapshot = same row)
    redundancy_rows: list[dict[str, float]] = []

    for q in qualifying:
        nid = q["narrative"]["narrative_id"]
        snapshots = repo.get_snapshot_history(nid, days)
        if not snapshots:
            continue

        # Build metric history (oldest first) — one pass, reused for all metrics
        snap_list = list(reversed(snapshots))
        metric_history = [
            {**{"date": s.get("snapshot_date")}, **{m: s.get(m) for m in SNAPSHOT_METRICS}}
            for s in snap_list
            if s.get("snapshot_date")
        ]

        # Collect aligned rows for redundancy matrix
        for entry in metric_history:
            row = {}
            for m in SNAPSHOT_METRICS:
                v = entry.get(m)
                if v is not None:
                    try:
                        row[m] = float(v)
                    except (ValueError, TypeError):
                        continue
            if len(row) >= 2:
                redundancy_rows.append(row)

        for ticker in q["tickers"]:
            if ticker not in price_cache:
                continue
            price_data = price_cache[ticker]

            for metric in SNAPSHOT_METRICS:
                for ld in lead_days_values:
                    result = compute_metric_price_correlation(
                        metric_history, price_data,
                        metric_key=metric, lead_days=ld,
                    )
                    results[metric][ld].append(result)

    # Aggregate rankings per metric
    rankings = []
    for metric in SNAPSHOT_METRICS:
        best_ld = 0
        best_hit_rate = -1.0

        for ld in lead_days_values:
            pairs = results[metric][ld]
            total = len(pairs)
            if total == 0:
                continue
            sig = sum(1 for p in pairs if p["is_significant"])
            hr = sig / total
            if hr > best_hit_rate:
                best_hit_rate = hr
                best_ld = ld

        best_pairs = results[metric][best_ld]
        total_pairs = len(best_pairs)
        sig_pairs = [p for p in best_pairs if p["is_significant"]]
        significant_count = len(sig_pairs)
        avg_corr = (
            round(sum(abs(p["correlation"]) for p in sig_pairs) / significant_count, 4)
            if significant_count > 0 else 0.0
        )

        rankings.append({
            "metric": metric,
            "best_lead_days": best_ld,
            "avg_correlation": avg_corr,
            "hit_rate": round(best_hit_rate, 4) if best_hit_rate >= 0 else 0.0,
            "significant_pairs": significant_count,
            "total_pairs": total_pairs,
        })

    rankings.sort(key=lambda r: r["hit_rate"], reverse=True)

    # Redundancy matrix: pairwise Pearson r between metrics themselves
    # NOTE: rows are pooled across narratives (not per-narrative), so this
    # measures global metric covariance, not within-narrative redundancy.
    redundancy_matrix = {}
    for i, m1 in enumerate(SNAPSHOT_METRICS):
        for m2 in SNAPSHOT_METRICS[i + 1:]:
            # Extract aligned pairs where both metrics have values in the same row
            pairs_1 = []
            pairs_2 = []
            for row in redundancy_rows:
                if m1 in row and m2 in row:
                    pairs_1.append(row[m1])
                    pairs_2.append(row[m2])
            if len(pairs_1) < 10:
                continue
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    r, _ = pearsonr(pairs_1, pairs_2)
                if not math.isnan(r):
                    redundancy_matrix[f"{m1}_vs_{m2}"] = round(float(r), 4)
            except Exception:
                continue

    return {
        "metrics_tested": SNAPSHOT_METRICS,
        "rankings": rankings,
        "redundancy_matrix": redundancy_matrix,
        "narrative_ticker_pairs": rankings[0]["total_pairs"] if rankings else 0,
        "days_analyzed": days,
    }


# --- 1.4: Per-domain source breakdown ---

@app.get("/api/narratives/{narrative_id}/sources")
def get_narrative_sources(narrative_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Returns per-domain source breakdown for a narrative."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")

    evidence = repo.get_document_evidence(narrative_id)
    domain_data: dict[str, dict] = {}
    for doc in evidence:
        domain = doc.get("source_domain") or "unknown"
        if domain not in domain_data:
            domain_data[domain] = {"domain": domain, "count": 0, "category": _categorize_domain(domain), "latest_at": ""}
        domain_data[domain]["count"] += 1
        pub = doc.get("published_at") or ""
        if pub > domain_data[domain]["latest_at"]:
            domain_data[domain]["latest_at"] = pub

    total = len(evidence) or 1
    result = sorted(domain_data.values(), key=lambda d: d["count"], reverse=True)
    for r in result:
        r["percentage"] = round(r["count"] / total * 100, 1)
    return result


# --- 1.5: Candidate buffer indicator ---

@app.get("/api/pipeline/buffer")
def get_buffer_status(user: dict = Depends(get_optional_user)):
    """Returns candidate buffer counts."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    pending = repo.get_candidate_buffer_count("pending")
    clustered = repo.get_candidate_buffer_count("clustered")
    return {"pending": pending, "clustered": clustered, "total": pending + clustered}


# --- 1.8: Paginated evidence documents ---

@app.get("/api/narratives/{narrative_id}/documents")
def get_narrative_documents(narrative_id: str = FPath(..., max_length=50), limit: int = 10, offset: int = 0, user: dict = Depends(get_optional_user)):
    """Paginated evidence documents for a narrative."""
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")
    total = repo.count_document_evidence(narrative_id)
    page = repo.get_document_evidence(narrative_id, limit=limit, offset=offset)
    return {"items": page, "total": total, "limit": limit, "offset": offset}


# --- 1.2: Alert count for bell badge (Phase 2.2 prep) ---

@app.get("/api/alerts/count")
def get_alert_count(user: dict = Depends(get_optional_user)):
    """Returns unread alert count for badge display."""
    repo = get_repo()
    if repo is None:
        return {"unread": 0}
    mgr = NotificationManager(repo)
    notifs = mgr.get_notifications(user["user_id"], unread_only=True)
    return {"unread": len(notifs)}


@app.get("/api/alerts/stream")
@limiter.limit("2/minute")
async def alert_stream(request: Request, user: dict = Depends(get_optional_user)):
    """SSE stream for real-time alert delivery. Polls new notifications every 5 seconds."""
    user_id = user["user_id"]

    if _SSE_AVAILABLE:
        async def event_generator():
            last_check = datetime.now(timezone.utc)
            try:
                yield {"data": json.dumps({"type": "connected"})}
                while True:
                    await asyncio.sleep(5)
                    repo = get_repo()
                    if repo is not None:
                        new_alerts = repo.get_notifications_since(user_id, last_check)
                        for alert in new_alerts:
                            yield {"event": "alert", "data": json.dumps(alert)}
                    last_check = datetime.now(timezone.utc)
            except Exception:
                pass
        return EventSourceResponse(event_generator())
    else:
        async def manual_generator():
            last_check = datetime.now(timezone.utc)
            yield "data: {\"type\": \"connected\"}\n\n"
            while True:
                await asyncio.sleep(5)
                repo = get_repo()
                if repo is not None:
                    new_alerts = repo.get_notifications_since(user_id, last_check)
                    for alert in new_alerts:
                        yield f"event: alert\ndata: {json.dumps(alert)}\n\n"
                last_check = datetime.now(timezone.utc)
        return StreamingResponse(manual_generator(), media_type="text/event-stream")


# ===========================================================================
# V3 Phase 2 — Core Features
# ===========================================================================

# --- 2.1: Portfolio endpoints ---

class AddHoldingRequest(BaseModel):
    ticker: str
    shares: float = 0.0


@app.get("/api/portfolio")
def get_portfolio(user: dict = Depends(get_optional_user)):
    """Get user's portfolio with holdings."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    portfolio = repo.get_portfolio_by_user(user["user_id"])
    if portfolio is None:
        return {"portfolio": None, "holdings": []}
    holdings = repo.get_portfolio_holdings(portfolio["id"])
    # Enrich with current prices from tracked securities
    for h in holdings:
        sym = (h.get("ticker") or "").upper()
        sec = next((s for s in TRACKED_SECURITIES if s["symbol"].upper() == sym), None)
        if sec is None:
            sec = _DYNAMIC_SECURITIES.get(sym)
        h["current_price"] = sec["current_price"] if sec else None
        h["price_change_24h"] = sec.get("price_change_24h") if sec else None
    return {"portfolio": portfolio, "holdings": holdings}


@app.post("/api/portfolio/holdings")
def add_holding(req: AddHoldingRequest, user: dict = Depends(get_optional_user)):
    """Add ticker to portfolio. Auto-creates portfolio if needed."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")


    portfolio = repo.get_portfolio_by_user(user["user_id"])
    if portfolio is None:
        pid = str(uuid.uuid4())
        repo.create_portfolio({
            "id": pid,
            "user_id": user["user_id"],
            "name": "My Portfolio",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        portfolio = {"id": pid}

    # Check for duplicate
    existing = repo.get_portfolio_holdings(portfolio["id"])
    if any(h.get("ticker", "").upper() == req.ticker.upper() for h in existing):
        return {"status": "already_exists"}

    holding_id = str(uuid.uuid4())
    repo.add_portfolio_holding({
        "id": holding_id,
        "portfolio_id": portfolio["id"],
        "ticker": req.ticker.upper(),
        "shares": req.shares,
        "added_at": datetime.now(timezone.utc).isoformat(),
    })
    repo.update_portfolio_timestamp(portfolio["id"])
    return {"status": "added", "holding_id": holding_id}


@app.delete("/api/portfolio/holdings/{holding_id}")
def remove_holding(holding_id: str = FPath(..., max_length=50), user: dict = Depends(get_optional_user)):
    """Remove a holding from portfolio."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    holding = repo.get_portfolio_holding(holding_id)
    if holding is None:
        raise HTTPException(status_code=404, detail="Holding not found")
    if holding.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized to delete this holding")
    repo.delete_portfolio_holding(holding_id)
    return {"status": "removed"}


@app.get("/api/portfolio/exposure")
def get_portfolio_exposure(user: dict = Depends(get_optional_user)):
    """Compute narrative exposure for entire portfolio."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    portfolio = repo.get_portfolio_by_user(user["user_id"])
    if portfolio is None:
        return {"exposures": []}

    holdings = repo.get_portfolio_holdings(portfolio["id"])
    ticker_set = {h.get("ticker", "").upper() for h in holdings}

    # Find narratives linked to these tickers (inverse lookup from linked_assets)
    all_narratives = repo.get_all_active_narratives()
    exposures: list[dict] = []
    seen_narratives: set[str] = set()

    for n in all_narratives:
        linked = json.loads(n.get("linked_assets") or "[]")
        affected_tickers = []
        for asset in linked:
            if isinstance(asset, dict):
                ticker = (asset.get("ticker") or asset.get("symbol") or "").upper()
                if ticker in ticker_set:
                    affected_tickers.append(ticker)

        if affected_tickers and n["narrative_id"] not in seen_narratives:
            seen_narratives.add(n["narrative_id"])
            exposures.append({
                "narrative_id": n["narrative_id"],
                "narrative_name": n.get("name") or "",
                "stage": n.get("stage") or "Emerging",
                "velocity": float(n.get("velocity_windowed") or 0),
                "ns_score": float(n.get("ns_score") or 0),
                "affected_tickers": affected_tickers,
            })

    exposures.sort(key=lambda e: abs(e["velocity"]), reverse=True)
    return {"exposures": exposures}


# ---------------------------------------------------------------------------
# Phase 6 — Portfolio Analytics Endpoints
# ---------------------------------------------------------------------------

def _get_portfolio_holdings_with_prices(user_id: str, repo) -> tuple[list[dict], float]:
    """Helper: returns (enriched_holdings, total_value)."""
    portfolio = repo.get_portfolio_by_user(user_id)
    if portfolio is None:
        return [], 0.0
    holdings = repo.get_portfolio_holdings(portfolio["id"])
    total_value = 0.0
    for h in holdings:
        sym = (h.get("ticker") or "").upper()
        sec = next((s for s in TRACKED_SECURITIES if s["symbol"].upper() == sym), None)
        if sec is None:
            sec = _DYNAMIC_SECURITIES.get(sym)
        price = float((sec or {}).get("current_price") or 0)
        change_pct = float((sec or {}).get("price_change_24h") or 0)
        shares = float(h.get("shares") or 0)
        value = round(shares * price, 2)
        cost_basis = h.get("cost_basis")
        pnl = round(value - (float(cost_basis) * shares if cost_basis else value), 2)
        h["current_price"] = price
        h["price_change_24h"] = change_pct
        h["current_value"] = value
        h["day_change"] = round(value * change_pct / 100, 2)
        h["pnl"] = pnl
        total_value += value
    return holdings, total_value


@app.get("/api/portfolio/summary")
def get_portfolio_summary(user: dict = Depends(get_optional_user)):
    """Header stats: total_value, total_pnl, day_change, position_count."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    holdings, total_value = _get_portfolio_holdings_with_prices(user["user_id"], repo)
    total_pnl = sum(h.get("pnl", 0) for h in holdings)
    day_change = sum(h.get("day_change", 0) for h in holdings)
    day_change_pct = round((day_change / total_value * 100) if total_value else 0, 2)
    return {
        "total_value": round(total_value, 2),
        "total_pnl": round(total_pnl, 2),
        "day_change": round(day_change, 2),
        "day_change_pct": day_change_pct,
        "position_count": len(holdings),
    }


@app.get("/api/portfolio/allocation")
def get_portfolio_allocation(
    group_by: str = Query("sector", pattern="^(sector|asset_class|risk)$"),
    user: dict = Depends(get_optional_user),
):
    """Allocation breakdown for treemap visualization."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    holdings, total_value = _get_portfolio_holdings_with_prices(user["user_id"], repo)
    if not holdings or total_value == 0:
        return []

    groups: dict[str, dict] = {}
    for h in holdings:
        sym = (h.get("ticker") or "").upper()
        if group_by == "sector":
            label = SECTOR_MAP.get(sym, "Other")
        elif group_by == "asset_class":
            sec = next((s for s in TRACKED_SECURITIES if s["symbol"].upper() == sym), None)
            if sec:
                ac = next((a for a in ASSET_CLASSES if a["id"] == sec.get("asset_class_id")), None)
                label = ac["name"] if ac else "Other"
            else:
                label = "Other"
        else:  # risk — simple bucketing by price change magnitude
            chg = abs(h.get("price_change_24h") or 0)
            label = "High Risk" if chg > 3 else ("Medium Risk" if chg > 1 else "Low Risk")

        if label not in groups:
            groups[label] = {"group": label, "value": 0.0, "pnl": 0.0, "tickers": []}
        groups[label]["value"] += h.get("current_value", 0)
        groups[label]["pnl"] += h.get("pnl", 0)
        groups[label]["tickers"].append(sym)

    result = []
    for g in groups.values():
        result.append({
            "group": g["group"],
            "value": round(g["value"], 2),
            "pct": round(g["value"] / total_value, 4),
            "pnl": round(g["pnl"], 2),
            "tickers": g["tickers"],
        })
    result.sort(key=lambda x: x["value"], reverse=True)
    return result


_perf_cache: dict[str, tuple[float, dict]] = {}
_PERF_CACHE_TTL = 3600  # 1 hour


@app.get("/api/portfolio/performance")
def get_portfolio_performance(
    days: int = Query(90, ge=7, le=365),
    benchmark: str = Query("SPY", max_length=12),
    user: dict = Depends(get_optional_user),
):
    """Historical portfolio value vs benchmark."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    holdings, _ = _get_portfolio_holdings_with_prices(user["user_id"], repo)
    if not holdings:
        return {"portfolio": [], "benchmark": [], "total_return_pct": 0.0, "benchmark_return_pct": 0.0}

    import time as _t
    cache_key = f"{user['user_id']}:{days}:{benchmark}:" + ":".join(sorted(h["ticker"] for h in holdings))
    now = _t.time()
    if cache_key in _perf_cache:
        ts, data = _perf_cache[cache_key]
        if now - ts < _PERF_CACHE_TTL:
            return data

    try:
        from stock_data import get_price_history as _gph
    except ImportError:
        return {"portfolio": [], "benchmark": [], "total_return_pct": 0.0, "benchmark_return_pct": 0.0}

    # Build a date-indexed map of prices for each ticker
    price_maps: dict[str, dict[str, float]] = {}
    for h in holdings:
        hist = _gph(h["ticker"], days=days)
        price_maps[h["ticker"]] = {r["date"]: r["close"] for r in hist}

    bench_hist = _gph(benchmark, days=days)
    bench_map = {r["date"]: r["close"] for r in bench_hist}

    # Collect all dates present in all holdings
    all_dates: set[str] = set()
    for pm in price_maps.values():
        all_dates.update(pm.keys())
    sorted_dates = sorted(all_dates)

    # Compute portfolio value per day (using shares * price; fall back to last known price)
    portfolio_series: list[dict] = []
    last_prices: dict[str, float] = {}
    for d in sorted_dates:
        day_value = 0.0
        for h in holdings:
            p = price_maps[h["ticker"]].get(d)
            if p is not None:
                last_prices[h["ticker"]] = p
            p = last_prices.get(h["ticker"], 0.0)
            day_value += float(h.get("shares") or 0) * p
        portfolio_series.append({"date": d, "value": round(day_value, 2)})

    bench_series: list[dict] = [{"date": d, "value": bench_map[d]} for d in sorted_dates if d in bench_map]

    # Returns
    total_return_pct = 0.0
    if len(portfolio_series) >= 2 and portfolio_series[0]["value"]:
        total_return_pct = round((portfolio_series[-1]["value"] - portfolio_series[0]["value"]) / portfolio_series[0]["value"] * 100, 2)
    bench_return_pct = 0.0
    if len(bench_series) >= 2 and bench_series[0]["value"]:
        bench_return_pct = round((bench_series[-1]["value"] - bench_series[0]["value"]) / bench_series[0]["value"] * 100, 2)

    result = {
        "portfolio": portfolio_series,
        "benchmark": bench_series,
        "total_return_pct": total_return_pct,
        "benchmark_return_pct": bench_return_pct,
    }
    _perf_cache[cache_key] = (now, result)
    return result


@app.get("/api/portfolio/correlation")
def get_portfolio_correlation(user: dict = Depends(get_optional_user)):
    """NxN correlation matrix for holdings using 90-day price history."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    holdings, _ = _get_portfolio_holdings_with_prices(user["user_id"], repo)
    tickers = [h["ticker"] for h in holdings]
    if len(tickers) < 2:
        return {"tickers": tickers, "matrix": [[1.0]] if tickers else [], "warnings": []}

    try:
        from stock_data import get_price_history as _gph
        from scipy.stats import pearsonr as _pearsonr
    except ImportError:
        return {"tickers": tickers, "matrix": [], "warnings": [], "error": "scipy unavailable"}

    price_maps: dict[str, dict[str, float]] = {}
    for t in tickers:
        hist = _gph(t, days=90)
        price_maps[t] = {r["date"]: r["close"] for r in hist}

    # Intersect dates
    date_sets = [set(price_maps[t].keys()) for t in tickers]
    common_dates = sorted(date_sets[0].intersection(*date_sets[1:]))
    if len(common_dates) < 5:
        return {"tickers": tickers, "matrix": [], "warnings": [], "error": "insufficient data"}

    series: list[list[float]] = [[price_maps[t][d] for d in common_dates] for t in tickers]

    n = len(tickers)
    matrix: list[list[float]] = [[1.0] * n for _ in range(n)]
    warnings: list[dict] = []
    for i in range(n):
        for j in range(i + 1, n):
            try:
                r, _ = _pearsonr(series[i], series[j])
                r = round(float(r), 4)
            except Exception:
                r = 0.0
            matrix[i][j] = r
            matrix[j][i] = r
            if r > 0.7:
                warnings.append({
                    "pair": [tickers[i], tickers[j]],
                    "correlation": r,
                    "severity": "high" if r > 0.85 else "medium",
                })

    return {"tickers": tickers, "matrix": matrix, "warnings": warnings}


@app.get("/api/portfolio/concentration")
def get_portfolio_concentration(user: dict = Depends(get_optional_user)):
    """Concentration risk: top3_pct, sector HHI, single-stock warnings."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    holdings, total_value = _get_portfolio_holdings_with_prices(user["user_id"], repo)
    if not holdings or total_value == 0:
        return {"top3_pct": 0.0, "top3_warning": False, "sector_hhi": 0.0, "sector_concentrated": False, "single_stock_warnings": []}

    sorted_holdings = sorted(holdings, key=lambda h: h.get("current_value", 0), reverse=True)
    top3_value = sum(h.get("current_value", 0) for h in sorted_holdings[:3])
    top3_pct = round(top3_value / total_value, 4)

    single_stock_warnings = []
    sector_values: dict[str, float] = {}
    for h in holdings:
        sym = (h.get("ticker") or "").upper()
        pct = h.get("current_value", 0) / total_value
        if pct > 0.25:
            single_stock_warnings.append({"ticker": sym, "pct": round(pct, 4)})
        sector = SECTOR_MAP.get(sym, "Other")
        sector_values[sector] = sector_values.get(sector, 0) + h.get("current_value", 0)

    # HHI: sum of squared market shares (in %, so range 0-10000)
    hhi = sum((v / total_value * 100) ** 2 for v in sector_values.values())
    hhi = round(hhi, 2)

    return {
        "top3_pct": top3_pct,
        "top3_warning": top3_pct > 0.60,
        "sector_hhi": hhi,
        "sector_concentrated": hhi > 2500,
        "single_stock_warnings": single_stock_warnings,
    }


@app.post("/api/portfolio/import")
async def import_portfolio_csv(
    file: UploadFile = File(...),
    user: dict = Depends(get_optional_user),
):
    """CSV import: columns ticker,shares (max 1000 rows)."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    content = await file.read()
    try:
        csv_text = content.decode("utf-8")
    except UnicodeDecodeError:
        csv_text = content.decode("latin-1", errors="replace")

    portfolio = repo.get_portfolio_by_user(user["user_id"])
    if portfolio is None:
        pid = str(uuid.uuid4())
        repo.create_portfolio({
            "id": pid,
            "user_id": user["user_id"],
            "name": "My Portfolio",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        portfolio = {"id": pid}

    import csv as _csv
    import io as _io
    reader = _csv.DictReader(_io.StringIO(csv_text))
    imported = 0
    errors: list[dict] = []
    row_num = 0
    for row in reader:
        row_num += 1
        if imported >= 1000:
            errors.append({"row": row_num, "error": "Import capped at 1000 rows"})
            break
        try:
            ticker = row.get("ticker", "").strip().upper()
            shares = float(row.get("shares", 0) or 0)
            cost_basis_raw = row.get("cost_basis", "").strip()
            cost_basis = float(cost_basis_raw) if cost_basis_raw else None
            if not ticker or shares <= 0:
                errors.append({"row": row_num, "error": f"Invalid ticker or shares: {ticker},{shares}"})
                continue
            holding_id = str(uuid.uuid4())
            repo.add_portfolio_holding({
                "id": holding_id,
                "portfolio_id": portfolio["id"],
                "ticker": ticker,
                "shares": shares,
                "cost_basis": cost_basis,
                "added_at": datetime.now(timezone.utc).isoformat(),
            })
            imported += 1
        except Exception as exc:
            errors.append({"row": row_num, "error": str(exc)})
    if imported > 0:
        repo.update_portfolio_timestamp(portfolio["id"])
    return {"imported": imported, "errors": errors}


# --- 2.4: Story timeline endpoints ---

@app.get("/api/narratives/{narrative_id}/timeline")
def get_narrative_timeline(narrative_id: str = FPath(..., max_length=50), days: int = 30, user: dict = Depends(get_optional_user)):
    """Returns daily snapshots + mutations as a unified timeline."""
    days = max(1, min(days, 365))
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")

    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=days)).isoformat()
    snapshots = repo.get_snapshots_range(narrative_id, start_date, end_date)

    mutations = []
    try:
        mutations = repo.get_mutations_for_narrative(narrative_id, limit=100)
    except Exception:
        pass

    # Build a date-indexed mutation lookup
    mut_by_date: dict[str, list] = {}
    for m in mutations:
        d = (m.get("detected_at") or "")[:10]
        mut_by_date.setdefault(d, []).append({
            "type": m.get("mutation_type"),
            "description": m.get("ai_explanation") or m.get("mutation_type"),
        })

    timeline = []
    for s in reversed(snapshots):  # oldest first
        d = s.get("snapshot_date") or ""
        timeline.append({
            "date": d,
            "ns_score": s.get("ns_score"),
            "velocity": s.get("velocity"),
            "document_count": s.get("doc_count"),
            "stage": s.get("lifecycle_stage"),
            "mutations": mut_by_date.get(d, []),
        })

    return {"narrative_id": narrative_id, "timeline": timeline}


@app.get("/api/narratives/{narrative_id}/changelog")
def get_narrative_changelog(
    narrative_id: str = FPath(..., max_length=50),
    days: int = 30,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: dict = Depends(get_optional_user),
):
    """Returns enriched mutation events as a changelog for audit/trust."""
    days = max(1, min(days, 365))
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")

    total = repo.count_changelog_for_narrative(narrative_id, days=days)
    mutations = repo.get_changelog_for_narrative(narrative_id, days=days, limit=limit, offset=offset)

    entries = []
    for m in mutations:
        contrib_docs = None
        raw_contrib = m.get("contributing_documents")
        if raw_contrib:
            try:
                contrib_docs = json.loads(raw_contrib)
            except (json.JSONDecodeError, TypeError):
                contrib_docs = None

        entries.append({
            "id": m.get("id"),
            "detected_at": m.get("detected_at"),
            "mutation_type": m.get("mutation_type"),
            "previous_value": m.get("previous_value"),
            "new_value": m.get("new_value"),
            "magnitude": m.get("magnitude"),
            "explanation": m.get("haiku_explanation") or "",
            "contributing_documents": contrib_docs,
            "pipeline_run_id": m.get("pipeline_run_id"),
        })

    return {
        "narrative_id": narrative_id,
        "narrative_name": n.get("name") or "",
        "days": days,
        "total_changes": total,
        "limit": limit,
        "offset": offset,
        "changelog": entries,
    }


@app.get("/api/narratives/{narrative_id}/compare")
def compare_narrative_snapshots(narrative_id: str = FPath(..., max_length=50), date1: str = Query(...), date2: str = Query(...), user: dict = Depends(get_optional_user)):
    """Side-by-side comparison of two snapshots."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")

    snap1 = repo.get_snapshot(narrative_id, date1)
    snap2 = repo.get_snapshot(narrative_id, date2)

    narrative_name = n["name"] if n else narrative_id

    differences = []
    if snap1 and snap2:
        for key in ["ns_score", "velocity", "lifecycle_stage", "doc_count", "entropy", "cohesion", "polarization"]:
            v1 = snap1.get(key)
            v2 = snap2.get(key)
            if v1 != v2:
                differences.append({"field": key, "date1_value": v1, "date2_value": v2})

    return {
        "narrative_name": narrative_name,
        "date1": date1,
        "date2": date2,
        "date1_data": snap1,
        "date2_data": snap2,
        "differences": differences,
    }


# ===========================================================================
# V3 Phase 3 — Data Enrichment
# ===========================================================================

@app.get("/api/earnings/upcoming")
def get_upcoming_earnings(days: int = 14, user: dict = Depends(get_optional_user)):
    """Returns upcoming earnings dates for linked tickers."""
    days = max(1, min(days, 365))
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    # Collect unique tickers from all narratives' linked_assets + tracked securities
    tickers = set()
    for sec in TRACKED_SECURITIES:
        tickers.add(sec["symbol"].upper())
    for sym in _DYNAMIC_SECURITIES:
        tickers.add(sym.upper())

    try:
        from earnings_service import get_upcoming_earnings as _get_earnings
        results = _get_earnings(sorted(tickers)[:30])  # cap at 30 to avoid slow calls
        if days:
            results = [r for r in results if r.get("days_until") is None or r["days_until"] <= days]
        return results
    except Exception as e:
        print(f"[Earnings] Failed to fetch earnings calendar: {e}")
        return []


# ===========================================================================
# Phase 2 Batch 2 — Analytics Endpoints
# ===========================================================================


@app.get("/api/analytics/narrative-histories")
@limiter.limit("10/minute")
@_timeout()
def get_narrative_histories(request: Request, days: int = 30, user: dict = Depends(get_optional_user)):
    """Bulk narrative histories with gap backfill. Uses days as calendar date range."""
    days = max(1, min(days, 365))
    from datetime import date, timedelta

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    today = date.today()
    cutoff = (today - timedelta(days=days)).isoformat()

    raw_snapshots = repo.get_bulk_narrative_snapshots(cutoff)
    all_narratives = repo.get_all_active_narratives()
    meta_by_id = {n["narrative_id"]: n for n in all_narratives}

    # Group snapshots by narrative_id
    grouped: dict[str, dict[str, dict]] = {}
    for s in raw_snapshots:
        nid = s["narrative_id"]
        grouped.setdefault(nid, {})[s["snapshot_date"]] = s

    result_narratives = {}
    for nid, snap_by_date in grouped.items():
        meta = meta_by_id.get(nid, {})
        history = []
        last_known = None
        current = today - timedelta(days=days)

        while current <= today:
            d = current.isoformat()
            if d in snap_by_date:
                s = snap_by_date[d]
                entry = {
                    "date": d,
                    "ns_score": s.get("ns_score"),
                    "velocity": s.get("velocity"),
                    "entropy": s.get("entropy"),
                    "cohesion": s.get("cohesion"),
                    "polarization": s.get("polarization"),
                    "doc_count": s.get("doc_count"),
                    "burst_ratio": s.get("burst_ratio"),
                    "gap_filled": False,
                }
                last_known = {k: v for k, v in entry.items() if k != "date"}
            elif last_known:
                entry = {"date": d, **{k: v for k, v in last_known.items()}}
                entry["gap_filled"] = True
            else:
                entry = {
                    "date": d, "ns_score": None, "velocity": None,
                    "entropy": None, "cohesion": None, "polarization": None,
                    "doc_count": None, "burst_ratio": None, "gap_filled": True,
                }
            history.append(entry)
            current += timedelta(days=1)

        result_narratives[nid] = {
            "name": meta.get("name", ""),
            "stage": meta.get("stage", ""),
            "history": history,
        }

    return {
        "days": days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "narratives": result_narratives,
    }


@app.get("/api/analytics/momentum-leaderboard")
@limiter.limit("10/minute")
@_timeout()
def get_momentum_leaderboard(request: Request, days: int = 7, user: dict = Depends(get_optional_user)):
    """Narratives ranked by momentum score (velocity trend via linear regression)."""
    from datetime import date, timedelta
    from scipy.stats import linregress

    days = max(2, min(days, 90))

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    all_narratives = repo.get_all_active_narratives()
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    velocity_rows = repo.get_velocity_snapshots_bulk(cutoff)

    # Group velocity snapshots by narrative_id
    vel_by_nid: dict[str, list] = {}
    for r in velocity_rows:
        vel_by_nid.setdefault(r["narrative_id"], []).append(r)

    leaderboard = []
    for n in all_narratives:
        nid = n["narrative_id"]
        current_velocity = n.get("velocity") or 0.0
        snapshots = vel_by_nid.get(nid, [])
        snapshots_available = len(snapshots)

        if snapshots_available >= 2:
            velocities = [s.get("velocity") or 0.0 for s in snapshots]
            x = list(range(len(velocities)))
            slope = linregress(x, velocities).slope
        else:
            slope = 0.0

        denom = max(abs(current_velocity), 0.01)
        slope_normalized = max(-1.0, min(1.0, slope / denom))
        momentum_score = current_velocity * (1 + slope_normalized)

        if slope > 0.05:
            slope_direction = "accelerating"
        elif slope < -0.05:
            slope_direction = "decelerating"
        else:
            slope_direction = "steady"

        # Top 3 linked assets (tickers only)
        assets = _parse_linked_assets(n)
        top_tickers = []
        for a in assets[:3]:
            if isinstance(a, dict):
                top_tickers.append(a.get("ticker", ""))
            else:
                top_tickers.append(str(a))

        burst_ratio = n.get("burst_ratio") or 0.0
        leaderboard.append({
            "narrative_id": nid,
            "name": n.get("name", ""),
            "stage": n.get("stage", ""),
            "current_velocity": round(current_velocity, 6),
            "momentum_score": round(momentum_score, 6),
            "slope": round(slope, 6),
            "slope_direction": slope_direction,
            "linked_assets": top_tickers,
            "burst_active": burst_ratio >= 3.0,
            "data_quality": {"snapshots_available": snapshots_available},
        })

    leaderboard.sort(key=lambda x: x["momentum_score"], reverse=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "leaderboard": leaderboard,
    }


@app.get("/api/analytics/narrative-overlap")
@limiter.limit("10/minute")
@_timeout()
def get_narrative_overlap(request: Request, days: int = 30, user: dict = Depends(get_optional_user)):
    """NxN overlap matrix (doc + asset + topic) for active narratives. Cached 4h."""
    days = max(1, min(days, 365))
    import time as _time
    global _overlap_cache

    cache_key = f"overlap_{days}"
    cached_entry = _overlap_cache.get(cache_key)
    if cached_entry and (_time.time() - cached_entry["cached_at"]) < _OVERLAP_CACHE_TTL:
        result_copy = cached_entry["result"].copy()
        result_copy["cached"] = True
        return result_copy

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    all_narratives = repo.get_all_active_narratives()
    if not all_narratives:
        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cached": False,
            "narratives": [],
            "matrix": [],
        }
        return result

    nid_list = [n["narrative_id"] for n in all_narratives]
    nid_index = {nid: i for i, nid in enumerate(nid_list)}
    n_count = len(nid_list)

    # Document overlap data (bulk queries)
    doc_overlaps = repo.get_document_overlaps()
    doc_counts = repo.get_doc_counts_per_narrative()

    # Build doc overlap lookup: (nid1, nid2) -> shared_docs
    doc_shared: dict[tuple, int] = {}
    for row in doc_overlaps:
        doc_shared[(row["nid1"], row["nid2"])] = row["shared_docs"]

    # Parse linked_assets and topic_tags per narrative
    asset_sets: dict[str, dict[str, float]] = {}  # nid -> {ticker: similarity}
    tag_sets: dict[str, set] = {}
    for n in all_narratives:
        nid = n["narrative_id"]
        assets = _parse_linked_assets(n)
        ticker_scores = {}
        for a in assets:
            if isinstance(a, dict):
                ticker_scores[a.get("ticker", "")] = a.get("similarity_score", 0.5)
            else:
                ticker_scores[str(a)] = 0.5
        asset_sets[nid] = ticker_scores

        tags_raw = n.get("topic_tags")
        if tags_raw and isinstance(tags_raw, str):
            try:
                tag_sets[nid] = set(json.loads(tags_raw))
            except Exception:
                tag_sets[nid] = set()
        elif tags_raw and isinstance(tags_raw, list):
            tag_sets[nid] = set(tags_raw)
        else:
            tag_sets[nid] = set()

    # Build NxN matrix
    matrix = [[0.0] * n_count for _ in range(n_count)]
    for i in range(n_count):
        matrix[i][i] = 1.0
        for j in range(i + 1, n_count):
            nid_a, nid_b = nid_list[i], nid_list[j]

            # Document overlap (Jaccard)
            key = (nid_a, nid_b) if nid_a < nid_b else (nid_b, nid_a)
            shared = doc_shared.get(key, 0)
            count_a = doc_counts.get(nid_a, 0)
            count_b = doc_counts.get(nid_b, 0)
            union = count_a + count_b - shared
            doc_jaccard = shared / union if union > 0 else 0.0

            # Asset overlap (shared tickers weighted by avg similarity)
            tickers_a = set(asset_sets.get(nid_a, {}).keys())
            tickers_b = set(asset_sets.get(nid_b, {}).keys())
            shared_tickers = tickers_a & tickers_b
            union_tickers = tickers_a | tickers_b
            if union_tickers:
                asset_overlap = len(shared_tickers) / len(union_tickers)
            else:
                asset_overlap = 0.0

            # Topic overlap (tag Jaccard)
            tags_a = tag_sets.get(nid_a, set())
            tags_b = tag_sets.get(nid_b, set())
            shared_tags = tags_a & tags_b
            union_tags = tags_a | tags_b
            topic_overlap = len(shared_tags) / len(union_tags) if union_tags else 0.0

            composite = 0.5 * doc_jaccard + 0.3 * asset_overlap + 0.2 * topic_overlap
            matrix[i][j] = round(composite, 4)
            matrix[j][i] = round(composite, 4)

    narratives_meta = [
        {"id": n["narrative_id"], "name": n.get("name", ""),
         "stage": n.get("stage", ""), "ns_score": n.get("ns_score", 0.0)}
        for n in all_narratives
    ]

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cached": False,
        "narratives": narratives_meta,
        "matrix": matrix,
    }

    if len(_overlap_cache) > 10:
        _overlap_cache.clear()
    _overlap_cache[cache_key] = {"result": result, "cached_at": _time.time()}
    return result


@app.get("/api/analytics/sector-convergence")
@limiter.limit("10/minute")
@_timeout()
def get_sector_convergence(request: Request, days: int = 30, user: dict = Depends(get_optional_user)):
    """Per-sector narrative pressure aggregation via linked assets."""
    days = max(1, min(days, 365))
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    all_narratives = repo.get_all_active_narratives()

    # Local cache for sector lookups to avoid redundant DB calls
    sector_cache: dict[str, str] = {}

    def resolve_sector(ticker: str) -> str:
        t = ticker.upper()
        if t in sector_cache:
            return sector_cache[t]
        cached = repo.get_stock_cache(t)
        if cached and cached.get("sector"):
            sector_cache[t] = cached["sector"]
            return cached["sector"]
        sector = SECTOR_MAP.get(t, "Other")
        sector_cache[t] = sector
        return sector

    # Aggregate per sector
    sectors: dict[str, dict] = {}  # sector_name -> aggregation
    for n in all_narratives:
        nid = n["narrative_id"]
        ns_score = n.get("ns_score") or 0.0
        assets = _parse_linked_assets(n)

        # Track which sectors this narrative contributes to (dedupe per narrative)
        narrative_sectors: dict[str, float] = {}  # sector -> max_similarity for dedup

        for a in assets:
            if isinstance(a, dict):
                ticker = a.get("ticker", "")
                similarity = a.get("similarity_score", 0.5)
            else:
                ticker = str(a)
                similarity = 0.5

            if not ticker:
                continue

            sector = resolve_sector(ticker)
            if sector not in sectors:
                sectors[sector] = {
                    "name": sector,
                    "narrative_ids": set(),
                    "weighted_pressure": 0.0,
                    "contributing_narratives": {},
                    "asset_scores": {},
                }

            sec = sectors[sector]
            sec["weighted_pressure"] += ns_score * similarity
            sec["narrative_ids"].add(nid)

            # Track best contribution per narrative per sector
            if nid not in sec["contributing_narratives"]:
                sec["contributing_narratives"][nid] = {
                    "narrative_id": nid,
                    "name": n.get("name", ""),
                    "ns_score": ns_score,
                    "stage": n.get("stage", ""),
                }

            # Track top assets per sector
            if ticker not in sec["asset_scores"] or similarity > sec["asset_scores"][ticker]:
                sec["asset_scores"][ticker] = similarity

    # Format response
    result_sectors = []
    for sec_data in sectors.values():
        top_assets = sorted(
            [{"ticker": t, "similarity_score": round(s, 4)}
             for t, s in sec_data["asset_scores"].items()],
            key=lambda x: x["similarity_score"],
            reverse=True,
        )[:10]

        result_sectors.append({
            "name": sec_data["name"],
            "narrative_count": len(sec_data["narrative_ids"]),
            "weighted_pressure": round(sec_data["weighted_pressure"], 4),
            "contributing_narratives": list(sec_data["contributing_narratives"].values()),
            "top_assets": top_assets,
        })

    result_sectors.sort(key=lambda x: x["weighted_pressure"], reverse=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sectors": result_sectors,
    }


# ===========================================================================
# Analytics Endpoint 5: Lifecycle Funnel
# ===========================================================================

_STAGE_ORDER = ["Emerging", "Growing", "Mature", "Declining", "Dormant"]
_STAGE_IDX = {s: i for i, s in enumerate(_STAGE_ORDER)}


@app.get("/api/analytics/lifecycle-funnel")
@limiter.limit("10/minute")
@_timeout()
def get_lifecycle_funnel(request: Request, days: int = 30, user: dict = Depends(get_optional_user)):
    """Stage transition funnel from mutation_events."""
    days = max(1, min(days, 365))
    from collections import defaultdict

    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    # --- Stage counts from current narratives table ---
    all_narratives = repo.get_all_active_narratives()
    stage_counts = {s: 0 for s in _STAGE_ORDER}
    created_at_map: dict[str, str] = {}
    for n in all_narratives:
        stage = n.get("stage", "Emerging")
        if stage in stage_counts:
            stage_counts[stage] += 1
        created_at_map[n.get("narrative_id", "")] = n.get("created_at", "")

    # --- Stage transitions from mutation_events ---
    mutations = repo.get_stage_change_mutations(days)

    # Group by (from, to) for counts
    transition_counts: dict[tuple[str, str], int] = defaultdict(int)
    for m in mutations:
        key = (m["previous_value"], m["new_value"])
        transition_counts[key] += 1

    # Group by narrative to compute avg_days between consecutive transitions
    nar_muts: dict[str, list[dict]] = defaultdict(list)
    for m in mutations:
        nar_muts[m["narrative_id"]].append(m)

    transition_days: dict[tuple[str, str], list[float]] = defaultdict(list)
    for nid, muts in nar_muts.items():
        for i, m in enumerate(muts):
            key = (m["previous_value"], m["new_value"])
            try:
                t_current = datetime.fromisoformat(
                    m["detected_at"].replace("Z", "+00:00")
                )
                if i == 0:
                    # First transition: measure from narrative created_at
                    ca = created_at_map.get(nid, "")
                    if ca:
                        t_prev = datetime.fromisoformat(ca.replace("Z", "+00:00"))
                    else:
                        continue
                else:
                    t_prev = datetime.fromisoformat(
                        muts[i - 1]["detected_at"].replace("Z", "+00:00")
                    )
                gap = (t_current - t_prev).total_seconds() / 86400
                if gap >= 0:
                    transition_days[key].append(gap)
            except Exception:
                pass

    # Build transitions list
    revival_count = 0
    total_transitions = sum(transition_counts.values())
    transitions = []
    for (from_s, to_s), count in transition_counts.items():
        entry: dict = {"from": from_s, "to": to_s, "count": count}
        timing = transition_days.get((from_s, to_s), [])
        if timing:
            entry["avg_days"] = round(sum(timing) / len(timing), 1)
        from_idx = _STAGE_IDX.get(from_s, 0)
        to_idx = _STAGE_IDX.get(to_s, 0)
        if from_idx > to_idx:
            entry["label"] = "Revival"
            revival_count += count
        transitions.append(entry)

    transitions.sort(
        key=lambda t: (_STAGE_IDX.get(t["from"], 0), _STAGE_IDX.get(t["to"], 0))
    )

    # Average lifespan: first snapshot to last_updated_at
    first_dates = repo.get_first_snapshot_dates()
    lifespans: list[float] = []
    for n in all_narratives:
        nid = n.get("narrative_id", "")
        first = first_dates.get(nid)
        last = n.get("last_updated_at")
        if first and last:
            try:
                d1 = datetime.fromisoformat(first)
                d2 = datetime.fromisoformat(last.replace("Z", "+00:00"))
                gap = (d2 - d1).total_seconds() / 86400
                if gap >= 0:
                    lifespans.append(gap)
            except Exception:
                pass
    avg_lifespan = round(sum(lifespans) / len(lifespans), 1) if lifespans else 0.0
    revival_rate = (
        round(revival_count / total_transitions, 2)
        if total_transitions > 0
        else 0.0
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": days,
        "stage_counts": stage_counts,
        "transitions": transitions,
        "avg_lifespan_days": avg_lifespan,
        "revival_rate": revival_rate,
    }


# ===========================================================================
# Analytics Endpoint 6: Lead Time Distribution
# ===========================================================================


@app.get("/api/analytics/lead-time-distribution")
@limiter.limit("10/minute")
def get_lead_time_distribution(request: Request, days: int = 90, threshold: float = 2.0, user: dict = Depends(get_optional_user)):
    """Pre-computed lead time distribution. Served from background cache."""
    days = max(1, min(days, 365))
    import time as _time

    cache_key = str(threshold)
    cached = _lead_time_cache.get(cache_key)
    is_cached = (
        cached is not None
        and (_time.time() - _lead_time_cache_at) < _LEAD_TIME_CACHE_TTL
    )

    if is_cached and cached:
        return {
            "generated_at": datetime.fromtimestamp(
                _lead_time_cache_at, tz=timezone.utc
            ).isoformat(),
            "cached": True,
            **cached,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cached": False,
        "data_points": [],
        "histogram_buckets": [],
        "median_lead_days": 0,
        "mean_lead_days": 0,
        "hit_rate": 0.0,
    }


# ===========================================================================
# Analytics Endpoint 7: Contrarian Signals
# ===========================================================================


@app.get("/api/analytics/contrarian-signals")
@limiter.limit("10/minute")
def get_contrarian_signals(request: Request, days: int = 30, user: dict = Depends(get_optional_user)):
    """Coordination-flagged narratives with price enrichment. From background cache."""
    import time as _time
    days = max(1, min(days, 365))

    is_cached = _contrarian_cache and (
        _time.time() - _contrarian_cache_at
    ) < _CONTRARIAN_CACHE_TTL

    if is_cached:
        signals = _contrarian_cache.get("signals", [])
        # Filter signals by detection date within requested window
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        signals = [s for s in signals if any(
            (e.get("detected_at") or "") >= cutoff
            for e in (s.get("coordination_events") or [])
        ) or not s.get("coordination_events")]
        return {
            "generated_at": datetime.fromtimestamp(
                _contrarian_cache_at, tz=timezone.utc
            ).isoformat(),
            "cached": True,
            "signals": signals,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cached": False,
        "signals": [],
    }


# ---------------------------------------------------------------------------
# Phase 3 Batch 3 — AI Narrative Deep Analysis
# ---------------------------------------------------------------------------

@app.post("/api/narratives/{narrative_id}/analyze")
@limiter.limit("5/minute")
def analyze_narrative(request: Request, narrative_id: str = FPath(..., max_length=50), force: bool = False, user: dict = Depends(get_optional_user)):
    """AI deep analysis of a narrative via Haiku. Cached for 6 hours."""
    repo = get_repo()
    if repo is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    n = repo.get_narrative(narrative_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Narrative not found")

    # --- Cache check ---
    cached_analysis = n.get("deep_analysis")
    cached_at = n.get("deep_analysis_at")

    if cached_at and cached_analysis and not force:
        try:
            cached_time = datetime.fromisoformat(cached_at.replace("Z", "+00:00"))
            age_hours = (datetime.now(timezone.utc) - cached_time).total_seconds() / 3600
            if age_hours < 6:
                should_invalidate = False

                # Check stage change since last analysis
                try:
                    raw_muts = repo.get_mutations_for_narrative(narrative_id, limit=50)
                    for m in raw_muts:
                        if m.get("mutation_type") == "stage_change" and (m.get("detected_at") or "") > cached_at:
                            should_invalidate = True
                            break
                except Exception:
                    pass

                # Check burst event
                if float(n.get("burst_ratio") or 0) >= 3.0:
                    should_invalidate = True

                # Check new coordination events
                if not should_invalidate:
                    try:
                        events = repo.get_adversarial_events(narrative_id=narrative_id, limit=5)
                        for ev in events:
                            if (ev.get("detected_at") or "") > cached_at:
                                should_invalidate = True
                                break
                    except Exception:
                        pass

                if not should_invalidate:
                    return {**json.loads(cached_analysis), "analyzed_at": cached_at, "cached": True}
        except Exception:
            pass

    # --- Context bundle ---
    metadata = {
        "name": n.get("name") or "",
        "stage": n.get("stage") or "Emerging",
        "ns_score": round(float(n.get("ns_score") or 0), 4),
        "velocity": round(float(n.get("velocity_windowed") or 0), 4),
        "entropy": round(float(n.get("entropy") or 0), 4),
        "cohesion": round(float(n.get("cohesion") or 0), 4),
        "polarization": round(float(n.get("polarization") or 0), 4),
        "burst_ratio": round(float(n.get("burst_ratio") or 0), 2),
        "topic_tags": _safe_json_list(n.get("topic_tags")),
    }

    evidence = []
    try:
        evidence = repo.get_document_evidence(narrative_id)
    except Exception:
        pass
    doc_summaries = [
        {"headline": (d.get("excerpt") or "")[:200], "source": d.get("source_domain") or "", "date": d.get("published_at") or ""}
        for d in evidence[:20]
    ]

    linked_raw = n.get("linked_assets") or "[]"
    linked_assets = json.loads(linked_raw) if isinstance(linked_raw, str) else linked_raw
    asset_tickers = []
    if isinstance(linked_assets, list):
        for a in linked_assets:
            if isinstance(a, dict):
                t = a.get("ticker", "")
                if t and not t.startswith("TOPIC:"):
                    asset_tickers.append(t)
            elif isinstance(a, str):
                asset_tickers.append(a)

    mutations = []
    try:
        mutations = repo.get_mutations_for_narrative(narrative_id, limit=20)
    except Exception:
        pass
    mutation_lines = [
        f"- {m.get('mutation_type', 'change')}: {m.get('previous_value', '?')} -> {m.get('new_value', '?')} ({m.get('detected_at', '')})"
        for m in mutations[:5]
    ]

    coord_events = []
    try:
        coord_events = repo.get_adversarial_events(narrative_id=narrative_id, limit=10)
    except Exception:
        pass

    # --- Build prompt ---
    doc_lines = "\n".join(
        f"- [{d['source']}] {d['headline']}" for d in doc_summaries[:10]
    )
    mut_block = "\n".join(mutation_lines) if mutation_lines else "None"
    tags_str = ", ".join(metadata["topic_tags"]) if metadata["topic_tags"] else "None tagged"
    assets_str = ", ".join(asset_tickers) if asset_tickers else "None mapped"

    prompt = (
        "Analyze this financial narrative and return a JSON object with these exact keys:\n"
        '- "thesis": A 2-3 sentence summary of what this narrative means for the market\n'
        '- "key_drivers": Array of 3-5 strings describing key drivers\n'
        '- "asset_impact": Array of objects with "asset" (ticker) and "impact" (one sentence) keys\n'
        '- "risk_factors": Array of 2-4 strings describing risks to this thesis\n'
        '- "historical_comparison": One sentence comparing to a similar past pattern, or null\n\n'
        f"NARRATIVE: {metadata['name']}\n"
        f"STAGE: {metadata['stage']}\n"
        f"METRICS: NS Score {metadata['ns_score']}, Velocity {metadata['velocity']}, "
        f"Entropy {metadata['entropy']}, Cohesion {metadata['cohesion']}, "
        f"Polarization {metadata['polarization']}, Burst Ratio {metadata['burst_ratio']}\n"
        f"TOPICS: {tags_str}\n"
        f"LINKED ASSETS: {assets_str}\n"
        f"RECENT DOCUMENTS ({len(doc_summaries)}):\n{doc_lines}\n"
        f"MUTATIONS ({len(mutations)}):\n{mut_block}\n"
        f"COORDINATION FLAGS: {len(coord_events)} event(s)\n\n"
        "Return ONLY valid JSON, no markdown fences."
    )

    # --- LLM call ---
    fallback_json = '{"thesis":"Analysis unavailable","key_drivers":[],"asset_impact":[],"risk_factors":[],"historical_comparison":null}'
    try:
        from settings import Settings
        from llm_client import LlmClient, BudgetExceededError
        settings = Settings()
        llm = LlmClient(settings, repo)
        result_text = llm.call_haiku("deep_analysis", narrative_id, prompt, max_tokens=1024)
    except BudgetExceededError as exc:
        raise HTTPException(status_code=429, detail=str(exc))
    except Exception:
        result_text = fallback_json

    # --- Parse result ---
    analyzed_at = datetime.now(timezone.utc).isoformat()
    try:
        # Strip markdown fences if present
        import re as _re
        cleaned = _re.sub(r'^```(?:json)?\s*\n?', '', result_text.strip())
        cleaned = _re.sub(r'\n?```\s*$', '', cleaned).strip()
        result = json.loads(cleaned)
        if not isinstance(result, dict):
            raise ValueError("Expected JSON object")
    except (json.JSONDecodeError, TypeError, ValueError):
        result = {
            "thesis": result_text if result_text else "Analysis unavailable",
            "key_drivers": [],
            "asset_impact": [],
            "risk_factors": [],
            "historical_comparison": None,
        }

    # --- Cache ---
    try:
        repo.update_narrative(narrative_id, {
            "deep_analysis": json.dumps(result),
            "deep_analysis_at": analyzed_at,
        })
    except Exception:
        pass

    return {**result, "analyzed_at": analyzed_at, "cached": False}


# ---------------------------------------------------------------------------
# Part C — Social Sentiment System endpoints
# ---------------------------------------------------------------------------

@app.get("/api/sentiment/market")
async def get_market_sentiment(user: dict = Depends(get_current_user)):
    """Market-wide sentiment gauge across all tracked securities."""
    tickers = [s["symbol"] for s in TRACKED_SECURITIES]
    if _sentiment_aggregator is None:
        return {
            "market_score": 0.0,
            "bullish_pct": 0.0,
            "bearish_pct": 0.0,
            "neutral_pct": 100.0,
            "top_bullish": [],
            "top_bearish": [],
            "spikes": [],
        }
    try:
        return _sentiment_aggregator.compute_market_sentiment(tickers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentiment/{ticker}/history")
async def get_sentiment_history(
    ticker: str = FPath(..., max_length=20, pattern=r"^[A-Z0-9.\-]{1,20}$"),
    hours: int = Query(default=168, ge=1, le=8760),
    user: dict = Depends(get_current_user),
):
    """Sentiment timeseries for a ticker (default 7 days)."""
    repo = get_repo()
    try:
        rows = repo.get_sentiment_timeseries(ticker, hours=hours)
        return {"ticker": ticker, "hours": hours, "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentiment/{ticker}")
async def get_ticker_sentiment(
    ticker: str = FPath(..., max_length=20, pattern=r"^[A-Z0-9.\-]{1,20}$"),
    user: dict = Depends(get_current_user),
):
    """Per-ticker composite sentiment with source breakdown."""
    repo = get_repo()

    if _sentiment_aggregator is not None:
        try:
            result = _sentiment_aggregator.compute_ticker_sentiment(ticker)
            try:
                repo.insert_sentiment_record(ticker, result)
            except Exception:
                pass
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Fallback: return persisted latest if aggregator not yet ready
    try:
        latest = repo.get_latest_sentiment(ticker)
        if latest:
            return latest
    except Exception:
        pass

    return {
        "ticker": ticker,
        "composite_score": 0.0,
        "news_component": 0.0,
        "social_component": 0.0,
        "momentum_component": 0.0,
        "message_volume_24h": 0,
        "sources": {},
        "spike_detected": False,
        "computed_at": None,
    }


@app.get("/api/social/trending")
async def get_trending_tickers(
    hours: int = Query(default=24, ge=1, le=168),
    limit: int = Query(default=10, ge=1, le=50),
    user: dict = Depends(get_current_user),
):
    """Top tickers by social mention volume in the last N hours."""
    repo = get_repo()
    try:
        rows = repo.get_trending_tickers(hours=hours, limit=limit)
        return {"hours": hours, "tickers": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/social/{ticker}")
async def get_social_detail(
    ticker: str = FPath(..., max_length=20, pattern=r"^[A-Z0-9.\-]{1,20}$"),
    user: dict = Depends(get_current_user),
):
    """Social sentiment detail for a ticker with source breakdown."""
    repo = get_repo()

    stocktwits_data = _stocktwits_adapter.get_sentiment(ticker)
    if stocktwits_data:
        try:
            repo.insert_social_mention(ticker, "stocktwits", {
                "mention_count": stocktwits_data.get("total_messages", 0),
                "bullish_count": stocktwits_data.get("bullish_count", 0),
                "bearish_count": stocktwits_data.get("bearish_count", 0),
            })
        except Exception:
            pass

    signal_component: dict | None = None
    try:
        all_signals = repo.get_all_narrative_signals()
        ticker_sigs = []
        for sig in all_signals:
            nar = repo.get_narrative(sig["narrative_id"])
            if not nar:
                continue
            la_raw = nar.get("linked_assets", "[]")
            try:
                la = json.loads(la_raw) if isinstance(la_raw, str) else (la_raw or [])
            except Exception:
                continue
            if any(a.get("ticker") == ticker for a in la if isinstance(a, dict)):
                ticker_sigs.append({
                    "narrative_id": sig["narrative_id"],
                    "name": nar.get("name"),
                    "direction": sig.get("direction"),
                    "confidence": sig.get("confidence"),
                })
        signal_component = {"count": len(ticker_sigs), "signals": ticker_sigs[:10]}
    except Exception:
        pass

    return {
        "ticker": ticker,
        "stocktwits": stocktwits_data,
        "narrative_signals": signal_component,
    }


# ---------------------------------------------------------------------------
# Dashboard layout endpoints (Phase 7)
# ---------------------------------------------------------------------------

_DEFAULT_DASHBOARD_LAYOUT = {
    "widgets": [
        {"id": "narrative_radar", "type": "narrative_radar", "title": "Narrative Radar"},
        {"id": "signal_leaderboard", "type": "signal_leaderboard", "title": "Signal Leaderboard"},
        {"id": "top_movers", "type": "top_movers", "title": "Top Movers"},
        {"id": "sentiment_meter", "type": "sentiment_meter", "title": "Sentiment Meter"},
        {"id": "alert_feed", "type": "alert_feed", "title": "Alert Feed"},
    ],
    "grid": {
        "lg": [
            {"i": "narrative_radar", "x": 0, "y": 0, "w": 8, "h": 4},
            {"i": "signal_leaderboard", "x": 8, "y": 0, "w": 4, "h": 4},
            {"i": "top_movers", "x": 0, "y": 4, "w": 4, "h": 3},
            {"i": "sentiment_meter", "x": 4, "y": 4, "w": 4, "h": 3},
            {"i": "alert_feed", "x": 8, "y": 4, "w": 4, "h": 3},
        ]
    },
}


@app.get("/api/dashboard/layout")
async def get_dashboard_layout(user: dict = Depends(get_current_user)):
    """Return saved dashboard layout or default for new user."""
    repo = get_repo()
    saved = repo.get_dashboard_layout(user["user_id"])
    if saved is not None:
        return saved
    return _DEFAULT_DASHBOARD_LAYOUT


@app.put("/api/dashboard/layout")
async def save_dashboard_layout(layout: dict, user: dict = Depends(get_current_user)):
    """Save user's dashboard layout."""
    repo = get_repo()
    repo.save_dashboard_layout(user["user_id"], layout)
    return {"status": "ok"}
