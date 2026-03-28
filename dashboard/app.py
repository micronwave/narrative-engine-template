"""
Narrative Engine Dashboard — Flask server.
Run from project root: python dashboard/app.py
"""
import json
import os
import pickle
import re
import secrets
import sys
import threading
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

# Allow importing from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, g, jsonify, render_template, abort, request
from werkzeug.exceptions import HTTPException

app = Flask(__name__)
app.template_folder = Path(__file__).parent / "templates"
app.static_folder = Path(__file__).parent / "static"
app.secret_key = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(32))
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024  # 2 MB upload limit

DB_PATH = str(Path(__file__).parent.parent / "data" / "narrative_engine.db")

TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,10}$")

# ── Startup migration (run once, not per-request) ────────────────────────────
_migration_done = False


def _ensure_migrated():
    global _migration_done
    if _migration_done:
        return
    if Path(DB_PATH).exists():
        try:
            from repository import SqliteRepository
            repo = SqliteRepository(DB_PATH)
            repo.migrate()
        except Exception:
            pass
    _migration_done = True


# ── CSRF protection for mutating endpoints ────────────────────────────────────

def csrf_protect(f):
    """Require X-Requested-With header to block cross-origin form/fetch attacks."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.headers.get("X-Requested-With") != "XMLHttpRequest":
            return jsonify({"error": "CSRF check failed"}), 403
        return f(*args, **kwargs)
    return decorated


# ── Safe pickle loader ────────────────────────────────────────────────────────

class _RestrictedUnpickler(pickle.Unpickler):
    """Only allow basic Python types to prevent arbitrary code execution."""
    _ALLOWED = {"builtins": {"dict", "list", "set", "tuple", "str", "int", "float", "bool"}}

    def find_class(self, module, name):
        allowed_names = self._ALLOWED.get(module)
        if allowed_names and name in allowed_names:
            return getattr(__import__(module), name)
        raise pickle.UnpicklingError(f"Forbidden class: {module}.{name}")


def safe_pickle_load(path):
    with open(path, "rb") as f:
        return _RestrictedUnpickler(f).load()


# ── Ticker validation ─────────────────────────────────────────────────────────

def validate_ticker(ticker: str) -> str:
    t = ticker.upper().strip()
    if not TICKER_RE.match(t):
        abort(400, description="Invalid ticker symbol")
    return t


def get_repo():
    """Get request-scoped repository instance. Returns None if DB doesn't exist."""
    if "repo" in g:
        return g.repo
    if not Path(DB_PATH).exists():
        g.repo = None
        return None
    try:
        from repository import SqliteRepository
        repo = SqliteRepository(DB_PATH)
        g.repo = repo
        return repo
    except Exception:
        g.repo = None
        return None


@app.teardown_appcontext
def _close_repo(exc):
    repo = g.pop("repo", None)
    if repo is not None and hasattr(repo, "close"):
        repo.close()


@app.errorhandler(413)
def _handle_too_large(e):
    return jsonify({"error": "Upload too large (max 2MB)"}), 413


@app.before_request
def _before_request():
    _ensure_migrated()


@app.after_request
def _set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self' https://fonts.googleapis.com 'unsafe-inline'; font-src 'self' https://fonts.gstatic.com"
    return response


_SETTINGS_SECRET_KEYS = {
    "ANTHROPIC_API_KEY", "FINNHUB_API_KEY", "COINGECKO_API_KEY",
    "TWELVE_DATA_API_KEY", "REDDIT_CLIENT_SECRET", "JWT_SECRET_KEY",
    "MARKETAUX_API_KEY", "NEWSDATA_API_KEY",
    "TWITTER_API_KEY", "TWITTER_API_SECRET",
    "TWITTER_ACCESS_TOKEN", "TWITTER_ACCESS_TOKEN_SECRET",
    "TYPEFULLY_API_KEY",
}


def get_settings_dict():
    """Get settings as a plain dict. Returns {} on failure. Strips secret keys."""
    try:
        from settings import settings
        return {
            k: v for k, v in settings.model_dump().items()
            if k not in _SETTINGS_SECRET_KEYS
        }
    except Exception:
        return {}


def age_display(created_at_str: str) -> str:
    """Convert ISO timestamp to human-readable age."""
    if not created_at_str:
        return "unknown"
    try:
        created = datetime.fromisoformat(created_at_str)
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - created
        days = delta.days
        if days == 0:
            hours = delta.seconds // 3600
            return f"{hours}h ago" if hours > 0 else "just now"
        return f"{days}d ago"
    except Exception:
        return "unknown"


def enrich_narrative(n: dict) -> dict:
    """Add computed display fields to a narrative dict."""
    n["age_display"] = age_display(n.get("created_at", ""))
    n["ns_score"] = float(n.get("ns_score") or 0.0)
    n["document_count"] = int(n.get("document_count") or 0)
    # Parse linked_assets if stored as JSON string
    la = n.get("linked_assets")
    if isinstance(la, str):
        try:
            n["linked_assets"] = json.loads(la)
        except Exception:
            n["linked_assets"] = []
    elif la is None:
        n["linked_assets"] = []
    return n


# ── Template routes ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    repo = get_repo()
    if repo is None:
        cfg = get_settings_dict()
        return render_template(
            "index.html",
            narratives=[],
            stats={},
            db_missing=True,
            sonnet_budget=cfg.get("SONNET_DAILY_TOKEN_BUDGET", 200000),
        )
    try:
        narratives = [enrich_narrative(n) for n in repo.get_all_active_narratives()]
        narratives.sort(key=lambda n: n["ns_score"], reverse=True)
        stats = repo.get_dashboard_stats()
    except Exception as e:
        narratives = []
        stats = {}
    cfg = get_settings_dict()
    return render_template(
        "index.html",
        narratives=narratives,
        stats=stats,
        db_missing=False,
        sonnet_budget=cfg.get("SONNET_DAILY_TOKEN_BUDGET", 200000),
    )


@app.route("/narrative/<narrative_id>")
def narrative_detail(narrative_id):
    repo = get_repo()
    if repo is None:
        abort(404)
    narrative = repo.get_narrative(narrative_id)
    if narrative is None:
        abort(404)
    narrative = enrich_narrative(narrative)
    evidence = repo.get_document_evidence(narrative_id)
    llm_calls = repo.get_llm_calls_for_narrative(narrative_id)
    adversarial = repo.get_adversarial_log_for_narrative(narrative_id)
    history = repo.get_centroid_history_dates(narrative_id)

    # Parse linked_assets for display
    linked_assets = narrative.get("linked_assets") or []

    # Domain breakdown from evidence
    domain_counts: dict = {}
    for e in evidence:
        d = e.get("source_domain") or "unknown"
        domain_counts[d] = domain_counts.get(d, 0) + 1
    domain_breakdown = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)

    return render_template(
        "narrative.html",
        narrative=narrative,
        evidence=evidence[:20],
        llm_calls=llm_calls,
        adversarial=adversarial,
        history=history,
        linked_assets=linked_assets,
        domain_breakdown=domain_breakdown,
    )


@app.route("/settings")
def settings_view():
    cfg = get_settings_dict()
    repo = get_repo()
    stats = repo.get_dashboard_stats() if repo else {}

    # Feed list from ingester
    try:
        from ingester import RssIngester
        feeds = RssIngester._DEFAULT_FEEDS
    except Exception:
        feeds = []

    # Asset library stats
    asset_lib_path = Path(__file__).parent.parent / "data" / "asset_library.pkl"
    asset_stats = {"exists": False, "count": 0}
    if asset_lib_path.exists():
        try:
            lib = safe_pickle_load(asset_lib_path)
            asset_stats = {
                "exists": True,
                "count": len(lib),
                "tickers": sum(1 for k in lib if not k.startswith("TOPIC:")),
                "topics": sum(1 for k in lib if k.startswith("TOPIC:")),
            }
        except Exception:
            pass

    return render_template("settings.html", cfg=cfg, feeds=feeds, asset_stats=asset_stats, stats=stats)


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/narratives")
def api_narratives():
    repo = get_repo()
    if repo is None:
        return jsonify([])
    try:
        narratives = [enrich_narrative(n) for n in repo.get_all_active_narratives()]
        narratives.sort(key=lambda n: n["ns_score"], reverse=True)
        return jsonify(narratives)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/narrative/<narrative_id>")
def api_narrative(narrative_id):
    repo = get_repo()
    if repo is None:
        return jsonify({"error": "database not found"}), 404
    narrative = repo.get_narrative(narrative_id)
    if narrative is None:
        return jsonify({"error": "not found"}), 404
    narrative = enrich_narrative(narrative)
    evidence = repo.get_document_evidence(narrative_id)
    llm_calls = repo.get_llm_calls_for_narrative(narrative_id)
    return jsonify({"narrative": narrative, "evidence": evidence, "llm_calls": llm_calls})


@app.route("/api/stats")
def api_stats():
    repo = get_repo()
    if repo is None:
        return jsonify({"db_missing": True})
    try:
        stats = repo.get_dashboard_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/activity")
def api_activity():
    repo = get_repo()
    if repo is None:
        return jsonify([])
    try:
        activity = repo.get_recent_pipeline_activity(limit=20)
        return jsonify(activity)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


_refresh_lock = threading.Lock()
_refresh_status: dict = {"running": False, "last_result": None, "last_run": None}


@app.route("/api/refresh", methods=["POST"])
@csrf_protect
def trigger_refresh():
    """Triggers quick refresh in background. Returns immediately."""
    if not _refresh_lock.acquire(blocking=False):
        return jsonify({"error": "Refresh already running"}), 409

    def _run_refresh():
        try:
            _refresh_status["running"] = True
            from quick_refresh import QuickRefresh
            from settings import settings as _settings
            from repository import SqliteRepository
            from vector_store import FaissVectorStore
            from embedding_model import MiniLMEmbedder
            from deduplicator import Deduplicator

            _repo = SqliteRepository(_settings.DB_PATH)
            _repo.migrate()
            _embedder = MiniLMEmbedder(_settings)
            _emb_dim = _embedder.dimension()
            _vs = FaissVectorStore(_settings.FAISS_INDEX_PATH)
            if not _vs.load():
                _vs.initialize(_emb_dim)
            _dedup = Deduplicator(
                threshold=_settings.LSH_THRESHOLD,
                num_perm=_settings.LSH_NUM_PERM,
                lsh_path=_settings.LSH_INDEX_PATH,
            )
            _dedup.load()

            refresher = QuickRefresh(_settings, _repo, _vs, _embedder, _dedup)
            result = refresher.run()
            _dedup.save()

            _refresh_status["last_result"] = result
            _refresh_status["last_run"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            _refresh_status["last_result"] = {"error": str(exc)}
        finally:
            _refresh_status["running"] = False
            _refresh_lock.release()

    thread = threading.Thread(target=_run_refresh, daemon=True)
    thread.start()
    return jsonify({"status": "started"})


@app.route("/api/refresh/status")
def get_refresh_status():
    """Returns current refresh status and last result."""
    return jsonify(_refresh_status)


@app.route("/api/mutations/today")
def get_mutations_today():
    """Returns today's mutation summary."""
    repo = get_repo()
    if repo is None:
        return jsonify({"error": "database not found"}), 404
    try:
        from mutations import MutationDetector
        from settings import settings as _settings
        from llm_client import LlmClient
        detector = MutationDetector(_settings, repo, LlmClient(_settings, repo))
        return jsonify(detector.generate_mutation_summary())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/mutations/narrative/<narrative_id>")
def get_narrative_mutations(narrative_id):
    """Returns mutations for specific narrative."""
    repo = get_repo()
    if repo is None:
        return jsonify({"error": "database not found"}), 404
    try:
        mutations = repo.get_mutations_for_narrative(narrative_id)
        return jsonify(mutations)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/narrative/<narrative_id>/timeline")
def get_narrative_timeline(narrative_id):
    """Returns 7-day story timeline."""
    repo = get_repo()
    if repo is None:
        return jsonify({"error": "database not found"}), 404
    try:
        from mutations import MutationDetector
        from settings import settings as _settings
        from llm_client import LlmClient
        detector = MutationDetector(_settings, repo, LlmClient(_settings, repo))
        return jsonify(detector.get_story_timeline(narrative_id, days=7))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/narrative/<narrative_id>/compare")
def compare_narrative(narrative_id):
    """Compare yesterday vs today."""
    repo = get_repo()
    if repo is None:
        return jsonify({"error": "database not found"}), 404
    try:
        from mutations import MutationDetector
        from settings import settings as _settings
        from llm_client import LlmClient
        from datetime import date, timedelta
        detector = MutationDetector(_settings, repo, LlmClient(_settings, repo))
        today = date.today().isoformat()
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        return jsonify(detector.compare_snapshots(narrative_id, yesterday, today))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stocks")
def api_stocks():
    """Returns all tracked stocks with current data, sorted by impact score."""
    repo = get_repo()
    if repo is None:
        return jsonify([])
    try:
        from stock_data import StockDataProvider
        from settings import settings as _settings

        # Load tickers from asset library pkl (same pattern as /settings page)
        asset_lib_path = Path(__file__).parent.parent / "data" / "asset_library.pkl"
        tickers: list[str] = []
        if asset_lib_path.exists():
            lib = safe_pickle_load(asset_lib_path)
            tickers = [k for k in lib if not k.startswith("TOPIC:")]

        if not tickers:
            return jsonify([])

        provider = StockDataProvider(repo)
        stocks = provider.get_quotes_batch(tickers[:50])  # cap at 50 for performance

        result = []
        for ticker, data in stocks.items():
            data["linked_narratives"] = len(repo.get_narratives_for_ticker(ticker))
            data["impact_score"] = repo.get_ticker_impact_score(ticker)
            result.append(data)

        result.sort(key=lambda x: x["impact_score"], reverse=True)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stock/<ticker>")
def api_stock(ticker):
    """Returns single stock with full data and linked narratives."""
    ticker = validate_ticker(ticker)
    repo = get_repo()
    if repo is None:
        return jsonify({"error": "database not found"}), 404
    try:
        from stock_data import StockDataProvider
        provider = StockDataProvider(repo)
        data = provider.get_quote(ticker)
        if not data:
            return jsonify({"error": "Stock not found or data unavailable"}), 404
        data["narratives"] = repo.get_narratives_for_ticker(ticker)
        data["impact_score"] = repo.get_ticker_impact_score(ticker)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stock/<ticker>/sparkline")
def api_stock_sparkline(ticker):
    """Returns sparkline price array for mini chart."""
    ticker = validate_ticker(ticker)
    repo = get_repo()
    if repo is None:
        return jsonify({"error": "database not found"}), 404
    try:
        from stock_data import StockDataProvider
        days = min(request.args.get("days", 7, type=int), 365)
        provider = StockDataProvider(repo)
        prices = provider.get_sparkline_data(ticker, days)
        return jsonify({"ticker": ticker, "days": days, "prices": prices})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stock/<ticker>/narratives")
def api_stock_narratives(ticker):
    """Returns narratives linked to this stock."""
    ticker = validate_ticker(ticker)
    repo = get_repo()
    if repo is None:
        return jsonify({"error": "database not found"}), 404
    try:
        return jsonify(repo.get_narratives_for_ticker(ticker))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Chat routes ───────────────────────────────────────────────────────────────

def _get_chat_manager():
    """Returns (ChatManager, repo) or (None, None) if DB unavailable."""
    repo = get_repo()
    if repo is None:
        return None, None
    try:
        from chat import ChatManager
        from llm_client import LlmClient
        from settings import settings as _settings
        llm = LlmClient(_settings, repo)
        return ChatManager(_settings, repo, llm), repo
    except Exception:
        return None, None


@app.route("/api/chat/session", methods=["POST"])
@csrf_protect
def create_chat_session():
    """Creates new chat session."""
    manager, _ = _get_chat_manager()
    if manager is None:
        return jsonify({"error": "database not found"}), 503
    data = request.json or {}
    try:
        session_id = manager.create_session(
            user_id=data.get("user_id", "local"),
            narrative_id=data.get("narrative_id"),
            ticker=data.get("ticker"),
        )
        return jsonify({"session_id": session_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat/sessions")
def list_chat_sessions():
    """Lists user's chat sessions."""
    manager, _ = _get_chat_manager()
    if manager is None:
        return jsonify([])
    try:
        user_id = request.args.get("user_id", "local")
        limit = request.args.get("limit", 20, type=int)
        return jsonify(manager.list_sessions(user_id, limit))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat/session/<session_id>")
def get_chat_session(session_id):
    """Gets session with messages."""
    manager, _ = _get_chat_manager()
    if manager is None:
        return jsonify({"error": "database not found"}), 503
    session = manager.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    return jsonify(session)


@app.route("/api/chat/session/<session_id>/message", methods=["POST"])
@csrf_protect
def send_chat_message(session_id):
    """Sends message and gets Haiku response."""
    manager, _ = _get_chat_manager()
    if manager is None:
        return jsonify({"error": "database not found"}), 503
    data = request.json
    if not data or "message" not in data:
        return jsonify({"error": "message required"}), 400
    msg = data["message"]
    if not isinstance(msg, str) or len(msg) > 10_000:
        return jsonify({"error": "message must be a string under 10000 chars"}), 400
    try:
        result = manager.send_message(session_id, msg)
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


ALLOWED_TEMPLATE_KWARGS = {"ticker", "narrative_id", "timeframe"}


@app.route("/api/chat/session/<session_id>/template", methods=["POST"])
@csrf_protect
def apply_chat_template(session_id):
    """Applies a named template as the next message."""
    manager, _ = _get_chat_manager()
    if manager is None:
        return jsonify({"error": "database not found"}), 503
    data = request.json
    if not data or "template" not in data:
        return jsonify({"error": "template required"}), 400
    try:
        kwargs = {k: v for k, v in data.items() if k in ALLOWED_TEMPLATE_KWARGS}
        result = manager.apply_template(session_id, data["template"], **kwargs)
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat/session/<session_id>", methods=["DELETE"])
@csrf_protect
def delete_chat_session(session_id):
    """Deletes session and all its messages."""
    _, repo = _get_chat_manager()
    if repo is None:
        return jsonify({"error": "database not found"}), 503
    try:
        repo.delete_chat_session(session_id)
        return jsonify({"status": "deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat/templates")
def get_chat_templates():
    """Returns available chat templates."""
    try:
        from chat import CHAT_TEMPLATES
        return jsonify(CHAT_TEMPLATES)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Portfolio routes ──────────────────────────────────────────────────────────

def _get_portfolio_manager():
    """Returns (PortfolioManager, portfolio_id) or (None, None)."""
    repo = get_repo()
    if repo is None:
        return None, None
    try:
        from portfolio import PortfolioManager
        from stock_data import StockDataProvider
        pm = PortfolioManager(repo, StockDataProvider(repo))
        pid = pm.get_or_create_portfolio()
        return pm, pid
    except Exception:
        return None, None


@app.route("/api/portfolio")
def get_portfolio():
    pm, pid = _get_portfolio_manager()
    if pm is None:
        return jsonify({"error": "database not found"}), 503
    try:
        holdings = pm.get_holdings(pid)
        return jsonify({"portfolio_id": pid, "holdings": holdings})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/portfolio/holding", methods=["POST"])
@csrf_protect
def add_holding():
    pm, pid = _get_portfolio_manager()
    if pm is None:
        return jsonify({"error": "database not found"}), 503
    data = request.json or {}
    if not data.get("ticker") or not data.get("shares"):
        return jsonify({"error": "ticker and shares required"}), 400
    try:
        holding_id = pm.add_holding(pid, data["ticker"], float(data["shares"]), data.get("cost_basis"))
        return jsonify({"holding_id": holding_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/portfolio/holding/<holding_id>", methods=["DELETE"])
@csrf_protect
def remove_holding(holding_id):
    pm, _ = _get_portfolio_manager()
    if pm is None:
        return jsonify({"error": "database not found"}), 503
    try:
        pm.remove_holding(holding_id)
        return jsonify({"status": "deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/portfolio/import", methods=["POST"])
@csrf_protect
def import_portfolio():
    pm, pid = _get_portfolio_manager()
    if pm is None:
        return jsonify({"error": "database not found"}), 503
    try:
        csv_content = request.data.decode("utf-8")
        result = pm.import_csv(pid, csv_content)
        return jsonify(result)
    except HTTPException:
        raise
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/portfolio/impact")
def get_portfolio_impact():
    pm, pid = _get_portfolio_manager()
    if pm is None:
        return jsonify({"error": "database not found"}), 503
    try:
        impact = pm.calculate_impact(pid)
        return jsonify(impact)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Watchlist routes ──────────────────────────────────────────────────────────

def _get_watchlist_manager():
    repo = get_repo()
    if repo is None:
        return None
    try:
        from watchlist import WatchlistManager
        return WatchlistManager(repo)
    except Exception:
        return None


@app.route("/api/watchlist", methods=["POST"])
@csrf_protect
def create_watchlist():
    wm = _get_watchlist_manager()
    if wm is None:
        return jsonify({"error": "database not found"}), 503
    data = request.json or {}
    try:
        watchlist_id = wm.create_watchlist(name=data.get("name", "My Watchlist"))
        return jsonify({"watchlist_id": watchlist_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/watchlists")
def list_watchlists():
    wm = _get_watchlist_manager()
    if wm is None:
        return jsonify([])
    try:
        return jsonify(wm.list_watchlists())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/watchlist/<watchlist_id>")
def get_watchlist(watchlist_id):
    wm = _get_watchlist_manager()
    if wm is None:
        return jsonify({"error": "database not found"}), 503
    watchlist = wm.get_watchlist_status(watchlist_id)
    if not watchlist:
        return jsonify({"error": "Not found"}), 404
    return jsonify(watchlist)


@app.route("/api/watchlist/<watchlist_id>/item", methods=["POST"])
@csrf_protect
def add_watchlist_item(watchlist_id):
    wm = _get_watchlist_manager()
    if wm is None:
        return jsonify({"error": "database not found"}), 503
    data = request.json or {}
    if not data.get("item_type") or not data.get("item_id"):
        return jsonify({"error": "item_type and item_id required"}), 400
    try:
        item_id = wm.add_item(watchlist_id, data["item_type"], data["item_id"])
        return jsonify({"item_id": item_id})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/watchlist/<watchlist_id>/item/<item_id>", methods=["DELETE"])
@csrf_protect
def remove_watchlist_item(watchlist_id, item_id):
    wm = _get_watchlist_manager()
    if wm is None:
        return jsonify({"error": "database not found"}), 503
    try:
        wm.remove_item(item_id)
        return jsonify({"status": "deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Notification routes ────────────────────────────────────────────────────────

def _get_notification_manager():
    repo = get_repo()
    if repo is None:
        return None
    try:
        from notifications import NotificationManager
        return NotificationManager(repo)
    except Exception:
        return None


@app.route("/api/notifications")
def get_notifications():
    nm = _get_notification_manager()
    if nm is None:
        return jsonify([])
    try:
        unread_only = request.args.get("unread", "false").lower() == "true"
        return jsonify(nm.get_notifications("local", unread_only=unread_only))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/notifications/rules")
def list_notification_rules():
    nm = _get_notification_manager()
    if nm is None:
        return jsonify([])
    try:
        return jsonify(nm.list_rules())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/notifications/rule", methods=["POST"])
@csrf_protect
def create_notification_rule():
    nm = _get_notification_manager()
    if nm is None:
        return jsonify({"error": "database not found"}), 503
    data = request.json or {}
    try:
        rule_id = nm.create_rule(
            user_id="local",
            rule_type=data["rule_type"],
            target_type=data["target_type"],
            target_id=data.get("target_id"),
            threshold=data.get("threshold"),
        )
        return jsonify({"rule_id": rule_id})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/notifications/rule/<rule_id>", methods=["DELETE"])
@csrf_protect
def delete_notification_rule(rule_id):
    nm = _get_notification_manager()
    if nm is None:
        return jsonify({"error": "database not found"}), 503
    try:
        nm.delete_rule(rule_id)
        return jsonify({"status": "deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/notifications/<notification_id>/read", methods=["POST"])
@csrf_protect
def mark_notification_read(notification_id):
    nm = _get_notification_manager()
    if nm is None:
        return jsonify({"error": "database not found"}), 503
    try:
        nm.mark_read(notification_id)
        return jsonify({"status": "read"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/notifications/read-all", methods=["POST"])
@csrf_protect
def mark_all_notifications_read():
    nm = _get_notification_manager()
    if nm is None:
        return jsonify({"error": "database not found"}), 503
    try:
        nm.mark_all_read("local")
        return jsonify({"status": "all read"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/notifications/rule-types")
def get_notification_rule_types():
    try:
        from notifications import RULE_TYPES
        return jsonify(RULE_TYPES)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Export routes ──────────────────────────────────────────────────────────────

def _get_export_manager():
    repo = get_repo()
    if repo is None:
        return None
    try:
        from export import ExportManager
        from llm_client import LlmClient
        from settings import settings as _settings
        llm = LlmClient(_settings, repo)
        return ExportManager(repo, llm)
    except Exception:
        return None


@app.route("/api/export/json")
def export_json():
    em = _get_export_manager()
    if em is None:
        return jsonify({"error": "database not found"}), 503
    try:
        target_date = request.args.get("date")
        content = em.export_narratives_json(target_date)
        return app.response_class(content, mimetype="application/json")
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/export/csv")
def export_csv():
    em = _get_export_manager()
    if em is None:
        return jsonify({"error": "database not found"}), 503
    try:
        target_date = request.args.get("date")
        content = em.export_narratives_csv(target_date)
        return app.response_class(
            content,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=narratives.csv"},
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/share/<narrative_id>/<platform>")
def get_share_text(narrative_id, platform):
    em = _get_export_manager()
    if em is None:
        return jsonify({"error": "database not found"}), 503
    try:
        text = em.generate_share_text(narrative_id, platform)
        return jsonify({"platform": platform, "text": text})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Health check ───────────────────────────────────────────────────────────────

@app.route("/api/health")
def health_check():
    repo = get_repo()
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database": "connected" if repo else "disconnected",
    })


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=debug, host="127.0.0.1", port=5000)
