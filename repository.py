import datetime
import json
import logging
import re
import sqlite3
from abc import ABC, abstractmethod
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Repository(ABC):
    """
    Abstract interface for all database operations.
    MVP implementation: SqliteRepository.

    # TODO SCALE: swap SqliteRepository for PostgresRepository (psycopg2) on AWS RDS — interface is identical
    """

    @abstractmethod
    def migrate(self) -> None:
        """Create all tables if they do not exist."""
        ...

    # --- Narrative Operations ---

    @abstractmethod
    def get_narrative(self, narrative_id: str) -> dict | None:
        """Get a single narrative by ID."""
        ...

    @abstractmethod
    def get_all_active_narratives(self, *, limit: int = 0, offset: int = 0,
                                  stage: str | None = None, topic: str | None = None) -> list[dict]:
        """Get all non-suppressed narratives. Optional stage/topic filters for API pagination."""
        ...

    @abstractmethod
    def count_active_narratives(self, *, stage: str | None = None, topic: str | None = None) -> int:
        """Count non-suppressed narratives, with optional filters."""
        ...

    @abstractmethod
    def insert_narrative(self, narrative: dict) -> None:
        """Insert a new narrative record."""
        ...

    @abstractmethod
    def update_narrative(self, narrative_id: str, updates: dict) -> None:
        """Update fields on an existing narrative."""
        ...

    @abstractmethod
    def update_narrative_tags(self, narrative_id: str, tags: list) -> None: ...

    @abstractmethod
    def get_narrative_count(self) -> int:
        """Get total count of all narratives (for consistency checks)."""
        ...

    @abstractmethod
    def get_narratives_by_stage(self, stage: str) -> list[dict]:
        """Get all narratives with a specific stage."""
        ...

    @abstractmethod
    def get_narratives_needing_decay(self, current_date: str) -> list[str]:
        """Get narrative_ids that received zero assignments on current_date."""
        ...

    @abstractmethod
    def record_narrative_assignment(self, narrative_id: str, date: str) -> None:
        """Record that a narrative received a document assignment on this date."""
        ...

    # --- Candidate Buffer Operations ---

    @abstractmethod
    def get_candidate_buffer(self, status: str = "pending", limit: int | None = None) -> list[dict]:
        """Get candidates with the given status, oldest-first, optionally bounded."""
        ...

    @abstractmethod
    def insert_candidate(self, candidate: dict) -> None:
        """Insert a document into the candidate buffer."""
        ...

    @abstractmethod
    def update_candidate_status(
        self, doc_id: str, status: str, narrative_id_assigned: str = None
    ) -> None:
        """Update the status of a candidate document."""
        ...

    @abstractmethod
    def get_candidate_buffer_count(self, status: str = "pending") -> int:
        """Count candidates with the given status."""
        ...

    @abstractmethod
    def get_corpus_domain_count(self) -> int:
        """Count distinct source domains across all buffered candidates."""
        ...

    @abstractmethod
    def clear_candidate_buffer(self, status: str = "clustered") -> None:
        """Delete all candidates with the given status."""
        ...

    @abstractmethod
    def delete_old_candidate_buffer(self, days: int) -> int:
        """Delete clustered entries older than N days. Return count deleted."""
        ...

    # --- Centroid History Operations ---

    @abstractmethod
    def insert_centroid_history(
        self, narrative_id: str, date: str, centroid_blob: bytes
    ) -> None:
        """Store a centroid snapshot."""
        ...

    @abstractmethod
    def get_centroid_history(self, narrative_id: str, days: int, *, limit: int = 0, offset: int = 0) -> list[dict]:
        """Get centroid history for a narrative, one entry per date, most recent first."""
        ...

    @abstractmethod
    def get_latest_centroid(self, narrative_id: str) -> bytes | None:
        """Get the most recent centroid blob for a narrative."""
        ...

    @abstractmethod
    def get_latest_centroids_batch(self, narrative_ids: list[str]) -> dict[str, bytes]:
        """Get most recent centroid blobs for multiple narratives in one query."""
        ...

    @abstractmethod
    def count_suppressed_with_documents(self) -> int:
        """Count suppressed narratives that still have document assignments."""
        ...

    # --- LLM Audit Operations ---

    @abstractmethod
    def log_llm_call(self, call_record: dict) -> None:
        """Log an LLM call to the audit log."""
        ...

    @abstractmethod
    def get_sonnet_calls_last_24h(self, narrative_id: str) -> list[dict]:
        """Get Sonnet calls for a narrative in the last 24 hours."""
        ...

    @abstractmethod
    def get_sonnet_daily_spend(self, date: str) -> dict | None:
        """Get the daily spend record for a date."""
        ...

    @abstractmethod
    def update_sonnet_daily_spend(self, date: str, tokens: int, calls: int) -> None:
        """Update or insert the daily spend record."""
        ...

    @abstractmethod
    def get_daily_llm_spend(self) -> float:
        """Get total LLM spend (all models) for today in USD."""
        ...

    # --- Adversarial Operations ---

    @abstractmethod
    def log_adversarial_event(self, event: dict) -> None:
        """Log a coordination detection event."""
        ...

    @abstractmethod
    def get_coordination_flags_rolling_window(
        self, narrative_id: str, days: int
    ) -> int:
        """Count coordination flags for a narrative in the last N days."""
        ...

    # --- Robots Cache Operations ---

    @abstractmethod
    def get_robots_cache(self, domain: str) -> dict | None:
        """Get cached robots.txt rules for a domain."""
        ...

    @abstractmethod
    def set_robots_cache(
        self, domain: str, rules_text: str, fetched_at: str
    ) -> None:
        """Cache robots.txt rules for a domain."""
        ...

    # --- Failed Job Operations ---

    @abstractmethod
    def insert_failed_job(self, job: dict) -> None:
        """Log a failed ingestion job."""
        ...

    @abstractmethod
    def get_retryable_failed_jobs(self, current_time: str) -> list[dict]:
        """Get jobs where next_retry_at <= current_time and retry_count < 3."""
        ...

    @abstractmethod
    def update_failed_job_retry(
        self, job_id: str, retry_count: int, next_retry_at: str
    ) -> None:
        """Update retry metadata for a failed job."""
        ...

    @abstractmethod
    def delete_failed_job(self, job_id: str) -> None:
        """Remove a failed job record (after successful retry)."""
        ...

    # --- Pipeline Run Log Operations ---

    @abstractmethod
    def log_pipeline_run(self, run_record: dict) -> None:
        """Log a pipeline step execution."""
        ...

    # --- Quick Refresh Operations ---

    @abstractmethod
    def assign_doc_to_narrative(self, doc_id: str, narrative_id: str) -> None:
        """Record that a document has been assigned to a narrative."""
        ...

    @abstractmethod
    def get_narrative_doc_count(self, narrative_id: str) -> int:
        """Return count of documents assigned to narrative (via document_evidence)."""
        ...

    @abstractmethod
    def update_narrative_doc_count(self, narrative_id: str, count: int) -> None:
        """Update document_count field on narrative."""
        ...

    # --- Snapshot Operations ---

    @abstractmethod
    def save_snapshot(self, snapshot: dict) -> None:
        """Insert or update a narrative_snapshot row."""
        ...

    @abstractmethod
    def get_snapshot(self, narrative_id: str, snapshot_date: str) -> dict | None:
        """Get snapshot for narrative on specific date."""
        ...

    @abstractmethod
    def get_snapshots_range(self, narrative_id: str, start_date: str, end_date: str) -> list[dict]:
        """Get all snapshots in date range, ordered by snapshot_date DESC."""
        ...

    @abstractmethod
    def get_baseline_doc_rate(self, narrative_id: str, lookback_days: int = 7) -> float:
        """Returns average doc_count from recent snapshots for baseline calculation."""
        ...

    @abstractmethod
    def get_snapshot_history(self, narrative_id: str, days: int = 30) -> list:
        """Returns recent daily snapshots for a narrative, ordered by date descending."""
        ...

    # --- Mutation Operations ---

    @abstractmethod
    def save_mutation(self, mutation: dict) -> None:
        """Insert a mutation_event row."""
        ...

    @abstractmethod
    def get_mutations_today(self) -> list[dict]:
        """Get all mutations detected today."""
        ...

    @abstractmethod
    def get_mutations_for_narrative(self, narrative_id: str, limit: int = 10) -> list[dict]:
        """Get recent mutations for a narrative."""
        ...

    @abstractmethod
    def get_changelog_for_narrative(self, narrative_id: str, days: int = 30, *, limit: int = 0, offset: int = 0) -> list[dict]:
        """Get mutations for a narrative within the last N days, for changelog."""
        ...

    # --- Document Evidence Operations ---

    @abstractmethod
    def insert_document_evidence(self, evidence: dict) -> None:
        """Store supporting evidence for a narrative."""
        ...

    @abstractmethod
    def get_document_evidence(self, narrative_id: str, *, limit: int = 0, offset: int = 0) -> list[dict]:
        """Get all evidence documents for a narrative."""
        ...

    @abstractmethod
    def get_document_evidence_by_ids(self, doc_ids: list[str]) -> list[dict]:
        """Get evidence documents by a list of doc_ids."""
        ...

    # --- Stock Cache Operations ---

    @abstractmethod
    def get_stock_cache(self, ticker: str) -> dict | None:
        """Get cached stock data for a ticker."""
        ...

    @abstractmethod
    def save_stock_cache(self, data: dict) -> None:
        """Insert or replace cached stock data."""
        ...

    @abstractmethod
    def get_narratives_for_ticker(self, ticker: str) -> list[dict]:
        """Return all active narratives linked to this ticker."""
        ...

    @abstractmethod
    def get_ticker_impact_score(self, ticker: str) -> float:
        """Sum of ns_scores for all narratives linked to this ticker."""
        ...

    # --- API Usage Tracking ---

    @abstractmethod
    def get_api_usage(self, api_name: str, date: str) -> dict | None:
        """Get API usage record for a given api_name and date."""
        ...

    @abstractmethod
    def increment_api_usage(self, api_name: str, date: str, limit: int) -> None:
        """Upsert API usage count — increments requests_used by 1."""
        ...

    # --- Portfolio Operations ---

    @abstractmethod
    def get_portfolio_by_user(self, user_id: str) -> dict | None: ...

    @abstractmethod
    def create_portfolio(self, portfolio: dict) -> None: ...

    @abstractmethod
    def add_portfolio_holding(self, holding: dict) -> None: ...

    @abstractmethod
    def delete_portfolio_holding(self, holding_id: str) -> None: ...

    @abstractmethod
    def get_portfolio_holdings(self, portfolio_id: str) -> list[dict]: ...

    @abstractmethod
    def update_portfolio_timestamp(self, portfolio_id: str) -> None: ...

    # --- Watchlist Operations ---

    @abstractmethod
    def create_watchlist(self, watchlist: dict) -> None: ...

    @abstractmethod
    def get_watchlist(self, watchlist_id: str) -> dict | None: ...

    @abstractmethod
    def list_watchlists(self, user_id: str) -> list[dict]: ...

    @abstractmethod
    def add_watchlist_item(self, item: dict) -> None: ...

    @abstractmethod
    def delete_watchlist_item(self, item_id: str) -> None: ...

    @abstractmethod
    def get_watchlist_items(self, watchlist_id: str) -> list[dict]: ...

    # --- Notification Operations ---

    @abstractmethod
    def create_notification_rule(self, rule: dict) -> None: ...

    @abstractmethod
    def get_enabled_notification_rules(self) -> list[dict]: ...

    @abstractmethod
    def list_notification_rules(self, user_id: str) -> list[dict]: ...

    @abstractmethod
    def update_notification_rule_enabled(self, rule_id: str, enabled: bool) -> None: ...

    @abstractmethod
    def delete_notification_rule(self, rule_id: str) -> None: ...

    @abstractmethod
    def create_notification(self, notification: dict) -> None: ...

    @abstractmethod
    def get_notifications(self, user_id: str, unread_only: bool = False) -> list[dict]: ...

    @abstractmethod
    def mark_notification_read(self, notification_id: str, user_id: str | None = None) -> None: ...

    @abstractmethod
    def mark_all_notifications_read(self, user_id: str) -> None: ...

    @abstractmethod
    def has_notification_today(self, rule_id: str) -> bool:
        """Check if a notification was already created today for this rule."""
        ...

    @abstractmethod
    def get_notifications_since(self, user_id: str, since: "datetime") -> list[dict]:
        """Return notifications created after `since` for SSE delivery."""
        ...

    @abstractmethod
    def get_dashboard_layout(self, user_id: str) -> dict | None:
        """Return saved dashboard layout for user, or None."""
        ...

    @abstractmethod
    def save_dashboard_layout(self, user_id: str, layout: dict) -> None:
        """Upsert dashboard layout for user."""
        ...

    # --- Support Operations ---

    @abstractmethod
    def get_narratives_created_on_date(self, date: str) -> list[dict]:
        """Return narratives created on a specific date (YYYY-MM-DD)."""
        ...

    @abstractmethod
    def get_mutations_today_for_narrative(self, narrative_id: str) -> list[dict]:
        """Return mutations detected today for a specific narrative."""
        ...

    @abstractmethod
    def get_narratives_by_date(self, date: str) -> list[dict]:
        """Return all active narratives for export (date param reserved for future filtering)."""
        ...

    # --- Analytics Operations ---

    @abstractmethod
    def get_bulk_narrative_snapshots(self, cutoff_date: str) -> list[dict]:
        """Bulk fetch snapshots for active + recently-dormant narratives since cutoff_date."""
        ...

    @abstractmethod
    def get_velocity_snapshots_bulk(self, cutoff_date: str) -> list[dict]:
        """Get velocity snapshots for all active narratives since cutoff_date."""
        ...

    @abstractmethod
    def get_document_overlaps(self) -> list[dict]:
        """Bulk query for narrative pairs sharing documents."""
        ...

    @abstractmethod
    def get_doc_counts_per_narrative(self) -> dict[str, int]:
        """Count distinct documents per narrative from assignments table."""
        ...

    @abstractmethod
    def get_stage_change_mutations(self, days: int = 30) -> list[dict]:
        """Get all stage_change mutations from the last N days."""
        ...

    @abstractmethod
    def get_first_snapshot_dates(self) -> dict[str, str]:
        """Return {narrative_id: earliest_snapshot_date} for all narratives."""
        ...

    # --- Feed Metadata Operations ---

    @abstractmethod
    def get_feed_metadata(self, feed_url: str) -> dict | None:
        """Return stored ETag/Last-Modified for a feed URL, or None."""
        ...

    @abstractmethod
    def upsert_feed_metadata(self, feed_url: str, etag: str | None, last_modified: str | None, new_doc_count: int) -> None:
        """Insert or update feed metadata. Tracks consecutive empty cycles."""
        ...

    # --- Price Tick / Candle Operations ---

    @abstractmethod
    def insert_ticks_batch(self, ticks: list[dict]) -> int:
        """Batch insert price ticks. Returns count of rows inserted."""
        ...

    @abstractmethod
    def get_recent_ticks(self, symbol: str, limit: int = 100) -> list[dict]:
        """Return the most recent ticks for a symbol, newest first."""
        ...

    @abstractmethod
    def aggregate_candles_1m(self, before_cutoff: str) -> int:
        """Aggregate raw ticks older than cutoff into 1-minute OHLCV candles. Returns count created."""
        ...

    @abstractmethod
    def prune_old_ticks(self, cutoff: str) -> int:
        """Delete ticks older than cutoff. Returns count deleted."""
        ...

    @abstractmethod
    def get_candles_1m(self, symbol: str, start: str, end: str) -> list[dict]:
        """Return 1-minute candles for a symbol within a time range."""
        ...

    # --- User Operations ---

    @abstractmethod
    def get_user_by_id(self, user_id: str) -> dict | None:
        """Get user by primary key."""
        ...

    @abstractmethod
    def get_user_by_email(self, email: str) -> dict | None:
        """Get user by email (for login lookup)."""
        ...

    @abstractmethod
    def create_user(self, user: dict) -> None:
        """Insert a new user. Expects keys: id, email, password_hash, created_at."""
        ...

    # --- Tweet Log Operations ---

    @abstractmethod
    def insert_tweet_log(self, data: dict) -> None:
        """Insert a tweet log entry."""
        ...

    @abstractmethod
    def get_last_tweet_for_narrative(self, narrative_id: str) -> dict | None:
        """Get most recent tweet log entry for a narrative."""
        ...

    @abstractmethod
    def get_original_tweet_for_narrative(self, narrative_id: str) -> dict | None:
        """Get the first successful tweet for a narrative (for quote-tweeting)."""
        ...

    @abstractmethod
    def get_tweet_count_today(self) -> int:
        """Count tweets posted today (for budget tracking)."""
        ...

    @abstractmethod
    def get_tweet_count_this_month(self) -> int:
        """Count tweets posted this calendar month (for budget tracking)."""
        ...

    @abstractmethod
    def get_tweet_count_for_narrative_since(self, narrative_id: str, since_iso: str) -> int:
        """Count posted tweets for a specific narrative since the given ISO timestamp."""
        ...

    # --- Signal Extraction Operations ---

    @abstractmethod
    def upsert_narrative_signal(self, signal: dict) -> None:
        """Insert or replace structured signal extraction for a narrative."""
        ...

    @abstractmethod
    def get_narrative_signal(self, narrative_id: str) -> dict | None:
        """Get the latest signal extraction for a narrative."""
        ...

    @abstractmethod
    def get_all_narrative_signals(self, *, limit: int = 0, offset: int = 0) -> list[dict]:
        """Get signal extractions. limit=0 means unbounded (internal use only)."""
        ...

    @abstractmethod
    def get_narratives_by_ids(self, ids: list[str]) -> dict[str, dict]:
        """Bulk fetch narratives by ID list. Returns {narrative_id: row_dict}."""
        ...

    @abstractmethod
    def get_adversarial_events_for_narratives(self, ids: list[str], limit_per: int = 10) -> dict[str, list]:
        """Bulk fetch adversarial events keyed by narrative_id."""
        ...

    @abstractmethod
    def get_snapshot_history_for_narratives(self, ids: list[str], days: int = 90) -> dict[str, list]:
        """Bulk fetch snapshot history keyed by narrative_id."""
        ...

    # --- Convergence Operations ---

    @abstractmethod
    def upsert_ticker_convergence(self, data: dict) -> None:
        """Insert or replace convergence data for a ticker."""
        ...

    @abstractmethod
    def get_ticker_convergence(self, ticker: str) -> dict | None:
        """Get convergence data for a specific ticker."""
        ...

    @abstractmethod
    def get_all_ticker_convergences(self) -> list[dict]:
        """Get all ticker convergence records."""
        ...

    @abstractmethod
    def get_top_convergences(self, limit: int = 20) -> list[dict]:
        """Get top convergences ordered by pressure_score DESC."""
        ...

    @abstractmethod
    def clear_ticker_convergences(self) -> None:
        """Delete all ticker convergence records (used before full recompute)."""
        ...

    @abstractmethod
    def replace_ticker_convergences(self, convergences: dict[str, dict]) -> None:
        """Replace all ticker convergence rows atomically inside one transaction."""
        ...

    def check_all_orphans(self) -> dict[str, dict]:
        """Read-only precheck: returns orphan counts/samples for FK relationships.

        Returns a dict keyed by relationship name with 'count' and 'sample_ids'.
        Run this before any FK shadow-table migration to verify data integrity.
        """
        raise NotImplementedError

    # --- Impact Score Operations (Phase 6) ---

    @abstractmethod
    def upsert_impact_score(self, data: dict) -> None:
        """Insert or replace a directional impact score for a narrative+ticker pair."""
        ...

    @abstractmethod
    def get_impact_scores_for_narrative(self, narrative_id: str) -> list[dict]:
        """Get all impact scores for a narrative."""
        ...

    @abstractmethod
    def get_impact_scores_for_ticker(self, ticker: str) -> list[dict]:
        """Get all impact scores for a ticker."""
        ...

    @abstractmethod
    def get_top_impact_scores(self, limit: int = 20) -> list[dict]:
        """Get top impact scores ordered by impact_score DESC."""
        ...

    # --- Token Blacklist Operations ---

    @abstractmethod
    def blacklist_token(self, jti: str, user_id: str, expires_at: str) -> None:
        """Add a token's jti to the blacklist (for logout/revocation)."""
        ...

    @abstractmethod
    def is_token_blacklisted(self, jti: str) -> bool:
        """Check if a token jti has been revoked."""
        ...

    @abstractmethod
    def cleanup_expired_blacklist(self) -> int:
        """Remove expired entries from token_blacklist. Returns count deleted."""
        ...

    # --- Refresh Token Operations (M2) ---

    @abstractmethod
    def store_refresh_token(self, jti: str, user_id: str, expires_at: str) -> None:
        """Store a new refresh token."""
        ...

    @abstractmethod
    def get_refresh_token(self, jti: str) -> dict | None:
        """Retrieve a refresh token by jti. Returns dict or None."""
        ...

    @abstractmethod
    def revoke_refresh_token(self, jti: str) -> None:
        """Mark a refresh token as revoked."""
        ...

    @abstractmethod
    def revoke_all_user_refresh_tokens(self, user_id: str) -> int:
        """Revoke all refresh tokens for a user. Returns count revoked."""
        ...

    # --- Auth Audit Logging (M9) ---

    @abstractmethod
    def log_auth_event(self, event: dict) -> None:
        """Log an authentication event to auth_audit_log."""
        ...

    # --- Sentiment Timeseries Operations ---

    @abstractmethod
    def insert_sentiment_record(self, ticker: str, scores: dict) -> None:
        """Insert a composite sentiment record for a ticker."""
        ...

    @abstractmethod
    def get_sentiment_timeseries(self, ticker: str, hours: int = 168) -> list[dict]:
        """Get sentiment timeseries for a ticker (default 7 days = 168h), newest first."""
        ...

    @abstractmethod
    def get_latest_sentiment(self, ticker: str) -> dict | None:
        """Get the most recent sentiment record for a ticker."""
        ...

    @abstractmethod
    def insert_social_mention(self, ticker: str, source: str, counts: dict) -> None:
        """Insert a social mention count record."""
        ...

    @abstractmethod
    def get_trending_tickers(self, hours: int = 24, limit: int = 10) -> list[dict]:
        """Get top tickers by social mention volume in the last N hours."""
        ...


class SqliteRepository(Repository):
    _SAFE_COL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def _sanitize_columns(self, keys) -> list[str]:
        """Validate column names against SQL injection. Raises ValueError on bad names."""
        cols = list(keys)
        for k in cols:
            if not self._SAFE_COL_RE.match(k):
                raise ValueError(f"Invalid column name: {k!r}")
        return cols

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def migrate(self) -> None:
        import logging as _logging
        _log = _logging.getLogger(__name__)
        with self._get_conn() as conn:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                _log.info("migrate: journal_mode=WAL")
            except sqlite3.OperationalError:
                try:
                    conn.execute("PRAGMA journal_mode=DELETE")
                    _log.warning("migrate: WAL unavailable; journal_mode=DELETE")
                except sqlite3.OperationalError as exc:
                    raise RuntimeError("migrate: cannot set WAL or DELETE journal mode") from exc
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS narratives (
                    narrative_id TEXT PRIMARY KEY,
                    name TEXT,
                    description TEXT,
                    stage TEXT,
                    created_at TEXT,
                    last_updated_at TEXT,
                    is_coordinated INTEGER DEFAULT 0,
                    coordination_flag_count INTEGER DEFAULT 0,
                    suppressed INTEGER DEFAULT 0,
                    linked_assets TEXT,
                    disclaimer TEXT,
                    human_review_required INTEGER DEFAULT 0,
                    is_catalyst INTEGER DEFAULT 0,
                    document_count INTEGER DEFAULT 0,
                    velocity REAL DEFAULT 0.0,
                    velocity_windowed REAL DEFAULT 0.0,
                    centrality REAL DEFAULT 0.0,
                    entropy REAL,
                    intent_weight REAL DEFAULT 0.0,
                    ns_score REAL DEFAULT 0.0,
                    cohesion REAL DEFAULT 0.0,
                    polarization REAL DEFAULT 0.0,
                    cross_source_score REAL DEFAULT 0.0,
                    last_assignment_date TEXT,
                    consecutive_declining_cycles INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS centroid_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    narrative_id TEXT,
                    date TEXT,
                    centroid_blob BLOB
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS candidate_buffer (
                    doc_id TEXT PRIMARY KEY,
                    narrative_id_assigned TEXT,
                    embedding_blob BLOB,
                    raw_text_hash TEXT,
                    source_url TEXT,
                    source_domain TEXT,
                    published_at TEXT,
                    ingested_at TEXT,
                    status TEXT DEFAULT 'pending',
                    raw_text TEXT,
                    author TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS llm_audit_log (
                    call_id TEXT PRIMARY KEY,
                    narrative_id TEXT,
                    model TEXT,
                    task_type TEXT,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    cost_estimate_usd REAL,
                    called_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sonnet_daily_spend (
                    date TEXT PRIMARY KEY,
                    total_tokens_used INTEGER DEFAULT 0,
                    total_calls INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS adversarial_log (
                    event_id TEXT PRIMARY KEY,
                    narrative_id TEXT,
                    detected_at TEXT,
                    source_domains TEXT,
                    similarity_score REAL,
                    action_taken TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS robots_cache (
                    domain TEXT PRIMARY KEY,
                    rules_text TEXT,
                    fetched_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS failed_ingestion_jobs (
                    job_id TEXT PRIMARY KEY,
                    source_url TEXT,
                    source_type TEXT,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    next_retry_at TEXT,
                    created_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_run_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    step_number INTEGER,
                    step_name TEXT,
                    status TEXT,
                    error_message TEXT,
                    duration_ms INTEGER,
                    run_at TEXT
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prl_run_id "
                "ON pipeline_run_log(run_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prl_step "
                "ON pipeline_run_log(step_name, run_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prl_run_step "
                "ON pipeline_run_log(run_id, step_number)"
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS narrative_assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    narrative_id TEXT,
                    doc_id TEXT,
                    assigned_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS document_evidence (
                    doc_id TEXT PRIMARY KEY,
                    narrative_id TEXT,
                    source_url TEXT,
                    source_domain TEXT,
                    published_at TEXT,
                    author TEXT,
                    excerpt TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS narrative_snapshots (
                    id TEXT PRIMARY KEY,
                    narrative_id TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    ns_score REAL,
                    velocity REAL,
                    entropy REAL,
                    cohesion REAL,
                    polarization REAL,
                    doc_count INTEGER,
                    lifecycle_stage TEXT,
                    haiku_label TEXT,
                    haiku_description TEXT,
                    sonnet_analysis TEXT,
                    created_at TEXT,
                    UNIQUE(narrative_id, snapshot_date)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mutation_events (
                    id TEXT PRIMARY KEY,
                    narrative_id TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    mutation_type TEXT NOT NULL,
                    previous_value TEXT,
                    new_value TEXT,
                    magnitude REAL,
                    haiku_explanation TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS portfolios (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL DEFAULT 'local',
                    name TEXT DEFAULT 'My Portfolio',
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_holdings (
                    id TEXT PRIMARY KEY,
                    portfolio_id TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    shares REAL NOT NULL,
                    cost_basis REAL,
                    added_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watchlists (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL DEFAULT 'local',
                    name TEXT NOT NULL,
                    created_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watchlist_items (
                    id TEXT PRIMARY KEY,
                    watchlist_id TEXT NOT NULL,
                    item_type TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    added_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS notification_rules (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL DEFAULT 'local',
                    rule_type TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_id TEXT,
                    threshold REAL,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL DEFAULT 'local',
                    rule_id TEXT,
                    title TEXT NOT NULL,
                    message TEXT,
                    link TEXT,
                    is_read INTEGER DEFAULT 0,
                    created_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_usage_log (
                    id TEXT PRIMARY KEY,
                    api_name TEXT NOT NULL,
                    date TEXT NOT NULL,
                    requests_used INTEGER DEFAULT 0,
                    requests_limit INTEGER,
                    last_request_at TEXT,
                    UNIQUE(api_name, date)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_cache (
                    ticker TEXT PRIMARY KEY,
                    name TEXT,
                    price REAL,
                    change_pct REAL,
                    volume INTEGER,
                    market_cap INTEGER,
                    sector TEXT,
                    industry TEXT,
                    sparkline_7d TEXT,
                    sparkline_30d TEXT,
                    updated_at TEXT
                )
            """)
            # Phase 2: price tick storage and 1-min candle aggregation
            conn.execute("""
                CREATE TABLE IF NOT EXISTS price_ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    volume REAL,
                    timestamp TEXT NOT NULL,
                    source TEXT NOT NULL
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_price_ticks_symbol_ts "
                "ON price_ticks(symbol, timestamp)"
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS price_candles_1m (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL,
                    timestamp TEXT NOT NULL
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_candles_1m_symbol_ts "
                "ON price_candles_1m(symbol, timestamp)"
            )

            # Uniqueness constraints on price tables
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_price_ticks_unique "
                "ON price_ticks(symbol, timestamp, source)"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_candles_1m_unique "
                "ON price_candles_1m(symbol, timestamp)"
            )

            # L2: add source column to price_candles_1m
            try:
                conn.execute("ALTER TABLE price_candles_1m ADD COLUMN source TEXT")
            except Exception:
                pass

            # Idempotent migration: add description column if it doesn't exist yet
            try:
                conn.execute("ALTER TABLE narratives ADD COLUMN description TEXT DEFAULT NULL")
            except Exception:
                pass  # Column already exists — safe to ignore

            # F2: burst velocity columns on narrative_snapshots
            for col, coltype in [
                ("burst_ratio", "REAL DEFAULT NULL"),
                ("burst_detected_at", "TEXT DEFAULT NULL"),
                ("snapshot_time", "TEXT DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narrative_snapshots ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # F4: topic_tags column on narratives
            try:
                conn.execute("ALTER TABLE narratives ADD COLUMN topic_tags TEXT DEFAULT NULL")
            except Exception:
                pass

            # F2-fix: burst_ratio column on narratives
            try:
                conn.execute("ALTER TABLE narratives ADD COLUMN burst_ratio REAL DEFAULT NULL")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE narratives ADD COLUMN pipeline_computed_at TEXT DEFAULT NULL")
            except Exception:
                pass

            # F5: extend narrative_snapshots with linked_assets, topic_tags, burst_ratio
            for col, coltype in [
                ("linked_assets", "TEXT DEFAULT NULL"),
                ("topic_tags", "TEXT DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narrative_snapshots ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # V3 Phase 4.1: Performance indexes
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_narratives_ns_score ON narratives(ns_score)",
                "CREATE INDEX IF NOT EXISTS idx_narratives_stage ON narratives(stage)",
                "CREATE INDEX IF NOT EXISTS idx_doc_evidence_narrative ON document_evidence(narrative_id, published_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_mutations_narrative ON mutation_events(narrative_id, detected_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_adversarial_narrative ON adversarial_log(narrative_id)",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_narrative ON narrative_snapshots(narrative_id, snapshot_date DESC)",
                "CREATE INDEX IF NOT EXISTS idx_centroid_narrative ON centroid_history(narrative_id, date DESC)",
                "CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, is_read)",
                "CREATE INDEX IF NOT EXISTS idx_candidate_buffer_status ON candidate_buffer(status)",
                "CREATE INDEX IF NOT EXISTS idx_watchlist_items ON watchlist_items(watchlist_id)",
                "CREATE INDEX IF NOT EXISTS idx_assignments_doc ON narrative_assignments(doc_id)",
                "CREATE INDEX IF NOT EXISTS idx_assignments_narrative ON narrative_assignments(narrative_id)",
                "CREATE INDEX IF NOT EXISTS idx_portfolio_holdings_portfolio ON portfolio_holdings(portfolio_id)",
                "CREATE INDEX IF NOT EXISTS idx_candidate_buffer_ingested ON candidate_buffer(ingested_at)",
                # P11c Batch 5: composite indexes for live query shapes
                "CREATE INDEX IF NOT EXISTS idx_watchlists_user_created ON watchlists(user_id, created_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_notification_rules_user_created ON notification_rules(user_id, created_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_notifications_rule_created ON notifications(rule_id, created_at)",
            ]:
                try:
                    conn.execute(idx_sql)
                except Exception:
                    pass

            # V3 Phase 1.7: source_type column on document_evidence
            try:
                conn.execute("ALTER TABLE document_evidence ADD COLUMN source_type TEXT DEFAULT 'news'")
            except Exception:
                pass

            # V3 Phase 3.2: public_interest column on narratives
            try:
                conn.execute("ALTER TABLE narratives ADD COLUMN public_interest REAL DEFAULT NULL")
            except Exception:
                pass

            # Phase 2 Batch 5: Changelog enrichment columns on mutation_events
            for col, coltype in [
                ("contributing_documents", "TEXT DEFAULT NULL"),
                ("pipeline_run_id", "TEXT DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE mutation_events ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # Phase 3 Batch 3: Deep analysis cache on narratives
            for col, coltype in [
                ("deep_analysis", "TEXT DEFAULT NULL"),
                ("deep_analysis_at", "TEXT DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narratives ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # Phase 0: Signal validation — new columns on narratives
            for col, coltype in [
                ("sentiment_mean", "REAL DEFAULT NULL"),
                ("sentiment_variance", "REAL DEFAULT NULL"),
                ("source_count", "INTEGER DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narratives ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # Phase 0: Signal validation — expand narrative_snapshots for multi-signal analysis
            for col, coltype in [
                ("sentiment_mean", "REAL DEFAULT NULL"),
                ("sentiment_variance", "REAL DEFAULT NULL"),
                ("source_count", "INTEGER DEFAULT NULL"),
                ("intent_weight", "REAL DEFAULT NULL"),
                ("cross_source_score", "REAL DEFAULT NULL"),
                ("weighted_source_score", "REAL DEFAULT NULL"),
                ("centrality", "REAL DEFAULT NULL"),
                ("velocity_windowed", "REAL DEFAULT NULL"),
                ("public_interest", "REAL DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narrative_snapshots ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # Phase 2 Batch 6: Users table for JWT auth
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email)"
            )

            # Tweet log for automated X/Twitter posting
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tweet_log (
                    id TEXT PRIMARY KEY,
                    narrative_id TEXT NOT NULL,
                    tweet_id TEXT,
                    tweet_text TEXT,
                    tweet_type TEXT NOT NULL,
                    parent_tweet_id TEXT,
                    metrics_snapshot TEXT DEFAULT '{}',
                    posted_at TEXT,
                    status TEXT DEFAULT 'posted'
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tweet_log_narrative "
                "ON tweet_log(narrative_id, posted_at)"
            )

            # Phase 1 Signal Redesign: structured signal extraction table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS narrative_signals (
                    narrative_id TEXT PRIMARY KEY,
                    direction TEXT NOT NULL DEFAULT 'neutral',
                    confidence REAL NOT NULL DEFAULT 0.0,
                    timeframe TEXT NOT NULL DEFAULT 'unknown',
                    magnitude TEXT NOT NULL DEFAULT 'incremental',
                    certainty TEXT NOT NULL DEFAULT 'speculative',
                    key_actors TEXT NOT NULL DEFAULT '[]',
                    affected_sectors TEXT NOT NULL DEFAULT '[]',
                    catalyst_type TEXT NOT NULL DEFAULT 'unknown',
                    extracted_at TEXT NOT NULL,
                    raw_response TEXT
                )
            """)

            # Phase 2 Signal Redesign: source tier tracking columns on narratives
            for col, coltype in [
                ("source_highest_tier", "INTEGER DEFAULT NULL"),
                ("source_tier_breadth", "INTEGER DEFAULT NULL"),
                ("source_escalation_velocity", "REAL DEFAULT NULL"),
                ("source_institutional_pickup", "INTEGER DEFAULT 0"),
                ("weighted_source_score", "REAL DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narratives ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # Phase 3 Signal Redesign: ticker convergence table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ticker_convergence (
                    ticker TEXT PRIMARY KEY,
                    convergence_count INTEGER NOT NULL DEFAULT 0,
                    direction_agreement REAL NOT NULL DEFAULT 0.0,
                    direction_consensus REAL NOT NULL DEFAULT 0.0,
                    weighted_confidence REAL NOT NULL DEFAULT 0.0,
                    source_diversity INTEGER NOT NULL DEFAULT 0,
                    pressure_score REAL NOT NULL DEFAULT 0.0,
                    contributing_narrative_ids TEXT NOT NULL DEFAULT '[]',
                    computed_at TEXT NOT NULL DEFAULT ''
                )
            """)
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_ticker_convergence_pressure ON ticker_convergence(pressure_score DESC)")
            except Exception:
                pass

            # Phase 3 Signal Redesign: convergence exposure column on narratives
            for col, coltype in [
                ("convergence_exposure", "REAL DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narratives ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # Phase 4 Signal Redesign: catalyst anchoring columns on narratives
            for col, coltype in [
                ("catalyst_proximity_score", "REAL DEFAULT NULL"),
                ("days_to_catalyst", "INTEGER DEFAULT NULL"),
                ("catalyst_type", "TEXT DEFAULT NULL"),
                ("macro_alignment", "REAL DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narratives ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # Phase 5 Signal Redesign: inflow velocity columns on narratives
            for col, coltype in [
                ("inflow_velocity", "REAL DEFAULT NULL"),
                ("avg_docs_per_cycle_7d", "REAL DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narratives ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

            # Foundation Fix 1: hysteresis cycle counter
            try:
                conn.execute("ALTER TABLE narratives ADD COLUMN cycles_in_current_stage INTEGER DEFAULT 0")
            except Exception:
                pass

            # Health Fix 7v4: track repeated labeling failures on narratives
            try:
                conn.execute("ALTER TABLE narratives ADD COLUMN labeling_attempts INTEGER DEFAULT 0")
            except Exception:
                pass

            # Foundation Fix 3: conditional HTTP for RSS feeds
            conn.execute("""
                CREATE TABLE IF NOT EXISTS feed_metadata (
                    feed_url TEXT PRIMARY KEY,
                    etag TEXT,
                    last_modified TEXT,
                    last_fetched_at TEXT,
                    consecutive_empty_cycles INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS token_blacklist (
                    jti TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    blacklisted_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )
            """)
            # M2: Refresh token rotation
            conn.execute("""
                CREATE TABLE IF NOT EXISTS refresh_tokens (
                    jti TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    revoked INTEGER DEFAULT 0
                )
            """)
            # M9: Auth audit logging
            conn.execute("""
                CREATE TABLE IF NOT EXISTS auth_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    email TEXT,
                    user_id TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    success INTEGER NOT NULL,
                    details TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_auth_audit_email_event
                    ON auth_audit_log (email, event_type, created_at)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_auth_audit_created
                    ON auth_audit_log (created_at)
            """)

            # Part C: Social Sentiment System tables
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sentiment_timeseries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    composite_score REAL,
                    news_component REAL,
                    social_component REAL,
                    momentum_component REAL,
                    message_volume INTEGER DEFAULT 0,
                    recorded_at TEXT NOT NULL
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sentiment_ticker_time "
                "ON sentiment_timeseries(ticker, recorded_at DESC)"
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS social_mentions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    source TEXT NOT NULL,
                    mention_count INTEGER DEFAULT 0,
                    bullish_count INTEGER DEFAULT 0,
                    bearish_count INTEGER DEFAULT 0,
                    recorded_at TEXT NOT NULL
                )
            """)
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_social_mentions_recorded_ticker ON social_mentions(recorded_at, ticker)")
            except Exception:
                pass

            # Phase 6 Signal Redesign: directional impact scores
            conn.execute("""
                CREATE TABLE IF NOT EXISTS impact_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    narrative_id TEXT,
                    ticker TEXT,
                    direction TEXT,
                    impact_score REAL,
                    confidence REAL,
                    time_horizon TEXT,
                    signal_components TEXT,
                    computed_at TEXT,
                    UNIQUE(narrative_id, ticker)
                )
            """)

            # L2: Email verification columns
            try:
                conn.execute("ALTER TABLE users ADD COLUMN email_verified INTEGER DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN verification_token TEXT")
            except Exception:
                pass

            # L3: RBAC role column
            try:
                conn.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user'")
            except Exception:
                pass

            # Part C: Dashboard layout persistence
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dashboard_layouts (
                    user_id TEXT PRIMARY KEY,
                    layout_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            # Fix pipeline_run_log: run_id was PRIMARY KEY but multiple steps
            # share the same run_id per pipeline cycle. Migrate to auto-inc id.
            try:
                row = conn.execute(
                    "SELECT sql FROM sqlite_master "
                    "WHERE type='table' AND name='pipeline_run_log'"
                ).fetchone()
                if row and "run_id TEXT PRIMARY KEY" in (row[0] or ""):
                    conn.execute("""
                        CREATE TABLE pipeline_run_log_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            run_id TEXT NOT NULL,
                            step_number INTEGER,
                            step_name TEXT,
                            status TEXT,
                            error_message TEXT,
                            duration_ms INTEGER,
                            run_at TEXT
                        )
                    """)
                    conn.execute("""
                        INSERT INTO pipeline_run_log_new
                            (run_id, step_number, step_name, status,
                             error_message, duration_ms, run_at)
                        SELECT run_id, step_number, step_name, status,
                               error_message, duration_ms, run_at
                        FROM pipeline_run_log
                    """)
                    conn.execute("DROP TABLE pipeline_run_log")
                    conn.execute(
                        "ALTER TABLE pipeline_run_log_new "
                        "RENAME TO pipeline_run_log"
                    )
            except Exception:
                pass
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prl_run_id "
                "ON pipeline_run_log(run_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prl_step "
                "ON pipeline_run_log(step_name, run_at DESC)"
            )

            # --- Migration: Clean TOPIC: pseudo-tickers from linked_assets ---
            rows = conn.execute(
                "SELECT narrative_id, linked_assets FROM narratives "
                "WHERE linked_assets LIKE '%TOPIC:%'"
            ).fetchall()
            if rows:
                cleaned_count = 0
                for row in rows:
                    raw = row[1]
                    if not raw:
                        continue
                    try:
                        assets = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if isinstance(assets, list) and assets:
                        cleaned = []
                        for a in assets:
                            if isinstance(a, dict):
                                if isinstance(a.get("ticker", ""), str) and a["ticker"].startswith("TOPIC:"):
                                    continue
                            elif isinstance(a, str) and a.startswith("TOPIC:"):
                                continue
                            cleaned.append(a)
                        if len(cleaned) != len(assets):
                            conn.execute(
                                "UPDATE narratives SET linked_assets = ? "
                                "WHERE narrative_id = ?",
                                (json.dumps(cleaned), row[0]),
                            )
                            cleaned_count += 1
                if cleaned_count:
                    logger.info(
                        "Cleaned TOPIC: pseudo-tickers from %d narratives",
                        cleaned_count,
                    )

            # --- Migration: Rename consecutive_declining_days → consecutive_declining_cycles ---
            columns = {
                col[1] for col in conn.execute("PRAGMA table_info(narratives)").fetchall()
            }
            if "consecutive_declining_cycles" not in columns:
                try:
                    conn.execute("ALTER TABLE narratives ADD COLUMN consecutive_declining_cycles INTEGER DEFAULT 0")
                except Exception:
                    pass
            if "consecutive_declining_days" in columns:
                # Copy non-zero values from old column, then drop it
                conn.execute("""
                    UPDATE narratives
                    SET consecutive_declining_cycles = consecutive_declining_days
                    WHERE consecutive_declining_days > 0
                      AND (consecutive_declining_cycles IS NULL OR consecutive_declining_cycles = 0)
                """)
                # SQLite ≥ 3.35.0 supports DROP COLUMN
                try:
                    conn.execute("ALTER TABLE narratives DROP COLUMN consecutive_declining_days")
                    logger.info("Dropped legacy column consecutive_declining_days")
                except Exception:
                    # Older SQLite — column persists but is unused
                    logger.debug("Could not drop consecutive_declining_days (SQLite < 3.35)")

            # --- Migration: Remove weak asset links below configured threshold ---
            # Lazy import to avoid any circular-dependency risk at module load
            # time (get_settings() is a safe singleton — settings.py has no
            # project-level imports).
            from settings import get_settings as _get_settings
            _weak_floor = _get_settings().ASSET_MAPPING_MIN_SIMILARITY

            weak_rows = conn.execute(
                "SELECT narrative_id, linked_assets FROM narratives "
                "WHERE linked_assets IS NOT NULL AND linked_assets != '[]' "
                "AND linked_assets != 'null'"
            ).fetchall()
            weak_cleaned = 0
            for row in weak_rows:
                raw = row[1]
                if not raw:
                    continue
                try:
                    assets = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if isinstance(assets, list) and assets:
                    # Single-pass O(n) partition — keep strong links, track weak
                    # ones separately for per-link audit logging.
                    filtered = []
                    removed = []
                    for a in assets:
                        if isinstance(a, dict) and a.get("similarity_score", 1.0) < _weak_floor:
                            removed.append(a)
                        else:
                            filtered.append(a)
                    if removed:
                        logger.info(
                            "Narrative %s: removing %d weak asset link(s): %s",
                            row[0],
                            len(removed),
                            ", ".join(
                                f"{a.get('ticker')}@{a.get('similarity_score', '?')}"
                                for a in removed
                            ),
                        )
                        conn.execute(
                            "UPDATE narratives SET linked_assets = ? "
                            "WHERE narrative_id = ?",
                            (json.dumps(filtered) if filtered else "[]", row[0]),
                        )
                        weak_cleaned += 1
            if weak_cleaned:
                logger.info(
                    "Cleaned weak asset links (< %.2f similarity) from %d narratives",
                    _weak_floor, weak_cleaned,
                )

    # --- Narrative Operations ---

    def get_narrative(self, narrative_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM narratives WHERE narrative_id = ?", (narrative_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_all_active_narratives(self, *, limit: int = 0, offset: int = 0,
                                  stage: str | None = None, topic: str | None = None) -> list[dict]:
        with self._get_conn() as conn:
            sql = "SELECT * FROM narratives WHERE suppressed = 0"
            params: list = []
            if stage:
                sql += " AND stage = ?"
                params.append(stage)
            else:
                sql += " AND stage != 'Dormant'"
            if topic:
                sql += (" AND topic_tags IS NOT NULL"
                        " AND EXISTS (SELECT 1 FROM json_each(topic_tags) WHERE value = ?)")
                params.append(topic)
            sql += " ORDER BY CAST(ns_score AS REAL) DESC"
            if limit > 0:
                sql += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def count_active_narratives(self, *, stage: str | None = None, topic: str | None = None) -> int:
        with self._get_conn() as conn:
            sql = "SELECT COUNT(*) as cnt FROM narratives WHERE suppressed = 0"
            params: list = []
            if stage:
                sql += " AND stage = ?"
                params.append(stage)
            else:
                sql += " AND stage != 'Dormant'"
            if topic:
                sql += (" AND topic_tags IS NOT NULL"
                        " AND EXISTS (SELECT 1 FROM json_each(topic_tags) WHERE value = ?)")
                params.append(topic)
            row = conn.execute(sql, params).fetchone()
            return int(row["cnt"]) if row else 0

    def insert_narrative(self, narrative: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(narrative.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO narratives ({cols}) VALUES ({placeholders})",
                list(narrative.values()),
            )

    def atomically_persist_cluster(
        self,
        narrative: dict,
        cycle_slot: str,
        centroid_blob: bytes,
        member_docs: list[dict],
        today: str,
        post_write_hook=None,
    ) -> None:
        """
        Persist a newly created cluster in one SQLite transaction.

        If any statement fails (or post_write_hook raises), all DB writes roll back.
        """
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(narrative.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO narratives ({cols}) VALUES ({placeholders})",
                list(narrative.values()),
            )
            conn.execute(
                "INSERT INTO centroid_history (narrative_id, date, centroid_blob) VALUES (?, ?, ?)",
                (narrative["narrative_id"], cycle_slot, centroid_blob),
            )

            for doc in member_docs:
                doc_id = doc.get("doc_id")
                if not doc_id:
                    continue
                conn.execute(
                    "INSERT INTO narrative_assignments (narrative_id, doc_id, assigned_at) VALUES (?, ?, ?)",
                    (narrative["narrative_id"], doc_id, today),
                )
                conn.execute(
                    "UPDATE candidate_buffer SET status = ?, narrative_id_assigned = ? WHERE doc_id = ?",
                    ("clustered", narrative["narrative_id"], doc_id),
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO document_evidence
                        (doc_id, narrative_id, source_url, source_domain, published_at, author, excerpt)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        doc_id,
                        narrative["narrative_id"],
                        doc.get("source_url") or "",
                        doc.get("source_domain") or "",
                        doc.get("published_at") or "",
                        doc.get("author") or "",
                        (doc.get("raw_text") or "")[:500],
                    ),
                )

            if post_write_hook is not None:
                post_write_hook()

    def verify_cluster_consistency(self, vector_ids: set[str]) -> dict:
        """Detect divergence between active SQLite narratives and in-memory vector IDs."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT narrative_id FROM narratives WHERE suppressed = 0 AND stage != 'Dormant'"
            ).fetchall()
        active_ids = {r[0] for r in rows}
        missing = sorted(active_ids - vector_ids)
        if missing:
            logger.warning(
                "Cluster divergence detected: %d active narratives missing from vector index",
                len(missing),
            )
        return {
            "active_count": len(active_ids),
            "vector_count": len(vector_ids),
            "missing_count": len(missing),
            "missing_ids": missing,
        }

    def update_narrative(self, narrative_id: str, updates: dict) -> None:
        if not updates:
            return
        normalized = dict(updates)
        if "document_count" in normalized:
            try:
                normalized["document_count"] = max(int(round(float(normalized["document_count"]))), 0)
            except (TypeError, ValueError):
                normalized["document_count"] = 0
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(normalized.keys())
            set_clause = ", ".join(f"{k} = ?" for k in safe_cols)
            values = [normalized[k] for k in safe_cols] + [narrative_id]
            conn.execute(
                f"UPDATE narratives SET {set_clause} WHERE narrative_id = ?",
                values,
            )

    def merge_narrative(self, survivor_id: str, absorbed_id: str, vector_store=None) -> None:
        """Merge absorbed narrative into survivor. Reassign docs, update counts, mark absorbed Dormant.

        Caller must call vector_store.save() after one or more merges to persist centroid deletions.
        """
        if survivor_id == absorbed_id:
            return

        old_desc: str = ""
        with self._get_conn() as conn:
            # Validate survivor exists
            survivor = conn.execute(
                "SELECT narrative_id FROM narratives WHERE narrative_id = ?",
                (survivor_id,),
            ).fetchone()
            if not survivor:
                raise ValueError(f"Survivor narrative {survivor_id} does not exist")

            # Validate absorbed exists and check idempotency
            row = conn.execute(
                "SELECT stage, description FROM narratives WHERE narrative_id = ?",
                (absorbed_id,),
            ).fetchone()
            if not row:
                raise ValueError(f"Absorbed narrative {absorbed_id} does not exist")
            if row["stage"] == "Dormant" and "Merged into" in (row["description"] or ""):
                return  # already merged in a prior run — skip
            old_desc = row["description"] or ""

        if vector_store is not None:
            try:
                vector_store.delete(absorbed_id)
            except Exception as exc:
                logger.error(
                    "merge_narrative aborted: failed to delete centroid for %s before DB merge: %s",
                    absorbed_id,
                    exc,
                )
                raise

        try:
            with self._get_conn() as conn:
            # 1. Reassign document_evidence rows
                conn.execute(
                    "UPDATE document_evidence SET narrative_id = ? WHERE narrative_id = ?",
                    (survivor_id, absorbed_id),
                )

                # 2. Reassign narrative_assignments rows
                conn.execute(
                    "UPDATE narrative_assignments SET narrative_id = ? WHERE narrative_id = ?",
                    (survivor_id, absorbed_id),
                )

                # 3. Update survivor's document_count to combined total
                combined = conn.execute(
                    "SELECT COUNT(*) as cnt FROM document_evidence WHERE narrative_id = ?",
                    (survivor_id,),
                ).fetchone()
                conn.execute(
                    "UPDATE narratives SET document_count = ? WHERE narrative_id = ?",
                    (combined["cnt"], survivor_id),
                )

                # 4. Preserve existing description, append merge note
                merge_note = f"Merged into {survivor_id}"
                new_desc = f"{old_desc} | {merge_note}" if old_desc else merge_note
                conn.execute(
                    "UPDATE narratives SET stage = 'Dormant', description = ?, document_count = 0 WHERE narrative_id = ?",
                    (new_desc, absorbed_id),
                )
        except Exception as exc:
            logger.error(
                "merge_narrative DB update failed after centroid delete; manual consistency audit needed "
                "(survivor=%s absorbed=%s): %s",
                survivor_id,
                absorbed_id,
                exc,
            )
            raise

    def update_narrative_tags(self, narrative_id: str, tags: list) -> None:
        """Store topic tags as JSON array string."""
        import json
        self.update_narrative(narrative_id, {"topic_tags": json.dumps(tags)})

    def get_narrative_count(self) -> int:
        with self._get_conn() as conn:
            row = conn.execute("SELECT COUNT(*) FROM narratives").fetchone()
            return row[0]

    def get_narratives_by_stage(self, stage: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM narratives WHERE stage = ?", (stage,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_narratives_needing_decay(self, current_date: str) -> list[str]:
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT narrative_id FROM narratives
                WHERE suppressed = 0
                  AND stage != 'Dormant'
                  AND narrative_id NOT IN (
                      SELECT narrative_id FROM narrative_assignments
                      WHERE assigned_at = ?
                  )
                """,
                (current_date,),
            ).fetchall()
            return [r[0] for r in rows]

    def record_narrative_assignment(self, narrative_id: str, date: str) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO narrative_assignments (narrative_id, assigned_at) VALUES (?, ?)",
                (narrative_id, date),
            )

    # --- Candidate Buffer Operations ---

    def get_candidate_buffer(self, status: str = "pending", limit: int | None = None) -> list[dict]:
        with self._get_conn() as conn:
            sql = (
                "SELECT * FROM candidate_buffer WHERE status = ? "
                "ORDER BY ingested_at ASC, doc_id ASC"
            )
            params: list = [status]
            if limit is not None:
                sql += " LIMIT ?"
                params.append(int(limit))
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def insert_candidate(self, candidate: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(candidate.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT OR IGNORE INTO candidate_buffer ({cols}) VALUES ({placeholders})",
                list(candidate.values()),
            )

    def update_candidate_status(
        self, doc_id: str, status: str, narrative_id_assigned: str = None
    ) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE candidate_buffer
                SET status = ?, narrative_id_assigned = ?
                WHERE doc_id = ?
                """,
                (status, narrative_id_assigned, doc_id),
            )

    def get_candidate_buffer_count(self, status: str = "pending") -> int:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM candidate_buffer WHERE status = ?", (status,)
            ).fetchone()
            return row[0]

    def get_corpus_domain_count(self) -> int:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT source_domain) FROM candidate_buffer "
                "WHERE status IN ('clustered', 'pending') AND source_domain IS NOT NULL "
                "AND source_domain != ''"
            ).fetchone()
            return row[0]

    def clear_candidate_buffer(self, status: str = "clustered") -> None:
        with self._get_conn() as conn:
            conn.execute(
                "DELETE FROM candidate_buffer WHERE status = ?", (status,)
            )

    def delete_old_candidate_buffer(self, days: int) -> int:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        ).isoformat()
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM candidate_buffer WHERE status = 'clustered' AND ingested_at < ?",
                (cutoff,),
            )
            return cursor.rowcount

    # --- Centroid History Operations ---

    def insert_centroid_history(
        self, narrative_id: str, date: str, centroid_blob: bytes
    ) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO centroid_history (narrative_id, date, centroid_blob) VALUES (?, ?, ?)",
                (narrative_id, date, centroid_blob),
            )

    def get_centroid_history(self, narrative_id: str, days: int, *, limit: int = 0, offset: int = 0) -> list[dict]:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        ).isoformat()[:10]  # date portion
        with self._get_conn() as conn:
            # Deduplicate to one entry per cycle slot (latest by rowid) so that
            # velocity compares distinct pipeline windows, not intra-slot noise.
            sql = """
                SELECT id, narrative_id, date, centroid_blob FROM (
                    SELECT id, narrative_id, date, centroid_blob,
                           ROW_NUMBER() OVER (PARTITION BY date ORDER BY id DESC) AS rn
                    FROM centroid_history
                    WHERE narrative_id = ? AND date >= ?
                ) WHERE rn = 1
                ORDER BY date DESC
            """
            params: list = [narrative_id, cutoff]
            if limit > 0:
                sql += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def get_latest_centroid(self, narrative_id: str) -> bytes | None:
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT centroid_blob FROM centroid_history
                WHERE narrative_id = ?
                ORDER BY date DESC
                LIMIT 1
                """,
                (narrative_id,),
            ).fetchone()
            return row[0] if row else None

    def get_latest_centroids_batch(self, narrative_ids: list[str]) -> dict[str, bytes]:
        """Get most recent centroid blobs for multiple narratives in one query."""
        if not narrative_ids:
            return {}
        result: dict[str, bytes] = {}
        _CHUNK = 500
        with self._get_conn() as conn:
            for i in range(0, len(narrative_ids), _CHUNK):
                chunk = narrative_ids[i:i + _CHUNK]
                placeholders = ",".join("?" * len(chunk))
                rows = conn.execute(
                    f"""
                    SELECT narrative_id, centroid_blob FROM (
                        SELECT narrative_id, centroid_blob,
                               ROW_NUMBER() OVER (
                                   PARTITION BY narrative_id ORDER BY date DESC
                               ) AS rn
                        FROM centroid_history
                        WHERE narrative_id IN ({placeholders})
                    ) WHERE rn = 1
                    """,
                    chunk,
                ).fetchall()
                result.update({r[0]: r[1] for r in rows})
        return result

    def count_suppressed_with_documents(self) -> int:
        """Count suppressed narratives receiving documents in the last 3 days (leak indicator)."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT n.narrative_id) FROM narratives n "
                "JOIN narrative_assignments na ON n.narrative_id = na.narrative_id "
                "WHERE n.suppressed = 1 "
                "AND na.assigned_at >= DATE('now', '-3 days')"
            ).fetchone()
            return row[0] if row else 0

    # --- LLM Audit Operations ---

    def log_llm_call(self, call_record: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(call_record.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO llm_audit_log ({cols}) VALUES ({placeholders})",
                list(call_record.values()),
            )

    def get_sonnet_calls_last_24h(self, narrative_id: str) -> list[dict]:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
        ).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM llm_audit_log
                WHERE narrative_id = ?
                  AND model LIKE '%sonnet%'
                  AND called_at >= ?
                """,
                (narrative_id, cutoff),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_sonnet_daily_spend(self, date: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM sonnet_daily_spend WHERE date = ?", (date,)
            ).fetchone()
            return dict(row) if row else None

    def update_sonnet_daily_spend(self, date: str, tokens: int, calls: int) -> None:
        # TODO SCALE: replace SQLite counter with Redis INCR for atomic budget tracking under concurrent workers
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO sonnet_daily_spend (date, total_tokens_used, total_calls)
                VALUES (?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    total_tokens_used = total_tokens_used + excluded.total_tokens_used,
                    total_calls = total_calls + excluded.total_calls
                """,
                (date, tokens, calls),
            )

    def get_daily_llm_spend(self) -> float:
        today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(cost_estimate_usd), 0) FROM llm_audit_log WHERE called_at >= ?",
                (today,),
            ).fetchone()
            return float(row[0]) if row else 0.0

    # --- Adversarial Operations ---

    def log_adversarial_event(self, event: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(event.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO adversarial_log ({cols}) VALUES ({placeholders})",
                list(event.values()),
            )

    def get_coordination_flags_rolling_window(
        self, narrative_id: str, days: int
    ) -> int:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        ).isoformat()
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM adversarial_log
                WHERE narrative_id = ? AND detected_at >= ?
                """,
                (narrative_id, cutoff),
            ).fetchone()
            return row[0]

    # --- Robots Cache Operations ---

    def get_robots_cache(self, domain: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM robots_cache WHERE domain = ?", (domain,)
            ).fetchone()
            return dict(row) if row else None

    def set_robots_cache(
        self, domain: str, rules_text: str, fetched_at: str
    ) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO robots_cache (domain, rules_text, fetched_at)
                VALUES (?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    rules_text = excluded.rules_text,
                    fetched_at = excluded.fetched_at
                """,
                (domain, rules_text, fetched_at),
            )

    # --- Failed Job Operations ---

    def insert_failed_job(self, job: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(job.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO failed_ingestion_jobs ({cols}) VALUES ({placeholders})",
                list(job.values()),
            )

    def get_retryable_failed_jobs(self, current_time: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM failed_ingestion_jobs
                WHERE next_retry_at <= ? AND retry_count < 3
                """,
                (current_time,),
            ).fetchall()
            return [dict(r) for r in rows]

    def update_failed_job_retry(
        self, job_id: str, retry_count: int, next_retry_at: str
    ) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE failed_ingestion_jobs
                SET retry_count = ?, next_retry_at = ?
                WHERE job_id = ?
                """,
                (retry_count, next_retry_at, job_id),
            )

    def delete_failed_job(self, job_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "DELETE FROM failed_ingestion_jobs WHERE job_id = ?", (job_id,)
            )

    # --- Pipeline Run Log Operations ---

    def log_pipeline_run(self, run_record: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(run_record.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO pipeline_run_log ({cols}) VALUES ({placeholders})",
                list(run_record.values()),
            )

    def get_recent_mutations(self, limit: int = 20) -> list[dict]:
        """Return recent mutation events with narrative names."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT me.*, n.name as narrative_name FROM mutation_events me "
                "LEFT JOIN narratives n ON me.narrative_id = n.narrative_id "
                "ORDER BY me.detected_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_recent_pipeline_events(self, limit: int = 20) -> list[dict]:
        """Return recent pipeline run log entries for cleanup/initialization steps."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_run_log WHERE step_name IN ('cleanup', 'initialization') "
                "ORDER BY run_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    # --- Document Evidence Operations ---

    def insert_document_evidence(self, evidence: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(evidence.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO document_evidence ({cols}) VALUES ({placeholders})",
                list(evidence.values()),
            )

    def get_document_evidence(self, narrative_id: str, *, limit: int = 0, offset: int = 0) -> list[dict]:
        with self._get_conn() as conn:
            sql = "SELECT * FROM document_evidence WHERE narrative_id = ? ORDER BY published_at DESC"
            params: list = [narrative_id]
            if limit > 0:
                sql += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def count_document_evidence(self, narrative_id: str) -> int:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM document_evidence WHERE narrative_id = ?",
                (narrative_id,),
            ).fetchone()
            return int(row["cnt"]) if row else 0

    def get_document_evidence_by_ids(self, doc_ids: list[str]) -> list[dict]:
        if not doc_ids:
            return []
        with self._get_conn() as conn:
            placeholders = ", ".join("?" * len(doc_ids))
            rows = conn.execute(
                f"SELECT * FROM document_evidence WHERE doc_id IN ({placeholders})",
                doc_ids,
            ).fetchall()
            return [dict(r) for r in rows]

    # --- Quick Refresh Operations ---

    def assign_doc_to_narrative(self, doc_id: str, narrative_id: str) -> None:
        today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO narrative_assignments (narrative_id, doc_id, assigned_at) "
                "VALUES (?, ?, ?)",
                (narrative_id, doc_id, today),
            )
            conn.execute(
                "UPDATE narratives SET last_assignment_date = ?, last_updated_at = ? "
                "WHERE narrative_id = ?",
                (today, now_iso, narrative_id),
            )

    def get_narrative_doc_count(self, narrative_id: str) -> int:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM document_evidence WHERE narrative_id = ?",
                (narrative_id,),
            ).fetchone()
            return row[0] if row else 0

    def update_narrative_doc_count(self, narrative_id: str, count: int) -> None:
        self.update_narrative(narrative_id, {"document_count": count})

    # --- Snapshot Operations ---

    def save_snapshot(self, snapshot: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(snapshot.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            update_clause = ", ".join(
                f"{k} = excluded.{k}" for k in safe_cols if k != "id"
            )
            conn.execute(
                f"""
                INSERT INTO narrative_snapshots ({cols}) VALUES ({placeholders})
                ON CONFLICT(narrative_id, snapshot_date) DO UPDATE SET {update_clause}
                """,
                list(snapshot.values()),
            )

    def get_snapshot(self, narrative_id: str, snapshot_date: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM narrative_snapshots WHERE narrative_id = ? AND snapshot_date = ?",
                (narrative_id, snapshot_date),
            ).fetchone()
            return dict(row) if row else None

    def get_snapshots_range(
        self, narrative_id: str, start_date: str, end_date: str
    ) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM narrative_snapshots
                WHERE narrative_id = ? AND snapshot_date BETWEEN ? AND ?
                ORDER BY snapshot_date DESC
                """,
                (narrative_id, start_date, end_date),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_baseline_doc_rate(self, narrative_id: str, lookback_days: int = 7) -> float:
        """Returns average doc_count from recent snapshots for baseline calculation."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT doc_count FROM narrative_snapshots "
                "WHERE narrative_id = ? ORDER BY snapshot_date DESC LIMIT ?",
                (narrative_id, lookback_days),
            ).fetchall()
        if not rows or len(rows) < 2:
            return 0.0
        counts = [r["doc_count"] or 0 for r in rows]
        # Average change per snapshot interval
        changes = [abs(counts[i] - counts[i+1]) for i in range(len(counts)-1)]
        return sum(changes) / len(changes) if changes else 0.0

    def get_snapshot_history(self, narrative_id: str, days: int = 30) -> list:
        """Returns recent daily snapshots for a narrative, ordered by date descending."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM narrative_snapshots WHERE narrative_id = ? "
                "ORDER BY snapshot_date DESC LIMIT ?",
                (narrative_id, days),
            ).fetchall()
        return [dict(r) for r in rows]

    # --- Mutation Operations ---

    def save_mutation(self, mutation: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(mutation.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO mutation_events ({cols}) VALUES ({placeholders})",
                list(mutation.values()),
            )

    def get_mutations_today(self) -> list[dict]:
        today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM mutation_events WHERE detected_at >= ? ORDER BY detected_at DESC",
                (today,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_mutations_for_narrative(
        self, narrative_id: str, limit: int = 10
    ) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mutation_events
                WHERE narrative_id = ?
                ORDER BY detected_at DESC
                LIMIT ?
                """,
                (narrative_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_changelog_for_narrative(
        self, narrative_id: str, days: int = 30, *, limit: int = 0, offset: int = 0
    ) -> list[dict]:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=days)
        ).isoformat()
        with self._get_conn() as conn:
            sql = """
                SELECT * FROM mutation_events
                WHERE narrative_id = ? AND detected_at >= ?
                ORDER BY detected_at DESC
            """
            params: list = [narrative_id, cutoff]
            if limit > 0:
                sql += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def count_changelog_for_narrative(self, narrative_id: str, days: int = 30) -> int:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=days)
        ).isoformat()
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM mutation_events WHERE narrative_id = ? AND detected_at >= ?",
                (narrative_id, cutoff),
            ).fetchone()
            return int(row["cnt"]) if row else 0

    # --- Dashboard Operations ---

    def get_recent_pipeline_activity(self, limit: int = 20) -> list[dict]:
        """Return recent pipeline_run_log entries ordered by run_at desc."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_run_log ORDER BY run_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_dashboard_stats(self) -> dict:
        """Return aggregate stats for the dashboard sidebar."""
        with self._get_conn() as conn:
            today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()

            active_count = conn.execute(
                "SELECT COUNT(*) FROM narratives WHERE suppressed = 0"
            ).fetchone()[0]

            suppressed_count = conn.execute(
                "SELECT COUNT(*) FROM narratives WHERE suppressed = 1"
            ).fetchone()[0]

            buffer_count = conn.execute(
                "SELECT COUNT(*) FROM candidate_buffer WHERE status = 'pending'"
            ).fetchone()[0]

            haiku_calls_today = conn.execute(
                "SELECT COUNT(*) FROM llm_audit_log WHERE model LIKE '%haiku%' AND called_at >= ?",
                (today,)
            ).fetchone()[0]

            sonnet_calls_today = conn.execute(
                "SELECT COUNT(*) FROM llm_audit_log WHERE model LIKE '%sonnet%' AND called_at >= ?",
                (today,)
            ).fetchone()[0]

            cost_today_row = conn.execute(
                "SELECT COALESCE(SUM(cost_estimate_usd), 0) FROM llm_audit_log WHERE called_at >= ?",
                (today,)
            ).fetchone()
            cost_today = float(cost_today_row[0]) if cost_today_row else 0.0

            sonnet_spend = conn.execute(
                "SELECT total_tokens_used FROM sonnet_daily_spend WHERE date = ?",
                (today,)
            ).fetchone()
            sonnet_tokens_used = int(sonnet_spend[0]) if sonnet_spend else 0

            adversarial_count = conn.execute(
                "SELECT COUNT(*) FROM adversarial_log"
            ).fetchone()[0]

            last_run = conn.execute(
                "SELECT run_at FROM pipeline_run_log ORDER BY run_at DESC LIMIT 1"
            ).fetchone()
            last_run_at = last_run[0] if last_run else None

            docs_ingested_today = conn.execute(
                "SELECT COUNT(*) FROM candidate_buffer WHERE ingested_at >= ?",
                (today,)
            ).fetchone()[0]

            return {
                "active_narratives": active_count,
                "suppressed_narratives": suppressed_count,
                "buffer_count": buffer_count,
                "haiku_calls_today": haiku_calls_today,
                "sonnet_calls_today": sonnet_calls_today,
                "cost_today_usd": cost_today,
                "sonnet_tokens_used_today": sonnet_tokens_used,
                "adversarial_events": adversarial_count,
                "last_run_at": last_run_at,
                "docs_ingested_today": docs_ingested_today,
            }

    def get_centroid_history_dates(self, narrative_id: str) -> list[dict]:
        """Return centroid history dates for a narrative (no blob data)."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT date FROM centroid_history WHERE narrative_id = ? ORDER BY date ASC",
                (narrative_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_ns_score_history(self, narrative_id: str) -> list[dict]:
        """Return ns_score snapshots from centroid_history for sparkline."""
        # We don't store ns_score in centroid_history, so return date + current ns_score
        # as a placeholder; real history would require schema extension
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT date FROM centroid_history WHERE narrative_id = ? ORDER BY date ASC",
                (narrative_id,)
            ).fetchall()
            return [{"date": r[0]} for r in rows]

    def get_adversarial_log_for_narrative(self, narrative_id: str) -> list[dict]:
        """Return adversarial log entries for a narrative."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM adversarial_log WHERE narrative_id = ? ORDER BY detected_at DESC",
                (narrative_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_adversarial_events(self, narrative_id: str | None = None, limit: int = 50) -> list[dict]:
        """Return adversarial events, optionally filtered by narrative_id."""
        with self._get_conn() as conn:
            if narrative_id:
                rows = conn.execute(
                    "SELECT * FROM adversarial_log WHERE narrative_id = ? ORDER BY detected_at DESC LIMIT ?",
                    (narrative_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM adversarial_log ORDER BY detected_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]

    def get_llm_calls_for_narrative(self, narrative_id: str, *, limit: int = 0, offset: int = 0) -> list[dict]:
        """Return all LLM audit log entries for a narrative."""
        with self._get_conn() as conn:
            sql = "SELECT * FROM llm_audit_log WHERE narrative_id = ? ORDER BY called_at DESC"
            params: list = [narrative_id]
            if limit > 0:
                sql += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    # --- Stock Cache Operations ---

    def get_stock_cache(self, ticker: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM stock_cache WHERE ticker = ?", (ticker.upper(),)
            ).fetchone()
            return dict(row) if row else None

    def save_stock_cache(self, data: dict) -> None:
        import json
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO stock_cache
                    (ticker, name, price, change_pct, volume, market_cap, sector, industry,
                     sparkline_7d, sparkline_30d, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data["ticker"],
                data.get("name"),
                data.get("price"),
                data.get("change_pct"),
                data.get("volume"),
                data.get("market_cap"),
                data.get("sector"),
                data.get("industry"),
                json.dumps(data.get("sparkline_7d", [])),
                json.dumps(data.get("sparkline_30d", [])),
                data.get("updated_at"),
            ))

    def get_narratives_for_ticker(self, ticker: str) -> list[dict]:
        """Return all active narratives where ticker appears in linked_assets."""
        ticker_upper = ticker.upper()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT n.narrative_id, n.name, n.ns_score, n.stage
                FROM narratives n, json_each(n.linked_assets) AS je
                WHERE n.suppressed = 0
                  AND (
                    UPPER(json_extract(je.value, '$.ticker')) = ?
                    OR UPPER(je.value) = ?
                  )
                """,
                (ticker_upper, f'"{ticker_upper}"'),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_ticker_impact_score(self, ticker: str) -> float:
        """Sum of ns_scores for all narratives linked to this ticker."""
        narratives = self.get_narratives_for_ticker(ticker)
        return round(sum(n["ns_score"] or 0.0 for n in narratives), 4)

    # --- API Usage Tracking ---

    def get_api_usage(self, api_name: str, date: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM api_usage_log WHERE api_name = ? AND date = ?",
                (api_name, date),
            ).fetchone()
            return dict(row) if row else None

    def increment_api_usage(self, api_name: str, date: str, limit: int) -> None:
        import uuid as _uuid
        from datetime import datetime, timezone
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO api_usage_log (id, api_name, date, requests_used, requests_limit, last_request_at)
                VALUES (?, ?, ?, 1, ?, ?)
                ON CONFLICT(api_name, date) DO UPDATE SET
                    requests_used = requests_used + 1,
                    last_request_at = excluded.last_request_at
            """, (
                str(_uuid.uuid4()),
                api_name,
                date,
                limit,
                datetime.now(timezone.utc).isoformat(),
            ))

    # --- Portfolio Operations ---

    def get_portfolio_by_user(self, user_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM portfolios WHERE user_id = ? LIMIT 1", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def create_portfolio(self, portfolio: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(portfolio.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(f"INSERT INTO portfolios ({cols}) VALUES ({placeholders})", list(portfolio.values()))

    def add_portfolio_holding(self, holding: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(holding.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(f"INSERT INTO portfolio_holdings ({cols}) VALUES ({placeholders})", list(holding.values()))

    def get_portfolio_holding(self, holding_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT ph.*, p.user_id FROM portfolio_holdings ph "
                "JOIN portfolios p ON ph.portfolio_id = p.id "
                "WHERE ph.id = ?",
                (holding_id,),
            ).fetchone()
            return dict(row) if row else None

    def delete_portfolio_holding(self, holding_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM portfolio_holdings WHERE id = ?", (holding_id,))

    def get_portfolio_holdings(self, portfolio_id: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM portfolio_holdings WHERE portfolio_id = ?", (portfolio_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def update_portfolio_timestamp(self, portfolio_id: str) -> None:
        from datetime import datetime, timezone
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE portfolios SET updated_at = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), portfolio_id),
            )

    # --- Watchlist Operations ---

    def create_watchlist(self, watchlist: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(watchlist.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(f"INSERT INTO watchlists ({cols}) VALUES ({placeholders})", list(watchlist.values()))

    def get_watchlist(self, watchlist_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM watchlists WHERE id = ?", (watchlist_id,)).fetchone()
            return dict(row) if row else None

    def list_watchlists(self, user_id: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM watchlists WHERE user_id = ? ORDER BY created_at DESC", (user_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def add_watchlist_item(self, item: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(item.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(f"INSERT INTO watchlist_items ({cols}) VALUES ({placeholders})", list(item.values()))

    def get_watchlist_item(self, item_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT wi.*, w.user_id FROM watchlist_items wi "
                "JOIN watchlists w ON wi.watchlist_id = w.id "
                "WHERE wi.id = ?",
                (item_id,),
            ).fetchone()
            return dict(row) if row else None

    def delete_watchlist_item(self, item_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM watchlist_items WHERE id = ?", (item_id,))

    def get_watchlist_items(self, watchlist_id: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM watchlist_items WHERE watchlist_id = ? ORDER BY added_at ASC",
                (watchlist_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    # --- Notification Operations ---

    def create_notification_rule(self, rule: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(rule.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(f"INSERT INTO notification_rules ({cols}) VALUES ({placeholders})", list(rule.values()))

    def get_enabled_notification_rules(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM notification_rules WHERE enabled = 1"
            ).fetchall()
            return [dict(r) for r in rows]

    def list_notification_rules(self, user_id: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM notification_rules WHERE user_id = ? ORDER BY created_at DESC", (user_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def update_notification_rule_enabled(self, rule_id: str, enabled: bool) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE notification_rules SET enabled = ? WHERE id = ?",
                (1 if enabled else 0, rule_id),
            )

    def get_notification_rule(self, rule_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM notification_rules WHERE id = ?", (rule_id,),
            ).fetchone()
            return dict(row) if row else None

    def delete_notification_rule(self, rule_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM notification_rules WHERE id = ?", (rule_id,))

    def create_notification(self, notification: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(notification.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(f"INSERT INTO notifications ({cols}) VALUES ({placeholders})", list(notification.values()))

    def get_notifications(self, user_id: str, unread_only: bool = False) -> list[dict]:
        with self._get_conn() as conn:
            if unread_only:
                rows = conn.execute(
                    "SELECT * FROM notifications WHERE user_id = ? AND is_read = 0 ORDER BY created_at DESC LIMIT 200",
                    (user_id,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 200",
                    (user_id,)
                ).fetchall()
            return [dict(r) for r in rows]

    def get_notification(self, notification_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM notifications WHERE id = ?",
                (notification_id,),
            ).fetchone()
            return dict(row) if row else None

    def mark_notification_read(self, notification_id: str, user_id: str | None = None) -> None:
        with self._get_conn() as conn:
            if user_id is None:
                conn.execute(
                    "UPDATE notifications SET is_read = 1 WHERE id = ?",
                    (notification_id,),
                )
            else:
                conn.execute(
                    "UPDATE notifications SET is_read = 1 WHERE id = ? AND user_id = ?",
                    (notification_id, user_id),
                )

    def mark_all_notifications_read(self, user_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute("UPDATE notifications SET is_read = 1 WHERE user_id = ?", (user_id,))

    def has_notification_today(self, rule_id: str) -> bool:
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM notifications WHERE rule_id = ? AND created_at LIKE ? LIMIT 1",
                (rule_id, f"{today}%"),
            ).fetchone()
            return row is not None

    def get_notifications_since(self, user_id: str, since: "datetime") -> list[dict]:
        """Return notifications created after `since` for SSE delivery."""
        since_str = since.isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM notifications WHERE user_id = ? AND created_at > ? ORDER BY created_at ASC LIMIT 50",
                (user_id, since_str),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_dashboard_layout(self, user_id: str) -> dict | None:
        """Return saved dashboard layout for user, or None if not set."""
        import json as _json
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT layout_json FROM dashboard_layouts WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                return None
            try:
                return _json.loads(row[0])
            except Exception:
                return None

    def save_dashboard_layout(self, user_id: str, layout: dict) -> None:
        """Upsert dashboard layout for user."""
        import json as _json
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        layout_json = _json.dumps(layout)
        with self._get_conn() as conn:
            conn.execute(
                """INSERT INTO dashboard_layouts (user_id, layout_json, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET layout_json=excluded.layout_json, updated_at=excluded.updated_at""",
                (user_id, layout_json, now),
            )

    # --- Support Operations ---

    def get_narratives_created_on_date(self, date: str) -> list[dict]:
        """Return active narratives created on given YYYY-MM-DD."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM narratives WHERE suppressed = 0 AND created_at LIKE ?",
                (f"{date}%",)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_mutations_today_for_narrative(self, narrative_id: str) -> list[dict]:
        """Return mutations detected today for a specific narrative."""
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).date().isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM mutation_events WHERE narrative_id = ? AND detected_at >= ?",
                (narrative_id, today)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_narratives_by_date(self, date: str) -> list[dict]:
        """Return all active narratives for export (date reserved for future use)."""
        return self.get_all_active_narratives()

    # --- Analytics Operations ---

    def get_bulk_narrative_snapshots(self, cutoff_date: str) -> list[dict]:
        """Bulk fetch snapshots for active + recently-dormant narratives since cutoff_date."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT ns.narrative_id, ns.snapshot_date, ns.ns_score, ns.velocity,
                       ns.entropy, ns.cohesion, ns.polarization, ns.doc_count,
                       ns.lifecycle_stage, ns.burst_ratio,
                       ns.sentiment_mean, ns.sentiment_variance, ns.source_count,
                       ns.intent_weight, ns.cross_source_score, ns.weighted_source_score,
                       ns.centrality,
                       ns.velocity_windowed, ns.public_interest
                FROM narrative_snapshots ns
                INNER JOIN narratives n ON ns.narrative_id = n.narrative_id
                WHERE (n.stage != 'Dormant' OR n.last_updated_at >= ?)
                  AND n.suppressed = 0
                  AND ns.snapshot_date >= ?
                ORDER BY ns.narrative_id, ns.snapshot_date ASC
                """,
                (cutoff_date, cutoff_date),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_velocity_snapshots_bulk(self, cutoff_date: str) -> list[dict]:
        """Get velocity snapshots for all active narratives since cutoff_date."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT ns.narrative_id, ns.snapshot_date, ns.velocity
                FROM narrative_snapshots ns
                INNER JOIN narratives n ON ns.narrative_id = n.narrative_id
                WHERE n.suppressed = 0
                  AND ns.snapshot_date >= ?
                ORDER BY ns.narrative_id, ns.snapshot_date ASC
                """,
                (cutoff_date,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_document_overlaps(self) -> list[dict]:
        """Bulk query for narrative pairs sharing documents."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT a1.narrative_id AS nid1, a2.narrative_id AS nid2,
                       COUNT(DISTINCT a1.doc_id) AS shared_docs
                FROM narrative_assignments a1
                INNER JOIN narrative_assignments a2
                  ON a1.doc_id = a2.doc_id AND a1.narrative_id < a2.narrative_id
                GROUP BY a1.narrative_id, a2.narrative_id
                """,
            ).fetchall()
        return [dict(r) for r in rows]

    def get_doc_counts_per_narrative(self) -> dict[str, int]:
        """Count distinct documents per narrative from assignments table."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT narrative_id, COUNT(DISTINCT doc_id) AS doc_count "
                "FROM narrative_assignments GROUP BY narrative_id"
            ).fetchall()
        return {r["narrative_id"]: r["doc_count"] for r in rows}

    def get_stage_change_mutations(self, days: int = 30) -> list[dict]:
        """Get all stage_change mutations from the last N days."""
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=days)
        ).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT narrative_id, detected_at, previous_value, new_value, magnitude
                FROM mutation_events
                WHERE mutation_type = 'stage_change' AND detected_at >= ?
                ORDER BY narrative_id, detected_at ASC
                """,
                (cutoff,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_first_snapshot_dates(self) -> dict[str, str]:
        """Return {narrative_id: earliest_snapshot_date} for all narratives."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT narrative_id, MIN(snapshot_date) AS first_date "
                "FROM narrative_snapshots GROUP BY narrative_id"
            ).fetchall()
        return {r["narrative_id"]: r["first_date"] for r in rows}

    # --- Price Tick / Candle Operations ---

    def insert_ticks_batch(self, ticks: list[dict]) -> int:
        """Batch insert price ticks. Returns count of rows inserted."""
        if not ticks:
            return 0
        with self._get_conn() as conn:
            cursor = conn.executemany(
                """
                INSERT OR IGNORE INTO price_ticks (symbol, price, volume, timestamp, source)
                VALUES (:symbol, :price, :volume, :timestamp, :source)
                """,
                ticks,
            )
            return cursor.rowcount

    def get_recent_ticks(self, symbol: str, limit: int = 100) -> list[dict]:
        """Return the most recent ticks for a symbol, newest first."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT symbol, price, volume, timestamp, source "
                "FROM price_ticks WHERE symbol = ? "
                "ORDER BY timestamp DESC LIMIT ?",
                (symbol, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def aggregate_candles_1m(self, before_cutoff: str) -> int:
        """Aggregate raw ticks older than cutoff into 1-minute OHLCV candles.

        Uses window functions (FIRST_VALUE/LAST_VALUE) to avoid O(n²)
        correlated subqueries.
        """
        # NOTE: Small race window between aggregation and pruning where ticks could arrive
        # with timestamps older than cutoff. These would be pruned without aggregation.
        # Acceptable for current single-process architecture.
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT symbol, minute_ts, open_price, high, low, close_price, volume, source
                FROM (
                    SELECT
                        t.symbol,
                        strftime('%Y-%m-%dT%H:%M:00', t.timestamp) AS minute_ts,
                        FIRST_VALUE(t.price) OVER (
                            PARTITION BY t.symbol, strftime('%Y-%m-%dT%H:%M:00', t.timestamp)
                            ORDER BY t.timestamp ASC, t.id ASC
                        ) AS open_price,
                        MAX(t.price) OVER (
                            PARTITION BY t.symbol, strftime('%Y-%m-%dT%H:%M:00', t.timestamp)
                        ) AS high,
                        MIN(t.price) OVER (
                            PARTITION BY t.symbol, strftime('%Y-%m-%dT%H:%M:00', t.timestamp)
                        ) AS low,
                        FIRST_VALUE(t.price) OVER (
                            PARTITION BY t.symbol, strftime('%Y-%m-%dT%H:%M:00', t.timestamp)
                            ORDER BY t.timestamp DESC, t.id DESC
                        ) AS close_price,
                        SUM(COALESCE(t.volume, 0)) OVER (
                            PARTITION BY t.symbol, strftime('%Y-%m-%dT%H:%M:00', t.timestamp)
                        ) AS volume,
                        MAX(t.source) OVER (
                            PARTITION BY t.symbol, strftime('%Y-%m-%dT%H:%M:00', t.timestamp)
                        ) AS source,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.symbol, strftime('%Y-%m-%dT%H:%M:00', t.timestamp)
                            ORDER BY t.id ASC
                        ) AS rn
                    FROM price_ticks t
                    WHERE t.timestamp < ?
                ) sub
                WHERE rn = 1
                """,
                (before_cutoff,),
            ).fetchall()

            count = 0
            for r in rows:
                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO price_candles_1m
                            (symbol, open, high, low, close, volume, timestamp, source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            r["symbol"], r["open_price"], r["high"],
                            r["low"], r["close_price"], r["volume"],
                            r["minute_ts"], r["source"],
                        ),
                    )
                    count += 1
                except Exception:
                    pass
            return count

    def prune_old_ticks(self, cutoff: str) -> int:
        """Delete ticks older than cutoff. Returns count deleted."""
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM price_ticks WHERE timestamp < ?", (cutoff,)
            )
            return cursor.rowcount

    def get_candles_1m(self, symbol: str, start: str, end: str) -> list[dict]:
        """Return 1-minute candles for a symbol within a time range."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT symbol, open, high, low, close, volume, timestamp, source "
                "FROM price_candles_1m "
                "WHERE symbol = ? AND timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp",
                (symbol, start, end),
            ).fetchall()
        return [dict(r) for r in rows]

    # --- User Operations ---

    def get_user_by_id(self, user_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_user_by_email(self, email: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE email = ?", (email,)
            ).fetchone()
            return dict(row) if row else None

    def create_user(self, user: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(user.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO users ({cols}) VALUES ({placeholders})",
                list(user.values()),
            )

    def update_user_password_hash(self, user_id: str, new_hash: str) -> None:
        """Update a user's password hash (for migration from old bcrypt to pre-hashed bcrypt)."""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (new_hash, user_id),
            )

    def get_user_by_verification_token(self, token: str) -> dict | None:
        """Look up a user by their email verification token."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT id, email, email_verified, verification_token, created_at "
                "FROM users WHERE verification_token = ?",
                (token,),
            ).fetchone()
            return dict(row) if row else None

    def mark_email_verified(self, user_id: str) -> None:
        """Set email_verified=1 and clear the verification token."""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE users SET email_verified = 1, verification_token = NULL WHERE id = ?",
                (user_id,),
            )

    # --- Token Blacklist Operations ---

    def blacklist_token(self, jti: str, user_id: str, expires_at: str) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO token_blacklist (jti, user_id, blacklisted_at, expires_at) VALUES (?, ?, ?, ?)",
                (jti, user_id, datetime.datetime.now(datetime.timezone.utc).isoformat(), expires_at),
            )

    def is_token_blacklisted(self, jti: str) -> bool:
        with self._get_conn() as conn:
            row = conn.execute("SELECT 1 FROM token_blacklist WHERE jti = ?", (jti,)).fetchone()
            return row is not None

    def cleanup_expired_blacklist(self) -> int:
        with self._get_conn() as conn:
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            cursor = conn.execute("DELETE FROM token_blacklist WHERE expires_at < ?", (now,))
            return cursor.rowcount

    # --- Refresh Token Operations (M2) ---

    def store_refresh_token(self, jti: str, user_id: str, expires_at: str) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO refresh_tokens (jti, user_id, expires_at, created_at, revoked) VALUES (?, ?, ?, ?, 0)",
                (jti, user_id, expires_at, datetime.datetime.now(datetime.timezone.utc).isoformat()),
            )

    def get_refresh_token(self, jti: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM refresh_tokens WHERE jti = ?", (jti,)).fetchone()
            return dict(row) if row else None

    def revoke_refresh_token(self, jti: str) -> None:
        with self._get_conn() as conn:
            conn.execute("UPDATE refresh_tokens SET revoked = 1 WHERE jti = ?", (jti,))

    def revoke_all_user_refresh_tokens(self, user_id: str) -> int:
        with self._get_conn() as conn:
            cursor = conn.execute("UPDATE refresh_tokens SET revoked = 1 WHERE user_id = ? AND revoked = 0", (user_id,))
            return cursor.rowcount

    # --- Auth Audit Logging (M9) ---

    def log_auth_event(self, event: dict) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """INSERT INTO auth_audit_log
                (event_type, email, user_id, ip_address, user_agent, success, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event["event_type"],
                    event.get("email"),
                    event.get("user_id"),
                    event.get("ip_address"),
                    event.get("user_agent"),
                    1 if event.get("success") else 0,
                    event.get("details"),
                    event.get("created_at", datetime.datetime.now(datetime.timezone.utc).isoformat()),
                ),
            )

    # --- Tweet Log Operations ---

    def insert_tweet_log(self, data: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(data.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO tweet_log ({cols}) VALUES ({placeholders})",
                list(data.values()),
            )

    def get_last_tweet_for_narrative(self, narrative_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM tweet_log WHERE narrative_id = ? "
                "AND status = 'posted' AND tweet_id IS NOT NULL "
                "ORDER BY posted_at DESC LIMIT 1",
                (narrative_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_original_tweet_for_narrative(self, narrative_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM tweet_log WHERE narrative_id = ? "
                "AND status = 'posted' AND tweet_id IS NOT NULL "
                "AND tweet_type NOT IN ('thread_reply') "
                "ORDER BY posted_at ASC LIMIT 1",
                (narrative_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_tweet_count_today(self) -> int:
        today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM tweet_log "
                "WHERE status = 'posted' AND posted_at LIKE ?",
                (f"{today}%",),
            ).fetchone()
            return row[0] if row else 0

    def get_tweet_count_this_month(self) -> int:
        month_prefix = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m")
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM tweet_log "
                "WHERE status = 'posted' AND posted_at LIKE ?",
                (f"{month_prefix}%",),
            ).fetchone()
            return row[0] if row else 0

    def get_tweet_count_for_narrative_since(self, narrative_id: str, since_iso: str) -> int:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM tweet_log "
                "WHERE narrative_id = ? AND status = 'posted' "
                "AND posted_at >= ?",
                (narrative_id, since_iso),
            ).fetchone()
            return row[0] if row else 0

    # --- Signal Extraction Operations ---

    def upsert_narrative_signal(self, signal: dict) -> None:
        import json
        nar_id = signal.get("narrative_id")
        if not nar_id:
            return
        key_actors = signal.get("key_actors", [])
        affected_sectors = signal.get("affected_sectors", [])
        extracted_at = signal.get("extracted_at", "")
        if not extracted_at:
            from datetime import datetime, timezone
            extracted_at = datetime.now(timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO narrative_signals
                    (narrative_id, direction, confidence, timeframe, magnitude,
                     certainty, key_actors, affected_sectors, catalyst_type,
                     extracted_at, raw_response)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                nar_id,
                signal.get("direction", "neutral"),
                signal.get("confidence", 0.0),
                signal.get("timeframe", "unknown"),
                signal.get("magnitude", "incremental"),
                signal.get("certainty", "speculative"),
                json.dumps(key_actors) if isinstance(key_actors, list) else str(key_actors),
                json.dumps(affected_sectors) if isinstance(affected_sectors, list) else str(affected_sectors),
                signal.get("catalyst_type", "unknown"),
                extracted_at,
                signal.get("raw_response", ""),
            ))

    def get_narrative_signal(self, narrative_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM narrative_signals WHERE narrative_id = ?",
                (narrative_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_all_narrative_signals(self, *, limit: int = 0, offset: int = 0) -> list[dict]:
        sql = (
            "SELECT narrative_id, direction, confidence, timeframe, magnitude, certainty, "
            "key_actors, affected_sectors, catalyst_type, extracted_at "
            "FROM narrative_signals ORDER BY extracted_at DESC"
        )
        params: list = []
        if limit > 0:
            sql += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        with self._get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def get_narratives_by_ids(self, ids: list[str]) -> dict[str, dict]:
        """Bulk fetch narratives by ID. Chunked to stay under SQLite parameter limit."""
        if not ids:
            return {}
        result: dict[str, dict] = {}
        chunk_size = 900
        with self._get_conn() as conn:
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i : i + chunk_size]
                placeholders = ",".join("?" * len(chunk))
                rows = conn.execute(
                    f"SELECT * FROM narratives WHERE narrative_id IN ({placeholders})",
                    chunk,
                ).fetchall()
                for r in rows:
                    result[r["narrative_id"]] = dict(r)
        return result

    def get_adversarial_events_for_narratives(self, ids: list[str], limit_per: int = 10) -> dict[str, list]:
        """Bulk fetch adversarial events keyed by narrative_id."""
        if not ids:
            return {}
        rows_by_id: dict[str, list] = {nid: [] for nid in ids}
        chunk_size = 900
        with self._get_conn() as conn:
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i : i + chunk_size]
                placeholders = ",".join("?" * len(chunk))
                rows = conn.execute(
                    f"SELECT * FROM adversarial_log WHERE narrative_id IN ({placeholders}) "
                    "ORDER BY narrative_id, detected_at DESC",
                    chunk,
                ).fetchall()
                counts: dict[str, int] = {}
                for r in rows:
                    nid = r["narrative_id"]
                    if nid not in rows_by_id:
                        continue
                    if counts.get(nid, 0) < limit_per:
                        rows_by_id[nid].append(dict(r))
                        counts[nid] = counts.get(nid, 0) + 1
        return rows_by_id

    def get_snapshot_history_for_narratives(self, ids: list[str], days: int = 90) -> dict[str, list]:
        """Bulk fetch snapshot history keyed by narrative_id."""
        if not ids:
            return {}
        rows_by_id: dict[str, list] = {nid: [] for nid in ids}
        chunk_size = 900
        with self._get_conn() as conn:
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i : i + chunk_size]
                placeholders = ",".join("?" * len(chunk))
                rows = conn.execute(
                    f"SELECT * FROM narrative_snapshots WHERE narrative_id IN ({placeholders}) "
                    "ORDER BY narrative_id, snapshot_date DESC",
                    chunk,
                ).fetchall()
                counts: dict[str, int] = {}
                for r in rows:
                    nid = r["narrative_id"]
                    if nid not in rows_by_id:
                        continue
                    if counts.get(nid, 0) < days:
                        rows_by_id[nid].append(dict(r))
                        counts[nid] = counts.get(nid, 0) + 1
        return rows_by_id

    # --- Convergence Operations ---

    def upsert_ticker_convergence(self, data: dict) -> None:
        import json as _json
        ticker = data.get("ticker")
        if not ticker:
            return
        contributing = data.get("contributing_narrative_ids", [])
        if isinstance(contributing, list):
            contributing = _json.dumps(contributing)
        computed_at = data.get("computed_at", "")
        if not computed_at:
            computed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO ticker_convergence
                    (ticker, convergence_count, direction_agreement,
                     direction_consensus, weighted_confidence, source_diversity,
                     pressure_score, contributing_narrative_ids, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker,
                int(data.get("convergence_count", 0)),
                float(data.get("direction_agreement", 0.0)),
                float(data.get("direction_consensus", 0.0)),
                float(data.get("weighted_confidence", 0.0)),
                int(data.get("source_diversity", 0)),
                float(data.get("pressure_score", 0.0)),
                contributing,
                computed_at,
            ))

    def get_ticker_convergence(self, ticker: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM ticker_convergence WHERE ticker = ?",
                (ticker,),
            ).fetchone()
            return dict(row) if row else None

    def get_all_ticker_convergences(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM ticker_convergence").fetchall()
            return [dict(r) for r in rows]

    def get_top_convergences(self, limit: int = 20) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM ticker_convergence ORDER BY pressure_score DESC LIMIT ?",
                (max(1, limit),),
            ).fetchall()
            return [dict(r) for r in rows]

    def clear_ticker_convergences(self) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM ticker_convergence")

    def replace_ticker_convergences(self, convergences: dict[str, dict]) -> None:
        import json as _json
        import datetime as _dt
        now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute("DELETE FROM ticker_convergence")
            for ticker, data in convergences.items():
                contributing = data.get("contributing_narrative_ids", [])
                if isinstance(contributing, list):
                    contributing = _json.dumps(contributing)
                computed_at = data.get("computed_at") or now_iso
                conn.execute("""
                    INSERT INTO ticker_convergence
                        (ticker, convergence_count, direction_agreement,
                         direction_consensus, weighted_confidence, source_diversity,
                         pressure_score, contributing_narrative_ids, computed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ticker,
                    int(data.get("convergence_count", 0)),
                    float(data.get("direction_agreement", 0.0)),
                    float(data.get("direction_consensus", 0.0)),
                    float(data.get("weighted_confidence", 0.0)),
                    int(data.get("source_diversity", 0)),
                    float(data.get("pressure_score", 0.0)),
                    contributing,
                    computed_at,
                ))

    def check_all_orphans(self) -> dict[str, dict]:
        """Read-only FK precheck. Returns {relationship: {count, sample_ids}}."""
        checks = [
            ("portfolio_holdings.portfolio_id -> portfolios.id",
             "SELECT h.portfolio_id FROM portfolio_holdings h "
             "LEFT JOIN portfolios p ON h.portfolio_id = p.id "
             "WHERE p.id IS NULL"),
            ("watchlist_items.watchlist_id -> watchlists.id",
             "SELECT wi.watchlist_id FROM watchlist_items wi "
             "LEFT JOIN watchlists w ON wi.watchlist_id = w.id "
             "WHERE w.id IS NULL"),
            ("notifications.rule_id -> notification_rules.id",
             "SELECT n.rule_id FROM notifications n "
             "LEFT JOIN notification_rules nr ON n.rule_id = nr.id "
             "WHERE n.rule_id IS NOT NULL AND nr.id IS NULL"),
            ("document_evidence.narrative_id -> narratives.narrative_id",
             "SELECT de.narrative_id FROM document_evidence de "
             "LEFT JOIN narratives na ON de.narrative_id = na.narrative_id "
             "WHERE de.narrative_id IS NOT NULL AND na.narrative_id IS NULL"),
            ("narrative_snapshots.narrative_id -> narratives.narrative_id",
             "SELECT ns.narrative_id FROM narrative_snapshots ns "
             "LEFT JOIN narratives na ON ns.narrative_id = na.narrative_id "
             "WHERE na.narrative_id IS NULL"),
            ("mutation_events.narrative_id -> narratives.narrative_id",
             "SELECT me.narrative_id FROM mutation_events me "
             "LEFT JOIN narratives na ON me.narrative_id = na.narrative_id "
             "WHERE na.narrative_id IS NULL"),
            ("adversarial_log.narrative_id -> narratives.narrative_id",
             "SELECT al.narrative_id FROM adversarial_log al "
             "LEFT JOIN narratives na ON al.narrative_id = na.narrative_id "
             "WHERE al.narrative_id IS NOT NULL AND na.narrative_id IS NULL"),
            ("centroid_history.narrative_id -> narratives.narrative_id",
             "SELECT ch.narrative_id FROM centroid_history ch "
             "LEFT JOIN narratives na ON ch.narrative_id = na.narrative_id "
             "WHERE na.narrative_id IS NULL"),
            ("narrative_assignments.narrative_id -> narratives.narrative_id",
             "SELECT nas.narrative_id FROM narrative_assignments nas "
             "LEFT JOIN narratives na ON nas.narrative_id = na.narrative_id "
             "WHERE na.narrative_id IS NULL"),
        ]
        result: dict[str, dict] = {}
        with self._get_conn() as conn:
            for rel, sql in checks:
                try:
                    rows = conn.execute(sql + " LIMIT 10").fetchall()
                    count_row = conn.execute(
                        f"SELECT COUNT(*) FROM ({sql})"
                    ).fetchone()
                    count = count_row[0] if count_row else len(rows)
                    result[rel] = {
                        "count": count,
                        "sample_ids": [r[0] for r in rows],
                    }
                except Exception as exc:
                    result[rel] = {"count": -1, "error": str(exc), "sample_ids": []}
        return result

    # --- Impact Score Operations (Phase 6) ---

    def upsert_impact_score(self, data: dict) -> None:
        import json as _json
        narrative_id = data.get("narrative_id")
        ticker = data.get("ticker")
        if not narrative_id or not ticker:
            return
        signal_components = data.get("signal_components", {})
        if isinstance(signal_components, dict):
            signal_components = _json.dumps(signal_components)
        computed_at = data.get("computed_at", "")
        if not computed_at:
            computed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO impact_scores
                    (narrative_id, ticker, direction, impact_score, confidence,
                     time_horizon, signal_components, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(narrative_id, ticker) DO UPDATE SET
                    direction = excluded.direction,
                    impact_score = excluded.impact_score,
                    confidence = excluded.confidence,
                    time_horizon = excluded.time_horizon,
                    signal_components = excluded.signal_components,
                    computed_at = excluded.computed_at
            """, (
                narrative_id,
                ticker,
                data.get("direction", "neutral"),
                float(data.get("impact_score", 0.0)),
                float(data.get("confidence", 0.0)),
                data.get("time_horizon", ""),
                signal_components,
                computed_at,
            ))

    def get_impact_scores_for_narrative(self, narrative_id: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM impact_scores WHERE narrative_id = ? ORDER BY impact_score DESC",
                (narrative_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_impact_scores_for_ticker(self, ticker: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM impact_scores WHERE ticker = ? ORDER BY impact_score DESC",
                (ticker,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_top_impact_scores(self, limit: int = 20) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM impact_scores ORDER BY impact_score DESC LIMIT ?",
                (max(1, limit),),
            ).fetchall()
            return [dict(r) for r in rows]

    # --- Sentiment Timeseries Operations ---

    def insert_sentiment_record(self, ticker: str, scores: dict) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO sentiment_timeseries
                    (ticker, composite_score, news_component, social_component,
                     momentum_component, message_volume, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker,
                float(scores.get("composite_score") or 0),
                float(scores.get("news_component") or 0),
                float(scores.get("social_component") or 0),
                float(scores.get("momentum_component") or 0),
                int(scores.get("message_volume_24h") or 0),
                now,
            ))

    def get_sentiment_timeseries(self, ticker: str, hours: int = 168) -> list[dict]:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours)
        ).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM sentiment_timeseries WHERE ticker = ? AND recorded_at >= ?"
                " ORDER BY recorded_at DESC",
                (ticker, cutoff),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_latest_sentiment(self, ticker: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM sentiment_timeseries WHERE ticker = ?"
                " ORDER BY recorded_at DESC LIMIT 1",
                (ticker,),
            ).fetchone()
            return dict(row) if row else None

    def insert_social_mention(self, ticker: str, source: str, counts: dict) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO social_mentions (ticker, source, mention_count, bullish_count, bearish_count, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                ticker,
                source,
                int(counts.get("mention_count") or 0),
                int(counts.get("bullish_count") or 0),
                int(counts.get("bearish_count") or 0),
                now,
            ))

    def get_trending_tickers(self, hours: int = 24, limit: int = 10) -> list[dict]:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours)
        ).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT ticker,
                       SUM(mention_count)  AS total_mentions,
                       SUM(bullish_count)  AS total_bullish,
                       SUM(bearish_count)  AS total_bearish
                FROM social_mentions
                WHERE recorded_at >= ?
                GROUP BY ticker
                ORDER BY total_mentions DESC
                LIMIT ?
            """, (cutoff, max(1, limit))).fetchall()
            return [dict(r) for r in rows]

    # --- Feed Metadata Operations ---

    def get_feed_metadata(self, feed_url: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM feed_metadata WHERE feed_url = ?", (feed_url,)
            ).fetchone()
            return dict(row) if row else None

    def upsert_feed_metadata(self, feed_url: str, etag: str | None, last_modified: str | None, new_doc_count: int) -> None:
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_conn() as conn:
            existing = conn.execute(
                "SELECT consecutive_empty_cycles FROM feed_metadata WHERE feed_url = ?",
                (feed_url,),
            ).fetchone()
            if new_doc_count > 0:
                empty_cycles = 0
            else:
                empty_cycles = (existing["consecutive_empty_cycles"] + 1) if existing else 1
            conn.execute(
                """INSERT INTO feed_metadata (feed_url, etag, last_modified, last_fetched_at, consecutive_empty_cycles)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(feed_url) DO UPDATE SET
                       etag = excluded.etag,
                       last_modified = excluded.last_modified,
                       last_fetched_at = excluded.last_fetched_at,
                       consecutive_empty_cycles = excluded.consecutive_empty_cycles""",
                (feed_url, etag, last_modified, now_iso, empty_cycles),
            )
