import datetime
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
    def get_all_active_narratives(self) -> list[dict]:
        """Get all non-suppressed narratives."""
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
    def get_candidate_buffer(self, status: str = "pending") -> list[dict]:
        """Get all candidates with the given status."""
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
    def get_centroid_history(self, narrative_id: str, days: int) -> list[dict]:
        """Get centroid history for a narrative, most recent first."""
        ...

    @abstractmethod
    def get_latest_centroid(self, narrative_id: str) -> bytes | None:
        """Get the most recent centroid blob for a narrative."""
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
    def get_changelog_for_narrative(self, narrative_id: str, days: int = 30) -> list[dict]:
        """Get mutations for a narrative within the last N days, for changelog."""
        ...

    # --- Document Evidence Operations ---

    @abstractmethod
    def insert_document_evidence(self, evidence: dict) -> None:
        """Store supporting evidence for a narrative."""
        ...

    @abstractmethod
    def get_document_evidence(self, narrative_id: str) -> list[dict]:
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

    # --- Chat Operations ---

    @abstractmethod
    def create_chat_session(self, session: dict) -> None:
        """Insert a new chat session."""
        ...

    @abstractmethod
    def get_chat_session(self, session_id: str) -> dict | None:
        """Get chat session by id."""
        ...

    @abstractmethod
    def list_chat_sessions(self, user_id: str, limit: int = 20) -> list[dict]:
        """List user's sessions ordered by updated_at DESC."""
        ...

    @abstractmethod
    def update_chat_session_timestamp(self, session_id: str) -> None:
        """Update session's updated_at to now."""
        ...

    @abstractmethod
    def save_chat_message(self, message: dict) -> None:
        """Insert a chat message."""
        ...

    @abstractmethod
    def get_chat_messages(self, session_id: str) -> list[dict]:
        """Get all messages for session ordered by created_at ASC."""
        ...

    @abstractmethod
    def delete_chat_session(self, session_id: str) -> None:
        """Delete session and all its messages."""
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
    def mark_notification_read(self, notification_id: str) -> None: ...

    @abstractmethod
    def mark_all_notifications_read(self, user_id: str) -> None: ...

    @abstractmethod
    def has_notification_today(self, rule_id: str) -> bool:
        """Check if a notification was already created today for this rule."""
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
    def get_all_narrative_signals(self) -> list[dict]:
        """Get all signal extractions."""
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


class SqliteRepository(Repository):
    # TODO SCALE: swap SqliteRepository for PostgresRepository (psycopg2) on AWS RDS — interface is identical

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
        conn.execute("PRAGMA busy_timeout=5000")
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
        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
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
                    consecutive_declining_days INTEGER DEFAULT 0
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
                CREATE TABLE IF NOT EXISTS chat_sessions (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL DEFAULT 'local',
                    narrative_id TEXT,
                    ticker TEXT,
                    title TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    is_active INTEGER DEFAULT 1
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    tokens_used INTEGER DEFAULT 0,
                    cost REAL DEFAULT 0,
                    created_at TEXT
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

            # Phase 3 Signal Redesign: convergence exposure column on narratives
            for col, coltype in [
                ("convergence_exposure", "REAL DEFAULT NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE narratives ADD COLUMN {col} {coltype}")
                except Exception:
                    pass

    # --- Narrative Operations ---

    def get_narrative(self, narrative_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM narratives WHERE narrative_id = ?", (narrative_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_all_active_narratives(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM narratives WHERE suppressed = 0"
            ).fetchall()
            return [dict(r) for r in rows]

    def insert_narrative(self, narrative: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(narrative.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO narratives ({cols}) VALUES ({placeholders})",
                list(narrative.values()),
            )

    def update_narrative(self, narrative_id: str, updates: dict) -> None:
        if not updates:
            return
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(updates.keys())
            set_clause = ", ".join(f"{k} = ?" for k in safe_cols)
            values = list(updates.values()) + [narrative_id]
            conn.execute(
                f"UPDATE narratives SET {set_clause} WHERE narrative_id = ?",
                values,
            )

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

    def get_candidate_buffer(self, status: str = "pending") -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM candidate_buffer WHERE status = ?", (status,)
            ).fetchall()
            return [dict(r) for r in rows]

    def insert_candidate(self, candidate: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(candidate.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO candidate_buffer ({cols}) VALUES ({placeholders})",
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

    def get_centroid_history(self, narrative_id: str, days: int) -> list[dict]:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        ).isoformat()[:10]  # date portion
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM centroid_history
                WHERE narrative_id = ? AND date >= ?
                ORDER BY date DESC
                """,
                (narrative_id, cutoff),
            ).fetchall()
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

    def check_and_deduct_sonnet_budget(self, date: str, estimated_tokens: int, budget: int) -> bool:
        """Atomically check and deduct budget. Returns True if within budget."""
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO sonnet_daily_spend (date, total_tokens_used, total_calls) VALUES (?, 0, 0)",
                (date,),
            )
            cursor = conn.execute(
                """
                UPDATE sonnet_daily_spend
                SET total_tokens_used = total_tokens_used + ?, total_calls = total_calls + 1
                WHERE date = ? AND total_tokens_used + ? < ?
                """,
                (estimated_tokens, date, estimated_tokens, budget),
            )
            return cursor.rowcount > 0

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

    def get_document_evidence(self, narrative_id: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM document_evidence WHERE narrative_id = ? ORDER BY published_at DESC",
                (narrative_id,),
            ).fetchall()
            return [dict(r) for r in rows]

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
        self, narrative_id: str, days: int = 30
    ) -> list[dict]:
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=days)
        ).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM mutation_events
                WHERE narrative_id = ? AND detected_at >= ?
                ORDER BY detected_at DESC
                """,
                (narrative_id, cutoff),
            ).fetchall()
            return [dict(r) for r in rows]

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

    def get_llm_calls_for_narrative(self, narrative_id: str) -> list[dict]:
        """Return all LLM audit log entries for a narrative."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM llm_audit_log WHERE narrative_id = ? ORDER BY called_at DESC",
                (narrative_id,)
            ).fetchall()
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

    # --- Chat Operations ---

    def create_chat_session(self, session: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(session.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO chat_sessions ({cols}) VALUES ({placeholders})",
                list(session.values()),
            )

    def get_chat_session(self, session_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM chat_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_chat_sessions(self, user_id: str, limit: int = 20) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM chat_sessions WHERE user_id = ? AND is_active = 1 "
                "ORDER BY updated_at DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def update_chat_session_timestamp(self, session_id: str) -> None:
        from datetime import datetime, timezone
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE chat_sessions SET updated_at = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), session_id),
            )

    def save_chat_message(self, message: dict) -> None:
        with self._get_conn() as conn:
            safe_cols = self._sanitize_columns(message.keys())
            cols = ", ".join(safe_cols)
            placeholders = ", ".join("?" * len(safe_cols))
            conn.execute(
                f"INSERT INTO chat_messages ({cols}) VALUES ({placeholders})",
                list(message.values()),
            )

    def get_chat_messages(self, session_id: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM chat_messages WHERE session_id = ? ORDER BY created_at ASC",
                (session_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_chat_session(self, session_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM chat_messages WHERE session_id = ?", (session_id,))
            conn.execute("DELETE FROM chat_sessions WHERE id = ?", (session_id,))

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

    def mark_notification_read(self, notification_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute("UPDATE notifications SET is_read = 1 WHERE id = ?", (notification_id,))

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
                       ns.intent_weight, ns.cross_source_score, ns.centrality,
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

    def get_all_narrative_signals(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM narrative_signals").fetchall()
            return [dict(r) for r in rows]

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
