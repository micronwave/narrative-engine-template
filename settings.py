from pathlib import Path
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- LLM ---
    ANTHROPIC_API_KEY: str
    HAIKU_MODEL: str = "claude-haiku-4-5-20251001"
    SONNET_MODEL: str = "claude-sonnet-4-6"
    HAIKU_MAX_TOKENS: int = 512
    SONNET_MAX_TOKENS: int = 2048
    SONNET_DAILY_TOKEN_BUDGET: int = 200000
    LLM_DAILY_BUDGET_USD: float = 5.0

    # --- Embedding ---
    EMBEDDING_MODEL_NAME: str = "all-mpnet-base-v2"
    EMBEDDING_MODE: str = "dense"  # "dense" or "hybrid"

    # --- Narrative tracking ---
    CENTROID_ALPHA: float = 0.15
    NOISE_BUFFER_THRESHOLD: int = 300
    ASSIGNMENT_SIMILARITY_FLOOR: float = 0.55
    CONFIDENCE_ESCALATION_THRESHOLD: float = 0.35
    VELOCITY_WINDOW_DAYS: int = 7
    ENTROPY_VOCAB_WINDOW: int = 10
    HDBSCAN_MIN_CLUSTER_SIZE: int = 8  # For smaller datasets, try 5 and 3 respectively
    HDBSCAN_MIN_SAMPLES: int = 5  # For smaller datasets, try 5 and 3 respectively

    # --- Asset mapping ---
    ASSET_MAPPING_MIN_SIMILARITY: float = 0.60

    # --- LSH deduplication ---
    LSH_THRESHOLD: float = 0.85
    LSH_NUM_PERM: int = 128

    # --- Paths ---
    ASSET_LIBRARY_PATH: str = "./data/asset_library.pkl"
    SEC_EDGAR_EMAIL: str = "research@example.com"
    DB_PATH: str = "./data/narrative_engine.db"
    LSH_INDEX_PATH: str = "./data/lsh_index.pkl"
    FAISS_INDEX_PATH: str = "./data/faiss_index.pkl"

    # --- Ingestion ---
    SCRAPE_MAX_THREADS: int = 3
    SYNC_BURST_WINDOW_SECONDS: int = 600   # V3: lowered from 300 for cold-start data
    SYNC_BURST_MIN_SOURCES: int = 3        # V3: lowered from 5 for cold-start data

    # --- Optional API keys (all default to empty = disabled) ---
    MARKETAUX_API_KEY: str = ""
    NEWSDATA_API_KEY: str = ""
    REDDIT_CLIENT_ID: str = ""
    REDDIT_CLIENT_SECRET: str = ""
    REDDIT_USER_AGENT: str = "narrative_engine/1.0"

    # --- Finnhub (optional — enables live stock prices) ---
    FINNHUB_API_KEY: str = ""
    FINNHUB_CACHE_TTL_SECONDS: int = 60
    FINNHUB_REFRESH_INTERVAL_SECONDS: int = 300
    IMPACT_SCORE_REFRESH_SECONDS: int = 600
    ANALYTICS_BG_REFRESH_SECONDS: int = 14400

    # --- Additional data providers (Phase 2) ---
    TWELVE_DATA_API_KEY: str = ""
    COINGECKO_API_KEY: str = ""
    ENABLE_TWELVE_DATA: bool = False
    ENABLE_COINGECKO: bool = False
    WEBSOCKET_SYMBOLS_LIMIT: int = 50
    WEBSOCKET_FLUSH_INTERVAL_SECONDS: int = 5
    WEBSOCKET_RECONNECT_MAX_DELAY_SECONDS: int = 300
    PRICE_TICK_RETENTION_HOURS: int = 48

    # --- Cohesion EMA smoothing ---
    COHESION_EMA_ALPHA: float = 0.3   # weight for new-cycle cohesion measurement

    # --- Pipeline frequency / burst velocity (F2) ---
    PIPELINE_FREQUENCY_HOURS: int = 4
    BURST_VELOCITY_WINDOW_HOURS: int = 2
    BURST_VELOCITY_ALERT_RATIO: float = 3.0
    SIGNAL_EXTRACTION_STALENESS_HOURS: int = 24
    CONVERGENCE_INDEPENDENCE_THRESHOLD: float = 0.30

    # --- Phase 4: Catalyst anchoring ---
    FRED_API_KEY: str = ""
    CATALYST_LOOKFORWARD_DAYS: int = 14
    FRED_CACHE_TTL_HOURS: int = 6

    # --- Phase 5: Learned signal weights ---
    SIGNAL_MODEL_PATH: str = "./data/signal_model.pkl"
    SIGNAL_MODEL_RETRAIN_DAYS: int = 7
    SIGNAL_MIN_TRAINING_SAMPLES: int = 30

    # --- API feature flags ---
    ENABLE_MARKETAUX: bool = True
    ENABLE_NEWSDATA: bool = True
    ENABLE_REDDIT: bool = True
    ENABLE_EDGAR: bool = False
    EDGAR_EMAIL: str = ""
    EDGAR_COMPANY_NAME: str = "NarrativeIntelligenceEngine"
    EDGAR_TICKERS: str = ""  # comma-separated list, e.g. "AAPL,MSFT,NVDA"

    # TRADIER_API_KEY intentionally deferred. Options data integration planned for later phase.

    # --- X/Twitter automated posting ---
    # Optional: bot integration (not included in template)
    TWITTER_API_KEY: str = ""
    TWITTER_API_SECRET: str = ""
    TWITTER_ACCESS_TOKEN: str = ""
    TWITTER_ACCESS_TOKEN_SECRET: str = ""
    TWITTER_ENABLED: bool = False  # Optional: bot integration (not included in template)
    TWITTER_DAILY_BUDGET: int = 12

    # --- Typefully (posts to X without X API credits) ---
    # Optional: bot integration (not included in template)
    TYPEFULLY_API_KEY: str = ""
    TYPEFULLY_SOCIAL_SET_ID: str = ""
    TYPEFULLY_ENABLED: bool = False  # Optional: bot integration (not included in template)
    TYPEFULLY_DAILY_BUDGET: int = 1
    TYPEFULLY_MONTHLY_BUDGET: int = 15

    # --- Discord webhook (draft queue for manual posting) ---
    DISCORD_WEBHOOK_URL: str = ""
    DISCORD_WEBHOOK_ENABLED: bool = False

    # --- SMTP / Email channel ---
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_FROM: str = ""
    SMTP_TO: str = ""
    SMTP_PASSWORD: str = ""

    # --- Generic webhook channel ---
    NOTIFICATION_WEBHOOK_URL: str = ""

    # --- Environment ---
    ENVIRONMENT: str = "development"

    # --- Auth ---
    AUTH_MODE: str = "stub"
    JWT_SECRET_KEY: str = ""
    JWT_EXPIRY_HOURS: int = 24

    # --- API rate limits ---
    MARKETAUX_DAILY_LIMIT: int = 100
    NEWSDATA_DAILY_LIMIT: int = 200
    REDDIT_POSTS_PER_SUB: int = 50
    TRUSTED_DOMAINS: list[str] = [
        "reuters.com",
        "apnews.com",
        "bloomberg.com",
        "wsj.com",
        "ft.com",
    ]

    # --- Validators ---

    @field_validator("ANTHROPIC_API_KEY")
    @classmethod
    def api_key_must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError(
                "ANTHROPIC_API_KEY must not be empty — system cannot start. "
                "Set it in your .env file."
            )
        return v

    @field_validator(
        "CENTROID_ALPHA",
        "ASSIGNMENT_SIMILARITY_FLOOR",
        "CONFIDENCE_ESCALATION_THRESHOLD",
        "LSH_THRESHOLD",
    )
    @classmethod
    def must_be_exclusive_unit_interval(cls, v: float, info) -> float:
        if not (0.0 < v < 1.0):
            raise ValueError(
                f"{info.field_name} must be in (0, 1) exclusive, got {v}"
            )
        return v

    @field_validator("NOISE_BUFFER_THRESHOLD", "SONNET_DAILY_TOKEN_BUDGET")
    @classmethod
    def must_be_positive_int(cls, v: int, info) -> int:
        if v <= 0:
            raise ValueError(
                f"{info.field_name} must be a positive integer, got {v}"
            )
        return v

    @field_validator("HDBSCAN_MIN_CLUSTER_SIZE", "HDBSCAN_MIN_SAMPLES")
    @classmethod
    def hdbscan_params_minimum(cls, v: int, info) -> int:
        if v < 2:
            raise ValueError(f"{info.field_name} must be >= 2, got {v}")
        return v

    @field_validator("LSH_NUM_PERM")
    @classmethod
    def lsh_num_perm_minimum(cls, v: int) -> int:
        if v < 64:
            raise ValueError(
                f"LSH_NUM_PERM must be >= 64 for acceptable accuracy, got {v}"
            )
        return v

    @field_validator("EMBEDDING_MODE")
    @classmethod
    def embedding_mode_must_be_valid(cls, v: str) -> str:
        if v not in ("dense", "hybrid"):
            raise ValueError(
                f"EMBEDDING_MODE must be 'dense' or 'hybrid', got '{v}'"
            )
        return v

    @field_validator("AUTH_MODE")
    @classmethod
    def auth_mode_must_be_valid(cls, v: str) -> str:
        if v not in ("stub", "jwt"):
            raise ValueError(
                f"AUTH_MODE must be 'stub' or 'jwt', got '{v}'"
            )
        return v

    @field_validator("JWT_SECRET_KEY")
    @classmethod
    def jwt_secret_must_be_strong(cls, v: str, info) -> str:
        if info.data.get("AUTH_MODE") == "jwt" and len(v.strip()) < 32:
            raise ValueError("JWT_SECRET_KEY must be at least 32 non-whitespace characters in JWT mode")
        return v


def ensure_data_dirs(s: Settings) -> None:
    """Create parent directories for all configured file paths if they don't exist."""
    for path_str in [
        s.DB_PATH,
        s.LSH_INDEX_PATH,
        s.FAISS_INDEX_PATH,
        s.ASSET_LIBRARY_PATH,
    ]:
        Path(path_str).parent.mkdir(parents=True, exist_ok=True)


_settings: Settings | None = None


def get_settings() -> Settings:
    """Lazy singleton — only instantiates on first access, not at import time."""
    global _settings
    if _settings is None:
        _settings = Settings()
        ensure_data_dirs(_settings)
    return _settings


def __getattr__(name: str):
    """Allow `from settings import settings` to work via lazy init."""
    if name == "settings":
        return get_settings()
    raise AttributeError(f"module 'settings' has no attribute {name!r}")
