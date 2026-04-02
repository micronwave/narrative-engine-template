"""
API-based ingesters: MarketAux, NewsData.io, Reddit.
All sources are opt-in via settings keys and feature flags.
Returns empty list silently when disabled or credentials absent.
"""

import calendar
import email.utils
import hashlib
import logging
import uuid
from datetime import date, datetime, timezone
from urllib.parse import urlparse

import requests

from ingester import RawDocument, is_financially_relevant
from repository import SqliteRepository
from settings import Settings

logger = logging.getLogger(__name__)


def _normalize_pubdate(raw: str) -> str:
    """Normalize a date string to UTC ISO8601. Mirrors ingester._parse_published_at logic."""
    # Try ISO8601 with timezone
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).isoformat()
        # No timezone — assume UTC
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except (ValueError, TypeError):
        pass
    # Try RFC 2822 (email-style dates)
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # Try dateutil as last resort
    try:
        from dateutil.parser import parse as du_parse
        dt = du_parse(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    logger.warning("[NewsData] Could not parse pubDate %r; using current UTC", raw)
    return datetime.now(timezone.utc).isoformat()


class ApiUsageTracker:
    """Tracks daily API quota usage in api_usage_log table."""

    def __init__(self, repository: SqliteRepository):
        self.repository = repository

    def can_use(self, api_name: str, limit: int) -> bool:
        """Returns True if today's usage is below limit."""
        usage = self.repository.get_api_usage(api_name, date.today().isoformat())
        if not usage:
            return True
        return usage["requests_used"] < limit

    def increment(self, api_name: str, limit: int) -> None:
        """Records one additional request for today."""
        self.repository.increment_api_usage(api_name, date.today().isoformat(), limit)


class MarketauxIngester:
    _API_URL = "https://api.marketaux.com/v1/news/all"

    def __init__(self, settings: Settings, repository: SqliteRepository):
        self.settings = settings
        self.repository = repository
        self.tracker = ApiUsageTracker(repository)

    def ingest(self) -> list[RawDocument]:
        if not self.settings.MARKETAUX_API_KEY or not self.settings.ENABLE_MARKETAUX:
            return []
        if not self.tracker.can_use("marketaux", self.settings.MARKETAUX_DAILY_LIMIT):
            logger.info("[MarketAux] Daily limit reached")
            return []
        try:
            resp = requests.get(self._API_URL, params={
                "api_token": self.settings.MARKETAUX_API_KEY,
                "language": "en",
                "limit": 50,
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            docs = []
            for article in data.get("data", []):
                text = f"{article.get('title', '')} {article.get('description', '')}".strip()
                if not text:
                    continue
                if not is_financially_relevant(text):
                    continue
                url = article.get("url", "")
                doc_id = hashlib.sha256(url.encode()).hexdigest()[:16] if url else str(uuid.uuid4())
                source = urlparse(url).netloc.replace("www.", "") if url else "marketaux.com"
                docs.append(RawDocument(
                    doc_id=doc_id,
                    raw_text=text,
                    source_url=url,
                    source_domain=source or "marketaux.com",
                    published_at=article.get("published_at", datetime.now(timezone.utc).isoformat()),
                    ingested_at=datetime.now(timezone.utc).isoformat(),
                    author=None,
                    raw_text_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                ))
            self.tracker.increment("marketaux", self.settings.MARKETAUX_DAILY_LIMIT)
            logger.info("[MarketAux] Ingested %d documents", len(docs))
            return docs
        except Exception as exc:
            logger.warning("[MarketAux] Error: %s", exc)
            return []


class NewsdataIngester:
    _API_URL = "https://newsdata.io/api/1/news"

    def __init__(self, settings: Settings, repository: SqliteRepository):
        self.settings = settings
        self.repository = repository
        self.tracker = ApiUsageTracker(repository)

    def ingest(self) -> list[RawDocument]:
        if not self.settings.NEWSDATA_API_KEY or not self.settings.ENABLE_NEWSDATA:
            return []
        if not self.tracker.can_use("newsdata", self.settings.NEWSDATA_DAILY_LIMIT):
            logger.info("[NewsData] Daily limit reached")
            return []
        try:
            resp = requests.get(self._API_URL, params={
                "apikey": self.settings.NEWSDATA_API_KEY,
                "category": "business",
                "language": "en",
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            docs = []
            for article in data.get("results", []):
                text = f"{article.get('title', '')} {article.get('content', '') or article.get('description', '')}".strip()
                if not text:
                    continue
                if not is_financially_relevant(text):
                    continue
                url = article.get("link", "")
                doc_id = hashlib.sha256(url.encode()).hexdigest()[:16] if url else str(uuid.uuid4())
                pub_raw = article.get("pubDate")
                published_at = _normalize_pubdate(pub_raw) if pub_raw else datetime.now(timezone.utc).isoformat()
                docs.append(RawDocument(
                    doc_id=doc_id,
                    raw_text=text,
                    source_url=url,
                    source_domain=article.get("source_id", "newsdata.io"),
                    published_at=published_at,
                    ingested_at=datetime.now(timezone.utc).isoformat(),
                    author=None,
                    raw_text_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                ))
            self.tracker.increment("newsdata", self.settings.NEWSDATA_DAILY_LIMIT)
            logger.info("[NewsData] Ingested %d documents", len(docs))
            return docs
        except Exception as exc:
            logger.warning("[NewsData] Error: %s", exc)
            return []


class RedditIngester:
    SUBREDDITS = ["wallstreetbets", "stocks", "investing", "options", "SecurityAnalysis"]
    MIN_UPVOTES = 10

    def __init__(self, settings: Settings, repository: SqliteRepository):
        self.settings = settings
        self.repository = repository
        self._reddit = None

    def _init_reddit(self) -> bool:
        if self._reddit:
            return True
        if not (self.settings.REDDIT_CLIENT_ID and self.settings.REDDIT_CLIENT_SECRET):
            return False
        try:
            import praw
            # prawcore logs full request bodies (including client_secret) at DEBUG.
            # Suppress to WARNING to prevent credential leakage in log files.
            logging.getLogger("prawcore").setLevel(logging.WARNING)
            self._reddit = praw.Reddit(
                client_id=self.settings.REDDIT_CLIENT_ID,
                client_secret=self.settings.REDDIT_CLIENT_SECRET,
                user_agent=self.settings.REDDIT_USER_AGENT,
            )
            return True
        except Exception as exc:
            logger.warning("[Reddit] Init error: %s", exc)
            return False

    def ingest(self) -> list[RawDocument]:
        if not self.settings.ENABLE_REDDIT or not self._init_reddit():
            return []
        docs = []
        for sub_name in self.SUBREDDITS:
            try:
                subreddit = self._reddit.subreddit(sub_name)
                for post in subreddit.hot(limit=self.settings.REDDIT_POSTS_PER_SUB):
                    if post.score < self.MIN_UPVOTES:
                        continue
                    flair = post.link_flair_text or ""
                    if "meme" in flair.lower():
                        continue
                    text = f"{post.title} {post.selftext}".strip() if post.selftext else post.title
                    truncated = text[:5000]
                    doc_id = hashlib.sha256(post.id.encode()).hexdigest()[:16]
                    docs.append(RawDocument(
                        doc_id=doc_id,
                        raw_text=truncated,
                        source_url=f"https://reddit.com{post.permalink}",
                        source_domain=f"reddit.com/r/{sub_name}",
                        published_at=datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                        ingested_at=datetime.now(timezone.utc).isoformat(),
                        author=None,
                        raw_text_hash=hashlib.sha256(truncated.encode("utf-8")).hexdigest(),
                    ))
            except Exception as exc:
                logger.warning("[Reddit] Error in r/%s: %s", sub_name, exc)
        logger.info("[Reddit] Ingested %d documents", len(docs))
        return docs


class ApiIngestionManager:
    """Orchestrates all API-based ingesters. Failures in one source don't affect others."""

    def __init__(self, settings: Settings, repository: SqliteRepository):
        self._ingesters = [
            MarketauxIngester(settings, repository),
            NewsdataIngester(settings, repository),
            RedditIngester(settings, repository),
        ]
        if settings.ENABLE_EDGAR and settings.EDGAR_EMAIL and settings.EDGAR_TICKERS:
            tickers = [t.strip() for t in settings.EDGAR_TICKERS.split(",") if t.strip()]
            if tickers:
                from ingester import EdgarIngester
                self._ingesters.append(EdgarIngester(
                    repository,
                    tickers=tickers,
                    company_name=settings.EDGAR_COMPANY_NAME,
                    email=settings.EDGAR_EMAIL,
                ))
                logger.info("[EDGAR] Wired EdgarIngester for %d tickers", len(tickers))
            else:
                logger.info("[EDGAR] ENABLE_EDGAR=True but EDGAR_TICKERS is empty — skipping")

    def ingest(self) -> list[RawDocument]:
        """Calls all ingesters and combines results."""
        all_docs: list[RawDocument] = []
        for ingester in self._ingesters:
            try:
                docs = ingester.ingest()
                all_docs.extend(docs)
            except Exception as exc:
                logger.error("[ApiIngestionManager] %s failed: %s", type(ingester).__name__, exc)
        return all_docs
