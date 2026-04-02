"""
This system ingests only publicly available data. Source attribution metadata
is mandatory on all output objects for Fair Use compliance. Do not strip attribution.
"""

import calendar
import email.utils
import hashlib
import html as html_module
import logging
import re
import time
import uuid
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import feedparser

from repository import Repository
from robots import can_fetch

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core type
# ---------------------------------------------------------------------------

@dataclass
class RawDocument:
    doc_id: str              # UUID generated at ingestion
    raw_text: str
    source_url: str
    source_domain: str
    published_at: str        # ISO8601
    ingested_at: str         # ISO8601
    author: str | None = None
    raw_text_hash: str = ""  # SHA256 of raw_text, computed after creation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def _extract_domain(url: str) -> str:
    return urlparse(url).netloc or url


# ---------------------------------------------------------------------------
# Financial relevance filter
# ---------------------------------------------------------------------------

_TIER1_FINANCIAL: frozenset[str] = frozenset([
    # Markets & instruments
    "market", "stock", "share", "equity", "bond", "yield", "etf", "index",
    "futures", "options", "derivative", "commodity", "currency", "forex",
    "crypto", "bitcoin", "coin", "token",
    # Macro & policy
    "economy", "economic", "gdp", "inflation", "recession", "deflation",
    "interest rate", "federal reserve", "central bank", "fed ", "ecb",
    "monetary", "fiscal", "treasury", "deficit", "surplus", "debt",
    "tariff", "trade", "sanction", "embargo", "export", "import",
    # Corporate
    "earnings", "revenue", "profit", "loss", "ipo", "merger", "acquisition",
    "dividend", "buyback", "valuation", "balance sheet", "cash flow",
    "bank", "finance", "financial", "investment", "investor", "hedge fund",
    "private equity", "venture capital", "startup",
    # Sectors with market-moving potential
    "oil", "energy", "gas", "pipeline", "opec", "refinery",
    "semiconductor", "chip", "ai ", "artificial intelligence",
    "housing", "mortgage", "real estate", "reit",
    "manufacturing", "supply chain", "logistics",
    "unemployment", "jobs", "labor", "wage",
])

_TIER2_GEOPOLITICAL: frozenset[str] = frozenset([
    "war", "conflict", "military", "sanction", "geopolit", "crisis",
    "strait", "blockade", "escalat",
])

# Full set preserved for backward compatibility
_FINANCIAL_KEYWORDS: frozenset[str] = _TIER1_FINANCIAL | _TIER2_GEOPOLITICAL


def is_financially_relevant(text: str) -> bool:
    """Return True if the text contains a Tier 1 financial keyword, or a Tier 2
    keyword co-occurring with a Tier 1 keyword. Tier 2-only articles are rejected."""
    lower = text.lower()
    has_tier1 = any(kw in lower for kw in _TIER1_FINANCIAL)
    if has_tier1:
        return True
    # Tier 2 alone is not enough
    return False


def _backoff_seconds(retry_count: int) -> int:
    return min(300, 60 * (2 ** retry_count))


def _log_failed_job(
    repository: Repository,
    source_url: str,
    source_type: str,
    error_message: str,
    retry_count: int = 0,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    backoff = _backoff_seconds(retry_count)
    next_retry_at = (
        (datetime.now(timezone.utc) + timedelta(seconds=backoff)).isoformat()
        if retry_count < 3
        else None
    )
    repository.insert_failed_job({
        "job_id": str(uuid.uuid4()),
        "source_url": source_url,
        "source_type": source_type,
        "error_message": error_message,
        "retry_count": retry_count,
        "next_retry_at": next_retry_at,
        "created_at": now,
    })


def _parse_published_at(entry: dict, fallback: str) -> str:
    """Extract ISO8601 published_at from a feedparser entry, falling back to ingested_at."""
    parsed_time = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed_time:
        try:
            ts = calendar.timegm(parsed_time)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            pass
    # feedparser sets published_parsed=None when it can't parse the date string.
    # Try RFC 2822 parse (email.utils handles the most common RSS date formats)
    # before giving up — returning raw would break ISO8601 contract for Phase 3.
    raw = entry.get("published") or entry.get("updated")
    if raw:
        try:
            dt = email.utils.parsedate_to_datetime(raw)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    logger.warning("Could not determine published_at for entry; using ingested_at as fallback")
    return fallback


_SCRIPT_STYLE_RE = re.compile(
    r'<(script|style)[^>]*>.*?</\1>', re.DOTALL | re.IGNORECASE
)
_HTML_TAG_RE = re.compile(r'<[^>]+>')


def _strip_html(text: str) -> str:
    text = _SCRIPT_STYLE_RE.sub('', text)   # Remove script/style blocks with content
    text = _HTML_TAG_RE.sub('', text)        # Remove remaining tags
    return html_module.unescape(text)        # Decode &amp; &lt; etc.


def _entry_text(entry: dict) -> str:
    """Extract the best available text from a feedparser entry, with HTML stripped."""
    content_list = entry.get("content")
    if content_list and isinstance(content_list, list):
        value = content_list[0].get("value", "")
        if value:
            return _strip_html(value)
    summary = entry.get("summary", "")
    title = entry.get("title", "")
    if summary:
        return _strip_html(f"{title}\n\n{summary}".strip())
    return title


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class Ingester(ABC):
    """
    Abstract interface for data ingestion.
    All implementations must check robots.txt via can_fetch() before HTTP requests.
    """

    @abstractmethod
    def ingest(self) -> list[RawDocument]:
        """
        Fetch and return raw documents.
        Must populate all required RawDocument fields.
        Must respect robots.txt.
        """
        ...


# ---------------------------------------------------------------------------
# RssIngester
# ---------------------------------------------------------------------------

class RssIngester(Ingester):

    _DEFAULT_FEEDS: list[str] = [
        # General financial news
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "https://finance.yahoo.com/rss/",
        "https://feeds.bloomberg.com/markets/news.rss",
        "https://www.ft.com/rss/home",
        "https://feeds.marketwatch.com/marketwatch/topstories",
        "https://seekingalpha.com/feed.xml",
        "https://www.investing.com/rss/news.rss",
        # Crypto / fintech
        "https://cointelegraph.com/rss",
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        # Central banks & macro
        "https://www.federalreserve.gov/feeds/press_all.xml",
        "https://www.ecb.europa.eu/rss/press.html",
        # Tech (market-moving)
        "https://feeds.arstechnica.com/arstechnica/technology-lab",
        "https://www.theverge.com/rss/index.xml",
        # Geopolitical & world news
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "https://feeds.washingtonpost.com/rss/world",
        "https://www.aljazeera.com/xml/rss/all.xml",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
        "https://www.theguardian.com/world/rss",
        "https://www.theguardian.com/business/rss",
        "https://feeds.npr.org/1004/rss.xml",
        "https://feeds.npr.org/1006/rss.xml",
        # Energy & commodities
        "https://oilprice.com/rss/main",
        "https://www.eia.gov/rss/todayinenergy.xml",
        # Earnings & corporate PR
        "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
        # Real estate
        "https://www.housingwire.com/feed/",
        # Tech deep dive
        "https://techcrunch.com/feed/",
        "https://www.wired.com/feed/rss",
        "https://siliconangle.com/feed/",
        # Crypto expanded
        "https://decrypt.co/feed",
        "https://bitcoinmagazine.com/.rss/full/",
        "https://thedefiant.io/feed",
        # Skipped — Benzinga: SSL EOF, TheFly: malformed XML
        # Skipped — UPI ×2: malformed XML (undefined entity)
        # Skipped — AP News: malformed XML
        # Skipped — S&P Global commodities: malformed XML
        # Skipped — Treasury RSS: malformed XML
        # Skipped — SEC EDGAR atom: malformed XML
        # Skipped — IMF: returns HTML not XML
        # Skipped — Zacks: malformed XML
        # Skipped — Motley Fool: robots.txt blocked
        # Skipped — StockAnalysis: malformed XML
        # Skipped — BusinessWire: connection timeout
        # Skipped — TheRealDeal: returns HTML not XML
    ]

    def __init__(
        self,
        repository: Repository,
        feed_urls: list[str] | None = None,
    ) -> None:
        self._repository = repository
        self._feed_urls: list[str] = (
            feed_urls if feed_urls is not None else list(self._DEFAULT_FEEDS)
        )

    def ingest(self) -> list[RawDocument]:
        docs: list[RawDocument] = []
        ingested_at = datetime.now(timezone.utc).isoformat()

        for feed_url in self._feed_urls:
            if not can_fetch(feed_url, self._repository):
                logger.info("robots.txt disallows %s — skipping feed", feed_url)
                continue

            # Conditional HTTP: check stored metadata for adaptive polling
            feed_meta = self._repository.get_feed_metadata(feed_url)
            if feed_meta:
                empty_cycles = feed_meta.get("consecutive_empty_cycles") or 0
                last_fetched = feed_meta.get("last_fetched_at")
                if empty_cycles >= 3 and last_fetched:
                    try:
                        fetched_dt = datetime.fromisoformat(last_fetched)
                        if datetime.now(timezone.utc) - fetched_dt < timedelta(hours=12):
                            logger.debug("Skipping stale feed (empty %d cycles): %s", empty_cycles, feed_url)
                            continue
                    except (ValueError, TypeError):
                        pass

            try:
                req = urllib.request.Request(
                    feed_url,
                    headers={"User-Agent": "NarrativeIntelligenceBot/1.0"},
                )
                # Add conditional HTTP headers
                if feed_meta:
                    if feed_meta.get("etag"):
                        req.add_header("If-None-Match", feed_meta["etag"])
                    if feed_meta.get("last_modified"):
                        req.add_header("If-Modified-Since", feed_meta["last_modified"])

                resp_etag = None
                resp_last_modified = None
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        feed_bytes = resp.read(10 * 1024 * 1024)  # 10 MB cap
                        resp_etag = resp.headers.get("ETag")
                        resp_last_modified = resp.headers.get("Last-Modified")
                except urllib.error.HTTPError as http_err:
                    if http_err.code == 304:
                        logger.debug("304 Not Modified for %s", feed_url)
                        continue  # conditional HTTP working — no metadata update needed
                    raise

                parsed = feedparser.parse(feed_bytes)
            except Exception as exc:
                logger.error("feedparser raised for %s: %s", feed_url, exc)
                _log_failed_job(self._repository, feed_url, "rss", str(exc))
                continue

            if parsed.bozo and not parsed.entries:
                exc_msg = str(getattr(parsed, "bozo_exception", "malformed feed"))
                logger.error("Feed error for %s: %s", feed_url, exc_msg)
                _log_failed_job(self._repository, feed_url, "rss", exc_msg)
                continue

            if parsed.bozo:
                logger.warning(
                    "Bozo feed (partially parseable) %s: %s",
                    feed_url,
                    getattr(parsed, "bozo_exception", ""),
                )

            pre_feed_count = len(docs)

            for entry in parsed.entries:
                link = entry.get("link", "")
                if not link:
                    continue

                raw_text = _entry_text(entry)
                if not raw_text.strip():
                    continue

                source_domain = _extract_domain(link)

                # Enrich short excerpts (paywalled/headline-only sources) with
                # domain + category metadata so embeddings have more signal.
                if len(raw_text) < 200:
                    title = entry.get("title", "")
                    tags_list = entry.get("tags") or []
                    section = tags_list[0].get("term", "") if tags_list else ""
                    parts = [source_domain]
                    if section:
                        parts.append(section)
                    parts.append(title or raw_text)
                    raw_text = ": ".join(parts)

                if not is_financially_relevant(raw_text):
                    logger.debug("Skipping non-financial article: %.80s", raw_text)
                    continue
                published_at = _parse_published_at(entry, ingested_at)
                doc_id = str(uuid.uuid4())

                doc = RawDocument(
                    doc_id=doc_id,
                    raw_text=raw_text,
                    source_url=link,
                    source_domain=source_domain,
                    published_at=published_at,
                    ingested_at=ingested_at,
                    author=entry.get("author") or None,
                    raw_text_hash=_compute_hash(raw_text),
                )
                docs.append(doc)

            # Update feed metadata with conditional HTTP info
            feed_doc_count = len(docs) - pre_feed_count
            self._repository.upsert_feed_metadata(feed_url, resp_etag, resp_last_modified, feed_doc_count)

        return docs


# ---------------------------------------------------------------------------
# EdgarIngester
# ---------------------------------------------------------------------------

class EdgarIngester(Ingester):

    _SEC_BASE_URL = "https://www.sec.gov/"
    _SEC_DOMAIN = "www.sec.gov"
    _TICKER_RE = re.compile(r'^[A-Za-z0-9.\-]+$')

    def __init__(
        self,
        repository: Repository,
        tickers: list[str],
        forms: list[str] | None = None,
        company_name: str = "NarrativeIntelligenceEngine",
        email: str = "narrative@example.com",
        download_dir: str = "./data/edgar",
    ) -> None:
        self._repository = repository
        self._tickers = tickers
        self._forms = forms or ["10-K", "10-Q", "8-K"]
        self._company_name = company_name
        self._email = email
        self._download_dir = Path(download_dir)

    def ingest(self) -> list[RawDocument]:
        if not can_fetch(self._SEC_BASE_URL, self._repository):
            logger.info("robots.txt disallows %s — skipping EdgarIngester", self._SEC_DOMAIN)
            return []

        try:
            from sec_edgar_downloader import Downloader
        except ImportError as exc:
            logger.error("sec-edgar-downloader not installed: %s", exc)
            return []

        self._download_dir.mkdir(parents=True, exist_ok=True)

        try:
            dl = Downloader(self._company_name, self._email, self._download_dir)
        except TypeError:
            # Older API: Downloader(company_name, email) — no path arg
            dl = Downloader(self._company_name, self._email)

        docs: list[RawDocument] = []
        ingested_at = datetime.now(timezone.utc).isoformat()

        for ticker in self._tickers:
            if not self._TICKER_RE.match(ticker):
                logger.warning("Invalid ticker format, skipping: %s", ticker)
                continue
            for form in self._forms:
                filing_url = (
                    f"https://www.sec.gov/cgi-bin/browse-edgar"
                    f"?action=getcompany&CIK={ticker}&type={form}"
                )
                try:
                    dl.get(form, ticker, limit=5)
                    docs.extend(self._read_filings(ticker, form, ingested_at))
                except Exception as exc:
                    logger.error(
                        "EDGAR fetch failed for ticker=%s form=%s: %s", ticker, form, exc
                    )
                    _log_failed_job(self._repository, filing_url, "edgar", str(exc))

        return docs

    def _read_filings(
        self, ticker: str, form: str, ingested_at: str
    ) -> list[RawDocument]:
        docs: list[RawDocument] = []
        # sec-edgar-downloader saves to: {download_dir}/sec-edgar-filings/{ticker}/{form}/
        filing_base = self._download_dir / "sec-edgar-filings" / ticker / form
        if not filing_base.resolve().is_relative_to(self._download_dir.resolve()):
            logger.warning("Path traversal blocked for ticker=%s", ticker)
            return docs
        if not filing_base.exists():
            return docs

        for submission_dir in filing_base.iterdir():
            if not submission_dir.is_dir():
                continue
            # Prefer primary-document files; fall back to any .txt
            candidates = list(submission_dir.glob("primary-document.txt"))
            if not candidates:
                candidates = list(submission_dir.glob("*.txt"))

            for txt_file in candidates:
                try:
                    raw_text = txt_file.read_text(encoding="utf-8", errors="replace")
                    raw_text = raw_text.strip()
                    if not raw_text:
                        continue

                    source_url = (
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{ticker}/{submission_dir.name}/{txt_file.name}"
                    )
                    doc = RawDocument(
                        doc_id=str(uuid.uuid4()),
                        raw_text=raw_text,
                        source_url=source_url,
                        source_domain=self._SEC_DOMAIN,
                        published_at=ingested_at,
                        ingested_at=ingested_at,
                        author=f"{ticker} (SEC {form} Filing)",
                        raw_text_hash=_compute_hash(raw_text),
                    )
                    docs.append(doc)
                except Exception as exc:
                    logger.error("Error reading EDGAR file %s: %s", txt_file, exc)

        return docs


# ---------------------------------------------------------------------------
# PlaywrightIngester (stub)
# ---------------------------------------------------------------------------

class PlaywrightIngester(Ingester):
    # TODO: implement after MVP validation and legal ToS review per target site

    def ingest(self) -> list[RawDocument]:
        raise NotImplementedError("PlaywrightIngester not implemented for MVP")
