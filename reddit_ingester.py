"""
Reddit Ingester — V3 Phase 3.1

Uses PRAW (already installed) to pull posts from financial subreddits.
Fails silently when credentials not configured (non-fatal, like RSS feeds).
"""

import hashlib
import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


class RedditIngester:
    """Ingests financial posts from Reddit via PRAW."""

    DEFAULT_SUBREDDITS = [
        "wallstreetbets",
        "stocks",
        "investing",
        "economics",
        "options",
    ]

    def __init__(self, repository, settings):
        self.repository = repository
        self.settings = settings
        self.client_id = getattr(settings, "REDDIT_CLIENT_ID", "")
        self.client_secret = getattr(settings, "REDDIT_CLIENT_SECRET", "")
        self.user_agent = getattr(settings, "REDDIT_USER_AGENT", "narrative_engine/1.0")
        self.subreddits = getattr(settings, "REDDIT_SUBREDDITS", self.DEFAULT_SUBREDDITS)
        self.posts_per_sub = getattr(settings, "REDDIT_POSTS_PER_SUB", 50)

    def is_enabled(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def ingest(self) -> list[dict]:
        """Fetch recent posts from configured subreddits. Returns list of RawDocument-like dicts."""
        if not self.is_enabled():
            logger.info("RedditIngester: disabled (no REDDIT_CLIENT_ID/SECRET)")
            return []

        try:
            import praw
        except ImportError:
            logger.warning("RedditIngester: praw not installed")
            return []

        try:
            reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent,
            )
        except Exception as e:
            logger.warning("RedditIngester: PRAW auth failed: %s", e)
            return []

        docs = []
        for sub_name in self.subreddits:
            try:
                sub_count = 0
                subreddit = reddit.subreddit(sub_name)
                for post in subreddit.hot(limit=self.posts_per_sub):
                    text = f"{post.title}\n\n{post.selftext or ''}"
                    if len(text.strip()) < 20:
                        continue

                    truncated = text[:5000]
                    doc = {
                        "doc_id": str(uuid.uuid4()),
                        "raw_text": truncated,
                        "source_url": f"https://reddit.com{post.permalink}",
                        "source_domain": f"reddit.com/r/{sub_name}",
                        "published_at": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                        "author": str(post.author) if post.author else None,
                        "raw_text_hash": _compute_hash(truncated),
                        "source_type": "reddit",
                        "reddit_score": post.score,
                        "reddit_comments": post.num_comments,
                    }
                    docs.append(doc)
                    sub_count += 1

                logger.info("RedditIngester: %d posts from r/%s", sub_count, sub_name)
            except Exception as e:
                logger.warning("RedditIngester: error on r/%s: %s", sub_name, e)
                continue

        return docs
