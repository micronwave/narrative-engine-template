"""
robots.txt compliance is a technical floor, not a legal guarantee.
Operators must independently review the Terms of Service of each target site
before enabling ingestion. This module enforces disallow rules but does not
constitute legal clearance.
"""

import logging
import urllib.request
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from repository import Repository

logger = logging.getLogger(__name__)

_USER_AGENT = "NarrativeIntelligenceBot/1.0"
_TTL_HOURS = 24


def can_fetch(url: str, repository: Repository) -> bool:
    """
    Check whether NarrativeIntelligenceBot is allowed to fetch the given URL.

    Uses a 24-hour cache stored in the robots_cache table. On any fetch error,
    logs a warning and returns True (assume ALLOW) to avoid blocking ingestion.
    """
    parsed = urlparse(url)
    domain = parsed.netloc
    if not domain:
        return True

    robots_url = f"{parsed.scheme}://{domain}/robots.txt"

    # Check cache first
    cached = repository.get_robots_cache(domain)
    if cached:
        fetched_at_str: str = cached["fetched_at"]
        fetched_at = datetime.fromisoformat(fetched_at_str)
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - fetched_at < timedelta(hours=_TTL_HOURS):
            rp = RobotFileParser()
            rp.parse(cached["rules_text"].splitlines())
            return rp.can_fetch(_USER_AGENT, url)

    # Fetch and cache fresh copy
    try:
        req = urllib.request.Request(
            robots_url, headers={"User-Agent": _USER_AGENT}
        )
        _MAX_ROBOTS_SIZE = 512 * 1024  # 512 KB — more than any legitimate robots.txt
        with urllib.request.urlopen(req, timeout=10) as resp:
            # Reject cross-domain redirects — caching another domain's rules is wrong
            final_domain = urlparse(resp.url).netloc
            if final_domain and final_domain != domain:
                logger.warning(
                    "robots.txt for %s redirected to %s — assuming ALLOW",
                    domain, final_domain,
                )
                return True
            rules_text = resp.read(_MAX_ROBOTS_SIZE).decode("utf-8", errors="replace")

        rp = RobotFileParser()
        rp.parse(rules_text.splitlines())

        fetched_at = datetime.now(timezone.utc).isoformat()
        repository.set_robots_cache(domain, rules_text, fetched_at)

        logger.debug("Fetched and cached robots.txt for %s", domain)
        return rp.can_fetch(_USER_AGENT, url)

    except Exception as exc:
        logger.warning(
            "robots.txt fetch failed for %s (%s) — assuming ALLOW", domain, exc
        )
        return True
