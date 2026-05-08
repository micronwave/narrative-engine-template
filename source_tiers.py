"""
Source authority tier classification and escalation tracking.

Every news source domain is classified into one of five authority tiers
(1 = institutional wire services, 5 = social/retail). Per-narrative
escalation metrics reveal how fast a narrative climbs from low-authority
to high-authority sources — a leading indicator of market impact.

Pure computation module — no side effects, no database access.
"""

from __future__ import annotations

from datetime import datetime, timezone

# ── Tier definitions ──────────────────────────────────────────────────

DOMAIN_TIERS: dict[str, int] = {
    # Tier 1: Wire services & institutional (break news first)
    "reuters.com": 1,
    "apnews.com": 1,
    "bloomberg.com": 1,
    "wsj.com": 1,
    "ft.com": 1,
    # Tier 2: Major business press (amplification & analysis)
    "cnbc.com": 2,
    "marketwatch.com": 2,
    "barrons.com": 2,
    "economist.com": 2,
    "bbc.com": 2,
    "nytimes.com": 2,
    "washingtonpost.com": 2,
    "theguardian.com": 2,
    # Known subdomains — same tier as parent
    "news.bbc.com": 2,
    "money.cnn.com": 2,
    # Tier 3: Industry & trade press (sector-specific signal)
    "techcrunch.com": 3,
    "arstechnica.com": 3,
    "theblock.co": 3,
    "oilprice.com": 3,
    "coindesk.com": 3,
    "semafor.com": 3,
    "theinformation.com": 3,
    "politico.com": 3,
    # Tier 4: Analysis & opinion platforms (retail-facing)
    "seekingalpha.com": 4,
    "fool.com": 4,
    "investopedia.com": 4,
    "zerohedge.com": 4,
    "benzinga.com": 4,
    "thestreet.com": 4,
    "finance.yahoo.com": 4,
    # Tier 5: Social & community
    "reddit.com": 5,
}

DEFAULT_TIER: int = 4

TIER_WEIGHTS: dict[int, float] = {
    1: 3.0,
    2: 2.0,
    3: 1.5,
    4: 1.0,
    5: 0.5,
}


# ── Functions ─────────────────────────────────────────────────────────

def get_domain_tier(domain: str | None) -> int:
    """Return the authority tier (1-5) for a source domain.

    Strips ``www.`` prefix for lookup. Returns DEFAULT_TIER for unknown
    or empty domains.
    """
    if not domain:
        return DEFAULT_TIER

    cleaned = domain.strip().lower()
    if not cleaned:
        return DEFAULT_TIER

    tier = DOMAIN_TIERS.get(cleaned)
    if tier is not None:
        return tier

    # Strip leading www. and retry
    if cleaned.startswith("www."):
        tier = DOMAIN_TIERS.get(cleaned[4:])
        if tier is not None:
            return tier

    return DEFAULT_TIER


def _parse_timestamp(raw: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp string into a timezone-aware datetime.

    Returns None on any failure. Naive datetimes are treated as UTC.
    """
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


_SAFE_ESCALATION_DEFAULTS: dict = {
    "highest_tier": 5,
    "tier_breadth": 0,
    "escalation_velocity": 0.0,
    "is_institutional_pickup": False,
    "tier_first_seen": {},
}


def compute_source_escalation(evidence: list[dict] | None) -> dict:
    """Compute source-authority escalation metrics for a narrative.

    Args:
        evidence: List of evidence dicts, each with ``source_domain``
                  and ``published_at`` fields.

    Returns:
        dict with keys: highest_tier, tier_breadth, escalation_velocity,
        is_institutional_pickup, tier_first_seen.
    """
    if not evidence:
        return dict(_SAFE_ESCALATION_DEFAULTS)

    now = datetime.now(timezone.utc)
    tier_first_seen: dict[int, datetime] = {}
    tier_latest_seen: dict[int, datetime] = {}
    earliest_ts: datetime | None = None

    for doc in evidence:
        domain = doc.get("source_domain")
        if not domain:
            continue

        tier = get_domain_tier(domain)
        ts = _parse_timestamp(doc.get("published_at"))

        if ts is not None:
            # Track earliest timestamp per tier
            if tier not in tier_first_seen or ts < tier_first_seen[tier]:
                tier_first_seen[tier] = ts
            # Track latest timestamp per tier (for institutional pickup)
            if tier not in tier_latest_seen or ts > tier_latest_seen[tier]:
                tier_latest_seen[tier] = ts
            # Track global earliest
            if earliest_ts is None or ts < earliest_ts:
                earliest_ts = ts

    if not tier_first_seen:
        return dict(_SAFE_ESCALATION_DEFAULTS)

    highest_tier = min(tier_first_seen.keys())
    tier_breadth = len(tier_first_seen)

    # Escalation velocity: 0.0 if only one tier or no time span
    if tier_breadth <= 1 or earliest_ts is None:
        escalation_velocity = 0.0
    else:
        hours_since_first = (now - earliest_ts).total_seconds() / 3600.0
        escalation_velocity = (5 - highest_tier) / max(hours_since_first, 0.1)

    # Institutional pickup: any tier 1/2 evidence published within 24h
    is_institutional_pickup = False
    for tier_num, latest_dt in tier_latest_seen.items():
        if tier_num <= 2:
            hours_ago = (now - latest_dt).total_seconds() / 3600.0
            if hours_ago <= 24.0:
                is_institutional_pickup = True
                break

    # Serialize tier_first_seen to ISO strings for storage
    tier_first_seen_iso: dict[int, str] = {
        t: dt.isoformat() for t, dt in tier_first_seen.items()
    }

    return {
        "highest_tier": highest_tier,
        "tier_breadth": tier_breadth,
        "escalation_velocity": round(escalation_velocity, 6),
        "is_institutional_pickup": is_institutional_pickup,
        "tier_first_seen": tier_first_seen_iso,
    }


def compute_weighted_source_score(
    evidence: list[dict] | None,
    corpus_domain_count: int,
) -> float:
    """Compute tier-weighted source diversity score.

    Like ``compute_cross_source_score`` but weights unique domains by
    their authority tier: tier 1 counts 3x, tier 5 counts 0.5x.

    Args:
        evidence: List of evidence dicts with ``source_domain`` field.
        corpus_domain_count: Total unique domains across the corpus
                             (denominator for normalization).

    Returns:
        Float in [0.0, 1.0].
    """
    if not evidence:
        return 0.0

    unique_domains: set[str] = set()
    for doc in evidence:
        domain = doc.get("source_domain")
        if domain:
            unique_domains.add(domain.strip().lower())

    if not unique_domains:
        return 0.0

    weighted_sum = sum(
        TIER_WEIGHTS.get(get_domain_tier(d), TIER_WEIGHTS[DEFAULT_TIER])
        for d in unique_domains
    )

    score = weighted_sum / max(corpus_domain_count, 1)
    return min(max(score, 0.0), 1.0)
