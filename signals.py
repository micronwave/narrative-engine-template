"""
All functions in this module are pure computation.
No LLM calls, no database writes, no side effects.
"""

import math
import re
from collections import Counter
from datetime import datetime, timezone

import logging
import numpy as np

logger = logging.getLogger(__name__)


def format_cycle_slot(now: datetime, cycle_hours: int) -> str:
    """Return the UTC cycle-slot key used for centroid history snapshots."""
    freq = max(cycle_hours, 1)
    now_utc = now.astimezone(timezone.utc)
    slot_hour = (now_utc.hour // freq) * freq
    return now_utc.strftime(f"%Y-%m-%dT{slot_hour:02d}")

# ---------------------------------------------------------------------------
# Sentiment Lexicons (financially-relevant)
# ---------------------------------------------------------------------------

POSITIVE_WORDS: list[str] = [
    "surge", "rally", "gain", "profit", "growth", "outperform", "beat",
    "exceed", "record", "breakout", "upgrade", "expansion", "accelerate",
    "momentum", "recovery", "rebound", "bullish", "opportunity", "advance",
    "upside", "optimism", "strong", "robust", "boost", "increase", "win",
    "positive", "success", "thrive", "prosper", "improvement", "rise",
    "climb", "higher", "revenue", "innovation", "leading", "dominant",
    "efficient", "profitable", "dividend", "milestone", "delivery",
    "outperformed", "confident", "accelerating", "growing", "expanded",
    "exceeded", "invested", "awarded", "approved", "launched", "soared",
]

NEGATIVE_WORDS: list[str] = [
    "crash", "plunge", "loss", "decline", "miss", "fail", "cut", "slash",
    "downgrade", "risk", "warning", "crisis", "debt", "concern", "worry",
    "bearish", "selloff", "collapse", "downturn", "recession", "layoff",
    "shortfall", "default", "restructure", "uncertainty", "headwind",
    "disappointing", "weak", "drop", "lower", "negative", "slump", "retreat",
    "contraction", "slowdown", "deficit", "penalty", "litigation", "probe",
    "investigation", "fraud", "recall", "disruption", "volatile", "pressure",
    "challenging", "deteriorate", "suspended", "halted", "missed", "fell",
    "reduced", "impairment", "writedown",
]

# ---------------------------------------------------------------------------
# Vocabulary Constants
# ---------------------------------------------------------------------------

FISCAL_INTENT_VOCAB: list[str] = [
    "allocating", "capex", "contracted", "committed",
    "executing", "deployed", "acquiring", "divesting",
]

HEDGE_VOCAB: list[str] = [
    "potential", "possible", "could", "speculative",
    "rumored", "considering", "exploring",
]

# Extended vocabulary used for entropy entity extraction.
_ENTITY_VOCAB: list[str] = FISCAL_INTENT_VOCAB + HEDGE_VOCAB + [
    "earnings", "revenue", "profit", "loss", "guidance", "forecast",
    "merger", "acquisition", "ipo", "dividend", "buyback", "margin",
    "growth", "expansion", "restructuring", "layoff", "bankruptcy",
    "interest", "rates", "inflation", "fed", "gdp", "unemployment",
]

# Pre-compile regex patterns for entity vocab (case-insensitive word-boundary).
_ENTITY_PATTERNS: list[re.Pattern] = [
    re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE)
    for term in _ENTITY_VOCAB
]

# Pre-compile regex patterns for intent/hedge vocab.
_FISCAL_PATTERNS: list[re.Pattern] = [
    re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE)
    for term in FISCAL_INTENT_VOCAB
]
_HEDGE_PATTERNS: list[re.Pattern] = [
    re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE)
    for term in HEDGE_VOCAB
]

# Ticker regex: 1-5 uppercase letters, word boundaries.
# Candidates are filtered by _KNOWN_TICKERS to prevent acronym false positives (NASA, CEO,
# WHO) while allowing valid single-letter symbols (F, V, C, A, H, D, O).
_TICKER_RE = re.compile(r"\b[A-Z]{1,5}\b")
# Known tradeable symbols: S&P 500 constituents + major ADRs.
# A whitelist beats a blacklist here — handles single-letter tickers and blocks acronyms.
_KNOWN_TICKERS: frozenset[str] = frozenset({
    # Mega-cap / broad tech
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "GOOG", "META", "TSLA",
    "ADBE", "CRM", "CSCO", "TXN", "INTU", "ORCL", "NOW", "SNOW", "DDOG",
    "OKTA", "ZS", "CRWD", "PANW", "FTNT", "NET", "MDB", "TEAM", "ZM",
    "DOCU", "TWLO", "SAP", "ASML", "TSM", "AMAT", "LRCX", "KLAC",
    "NTES", "BIDU",
    # Consumer / retail / media
    "WMT", "COST", "HD", "TGT", "MCD", "SBUX", "LOW", "TJX", "BKNG",
    "MAR", "HLT", "DIS", "NFLX", "NKE", "LULU", "SNAP", "PINS", "MTCH", "TTD",
    "ROKU", "SPOT", "EA", "TTWO", "RBLX", "UBER", "LYFT", "ABNB", "DASH",
    # Consumer goods
    "PG", "KO", "PEP", "MO", "PM",
    # Financials
    "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "AXP", "V", "MA",
    "PYPL", "SQ", "COIN", "CME", "ICE", "SPGI", "MSCI", "MCO", "USB", "PNC",
    "TFC", "COF", "DFS", "SYF", "BK", "STT", "PRU", "MET", "AIG", "AFL",
    "PFG", "TRV", "ALL", "CB", "FIS", "FISV", "GPN", "AON",
    # Healthcare
    "UNH", "LLY", "MRK", "ABBV", "JNJ", "PFE", "GILD", "AMGN", "BMY",
    "MDT", "ABT", "TMO", "DHR", "ISRG", "SYK", "BSX", "BDX", "ELV",
    "HUM", "CI", "CVS", "WBA", "CAH", "ABC", "WAT", "MTD", "PKI", "IDXX",
    "IQV", "CRL", "ILMN", "BIIB", "VRTX", "ALNY", "MRNA", "BNTX", "HCA",
    "REGN", "ZTS", "A",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG", "OXY", "DVN", "FANG", "MRO", "APA",
    "OVV", "CTRA", "PR", "VLO", "MPC", "PSX", "HAL", "BKR", "SU", "ENB",
    # Industrials / defense / auto
    "HON", "CAT", "DE", "EMR", "ITW", "RTX", "LMT", "GD", "NOC", "BA",
    "HII", "LHX", "TDG", "LDOS", "SAIC", "GE", "ETN", "UPS", "FDX", "ACN",
    "CHRW", "EXPD", "XPO", "JBHT", "KNX", "F", "GM",
    # Materials / mining
    "LIN", "APD", "ECL", "PPG", "SHW", "DOW", "VMC", "MLM", "NEM", "GOLD",
    "AEM", "FNV", "WPM", "RGLD", "FCX", "NUE", "STLD",
    # Shipping
    "MATX", "ZIM", "DAC", "GOGL", "SBLK", "EGLE",
    # Utilities
    "SO", "DUK", "NEE", "AES", "ES", "ETR", "PPL", "XEL", "WEC", "CNP",
    "NI", "OGE", "PNW", "IDA", "NWE", "AVA", "POR", "HE", "SRE", "PEG",
    "AEE", "AEP", "EXC", "PCG", "ED", "D", "AWK",
    # REITs
    "SPG", "O", "AMT", "CCI", "SBAC", "DLR", "EQIX", "PSA", "EXR", "PLD",
    "AVB", "EQR", "MAA", "CPT", "UDR", "ESS", "INVH", "AMH", "VTR", "WELL",
    "OHI", "SBRA", "NHI", "HR", "DOC", "CTRE", "MPW", "LTC",
    # International / ADRs
    "NVO", "AZN", "GSK", "NOVN", "ROG", "SNY", "BAYRY", "RHHBY", "SIEGY",
    "NSRGY", "RY", "TD", "BNS", "BMO", "CM", "CNI", "CP", "JD", "PDD",
    "NIO", "XPEV", "LI", "BABA", "TCEHY",
    # Travel / hospitality
    "HLT", "H", "IHG", "CCL", "RCL", "NCLH", "DAL", "UAL", "AAL", "LUV",
    "ALK", "CSX", "NSC", "UNP",
    # Waste / water
    "WM", "RSG",
    # Misc
    "AVGO", "COIN", "SHOP",
})

# POSITIVE/NEGATIVE word sets for O(1) lookup.
_POS_SET = set(POSITIVE_WORDS)
_NEG_SET = set(NEGATIVE_WORDS)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _sentiment_score(text: str) -> float:
    """Simple lexicon-based sentiment: (positive_count - negative_count) / (total_words + 1e-9)."""
    words = re.findall(r"\b\w+\b", text.lower())
    total = len(words)
    pos = sum(1 for w in words if w in _POS_SET)
    neg = sum(1 for w in words if w in _NEG_SET)
    return (pos - neg) / (total + 1e-9)


def compute_sentiment_scores(documents: list[str]) -> dict:
    """Public wrapper for per-document sentiment analysis.
    Returns {mean, min, max, std, count, polarization_label}."""
    if not documents:
        return {"mean": 0.0, "min": 0.0, "max": 0.0, "std": 0.0, "count": 0, "polarization_label": "No data"}
    scores = [_sentiment_score(doc) for doc in documents]
    mean = float(np.mean(scores))
    std = float(np.std(scores))
    polarization_label = "Sources aligned" if std < 0.05 else ("Sources disagree" if std > 0.15 else "Moderate spread")
    return {
        "mean": round(mean, 4),
        "min": round(float(np.min(scores)), 4),
        "max": round(float(np.max(scores)), 4),
        "std": round(std, 4),
        "count": len(scores),
        "polarization_label": polarization_label,
    }


# ---------------------------------------------------------------------------
# Signal Functions
# ---------------------------------------------------------------------------

def compute_velocity(
    centroid_today: np.ndarray,
    centroid_yesterday: np.ndarray,
) -> float:
    """
    V = magnitude(C_today − C_yesterday) / (magnitude(C_yesterday) + 1e-9)
    """
    diff = centroid_today - centroid_yesterday
    mag_diff = float(np.linalg.norm(diff))
    mag_yesterday = float(np.linalg.norm(centroid_yesterday))
    return mag_diff / (mag_yesterday + 1e-9)


def compute_velocity_windowed(
    centroid_history: list[np.ndarray],
    window_days: int,
) -> float:
    """
    Average of cycle-slot velocities over window_days.
    centroid_history is ordered most-recent first and deduplicated by the
    repository layer to one snapshot per cycle slot.
    Returns 0.0 if fewer than 2 entries.
    """
    if len(centroid_history) < 2:
        return 0.0

    # Limit to window_days intervals (need window_days + 1 entries).
    history = centroid_history[: window_days + 1]
    velocities = [
        compute_velocity(history[i], history[i + 1])
        for i in range(len(history) - 1)
    ]
    return float(np.mean(velocities)) if velocities else 0.0


def extract_known_tickers(text: str, min_length: int = 2) -> list[str]:
    """Return sorted unique tickers from text that are in the known-ticker whitelist.

    min_length=2 (default) excludes single-letter symbols (A, C, D, F, H, O, V) which
    are indistinguishable from common English words in unstructured news text.
    """
    return sorted(
        {t for t in _TICKER_RE.findall(text) if t in _KNOWN_TICKERS and len(t) >= min_length}
    )


def compute_entropy(
    documents: list[str],
    min_vocab_size: int,
    include_single_letter_tickers: bool = False,
) -> float | None:
    """
    H = −Σ p(x) log p(x) over distribution of unique extracted entities.
    Entities = known ticker symbols (whitelist-filtered) + fiscal/financial vocabulary terms.
    Returns None when unique entity count < min_vocab_size.
    By default, excludes single-letter tickers to avoid sentence-leading noise.
    Uses natural log (np.log).
    """
    entity_counts: Counter = Counter()

    ticker_min_length = 1 if include_single_letter_tickers else 2
    for doc in documents:
        tickers = extract_known_tickers(doc, min_length=ticker_min_length)
        entity_counts.update(tickers)

        # Fiscal/financial vocabulary terms (case-insensitive).
        for pattern in _ENTITY_PATTERNS:
            count = len(pattern.findall(doc))
            if count:
                entity_counts[pattern.pattern] += count

    if len(entity_counts) < min_vocab_size:
        return None

    total = sum(entity_counts.values())
    if total == 0:
        return None

    entropy = 0.0
    for count in entity_counts.values():
        if count > 0:
            p = count / total
            entropy -= p * np.log(p)

    return float(entropy)


def compute_intent_weight(documents: list[str]) -> float:
    """
    intent_ratio = fiscal_matches / (hedge_matches + fiscal_matches + 1e-9)
    Matches whole words case-insensitively.
    """
    fiscal_matches = 0
    hedge_matches = 0

    for doc in documents:
        for pattern in _FISCAL_PATTERNS:
            fiscal_matches += len(pattern.findall(doc))
        for pattern in _HEDGE_PATTERNS:
            hedge_matches += len(pattern.findall(doc))

    return fiscal_matches / (hedge_matches + fiscal_matches + 1e-9)


def compute_cross_source_score(
    narrative_domains: list[str],
    corpus_domain_count: int,
) -> float:
    """
    cross_source_score = len(unique_source_domains) / max(corpus_domain_count, 1)
    """
    unique_domains = len(set(narrative_domains))
    return unique_domains / max(corpus_domain_count, 1)


def compute_cohesion(embeddings: list[np.ndarray]) -> float:
    """
    Mean of pairwise cosine similarities among all document embeddings.
    For L2-normalized vectors, cosine similarity = dot product.
    Returns 0.0 (insufficient data) if fewer than 2 embeddings.
    """
    if len(embeddings) < 2:
        return 0.0

    arr = np.array(embeddings, dtype=np.float32)  # (N, D)
    # Cosine similarity matrix via dot product (valid for L2-normalized vecs).
    sim_matrix = arr @ arr.T  # (N, N)

    n = arr.shape[0]
    upper = np.triu_indices(n, k=1)
    pairwise_sims = sim_matrix[upper]

    return float(np.mean(pairwise_sims)) if pairwise_sims.size > 0 else 0.0


def compute_polarization(documents: list[str]) -> float:
    """
    Standard deviation of per-document sentiment scores.
    Returns 0.0 if fewer than 2 documents.
    """
    if len(documents) < 2:
        return 0.0

    scores = [_sentiment_score(doc) for doc in documents]
    return float(np.std(scores))


def compute_public_interest(
    cross_source_score: float,
    cross_source_prev: float,
    doc_count: int,
    doc_count_prev: int,
    reddit_doc_count: int = 0,
) -> float:
    """Public interest indicator: HIGH (>0.6), MEDIUM (>0.3), LOW.
    Combines cross-source acceleration, doc count acceleration, and Reddit signal."""
    # Cross-source acceleration (is coverage spreading to more sources?)
    cs_accel = max(0, cross_source_score - cross_source_prev) / max(cross_source_prev, 0.01)
    cs_component = min(1.0, cs_accel * 2.0)  # 50% increase = 1.0

    # Doc count acceleration (is inflow accelerating?)
    doc_diff = doc_count - doc_count_prev
    doc_accel = doc_diff / max(doc_count_prev, 1)
    doc_component = min(1.0, max(0, doc_accel))

    # Reddit signal (retail attention proxy)
    reddit_component = min(1.0, reddit_doc_count / 10.0) if reddit_doc_count > 0 else 0.0

    # Weighted composite
    score = cs_component * 0.4 + doc_component * 0.3 + reddit_component * 0.3
    return round(min(1.0, max(0.0, score)), 4)


def compute_ns_score(
    velocity: float,
    intent_weight: float,
    cross_source_score: float,
    cohesion: float,
    polarization: float,
    centrality: float,
    entropy: float | None,
    entropy_vocab_window: int = 10,
) -> float:
    """
    Ns = (0.25 × velocity_normalized × intent_weight)
       + (0.20 × cross_source_score)
       + (0.15 × cohesion)
       + (0.15 × polarization_normalized)
       + (0.15 × centrality)
       + (0.10 × entropy_normalized)

    entropy_vocab_window: the ENTROPY_VOCAB_WINDOW setting (passed by the
    pipeline to keep this module free of settings imports).
    """
    velocity_normalized = min(velocity / 0.5, 1.0)
    polarization_normalized = min(polarization / 0.5, 1.0)

    if entropy is not None:
        log_window = math.log(entropy_vocab_window) if entropy_vocab_window > 1 else 1.0
        entropy_normalized = min(entropy / log_window, 1.0)
    else:
        entropy_normalized = 0.0

    ns = (
        0.25 * velocity_normalized * intent_weight
        + 0.20 * cross_source_score
        + 0.15 * cohesion
        + 0.15 * polarization_normalized
        + 0.15 * centrality
        + 0.10 * entropy_normalized
    )
    return float(max(0.0, min(ns, 1.0)))


# ---------------------------------------------------------------------------
# Lifecycle stage progression (F1)
# ---------------------------------------------------------------------------

def get_narrative_age_days(created_at: str) -> int:
    """Returns days since narrative creation from an ISO timestamp string."""
    try:
        created_date = datetime.fromisoformat(created_at).date()
        today = datetime.now(timezone.utc).date()
        return max(0, (today - created_date).days)
    except Exception:
        logger.warning("Could not parse created_at '%s' — defaulting age to 0", created_at)
        return 0


def compute_lifecycle_stage(
    current_stage: str,
    document_count: int,
    velocity_windowed: float,
    entropy: float | None,
    consecutive_declining_cycles: int,
    days_since_creation: int,
    cycles_in_current_stage: int = 0,
) -> str:
    """
    Returns the new lifecycle stage based on narrative metrics.

    Stages: Emerging → Growing → Mature → Declining → Dormant

    Rules applied in order:
    1. Revival: Declining/Dormant with velocity > 0.10 → Growing
    2. Emerging → Growing: (doc_count >= 8 AND velocity > 0.02)
       OR age-based fallback (doc_count >= 10 AND age >= 2 days)
    3. Growing → Mature: (days >= 5 AND entropy >= 1.2 AND doc_count >= 15)
       OR volume-based fallback (doc_count >= 30 AND age >= 7 days)
    4. Mature → Declining: consecutive_declining_cycles >= 30
       OR (consecutive_declining_cycles >= 18 AND velocity < 0.008)
    5. Declining → Dormant: consecutive_declining_cycles >= 42 AND velocity < 0.01

    Hysteresis: transitions (except revival) require >= 3 cycles in current stage.
    Never skips stages (Emerging cannot jump directly to Mature).
    """
    # Revival check — applies before any other rule (no hysteresis)
    if current_stage in ("Declining", "Dormant") and velocity_windowed > 0.10:
        return "Growing"

    growing_velocity_threshold = 0.02
    declining_velocity_threshold = 0.008

    # Compute proposed stage from rules
    if current_stage == "Emerging":
        if ((document_count >= 8 and velocity_windowed > growing_velocity_threshold)
                or (document_count >= 10 and days_since_creation >= 2)):
            proposed = "Growing"
        else:
            proposed = "Emerging"

    elif current_stage == "Growing":
        if ((days_since_creation >= 5
                and entropy is not None
                and entropy >= 1.2
                and document_count >= 15)
                or (document_count >= 30 and days_since_creation >= 7)):
            proposed = "Mature"
        else:
            proposed = "Growing"

    elif current_stage == "Mature":
        if consecutive_declining_cycles >= 30:
            proposed = "Declining"
        elif consecutive_declining_cycles >= 18 and velocity_windowed < declining_velocity_threshold:
            proposed = "Declining"
        else:
            proposed = "Mature"

    elif current_stage == "Declining":
        if consecutive_declining_cycles >= 42 and velocity_windowed < 0.01:
            proposed = "Dormant"
        else:
            proposed = "Declining"

    else:
        # Dormant stays dormant unless revived (handled above)
        logger.warning("Unknown lifecycle stage '%s' — returning unchanged", current_stage)
        return current_stage

    # Hysteresis gate: suppress transitions if not enough cycles in current stage
    if proposed != current_stage and cycles_in_current_stage < 3:
        return current_stage

    return proposed


# ---------------------------------------------------------------------------
# Burst velocity (F2)
# ---------------------------------------------------------------------------

def compute_inflow_velocity(
    doc_count_current_cycle: int,
    avg_docs_per_cycle_7d: float,
) -> float:
    """
    Measures how fast documents are arriving relative to the 7-day average.

    Returns:
    - 1.0 = average flow rate
    - >1.0 = accelerating (more docs than usual)
    - <1.0 = decelerating
    - 0.0 = no documents this cycle

    Clamped to [0.0, 10.0] to prevent outlier spikes.
    """
    if avg_docs_per_cycle_7d < 1.0:
        avg_docs_per_cycle_7d = 1.0
    return min(doc_count_current_cycle / avg_docs_per_cycle_7d, 10.0)


def compute_burst_velocity(
    recent_doc_count: int,
    baseline_docs_per_window: float,
    alert_ratio: float = 3.0,
) -> dict:
    """
    Measures document ingestion rate acceleration.
    Graceful degradation: returns ratio=0 when baseline is 0 (insufficient data).
    """
    rate = float(recent_doc_count)
    if baseline_docs_per_window <= 0:
        return {"rate": rate, "baseline": 0.0, "ratio": 0.0, "is_burst": False}
    ratio = rate / baseline_docs_per_window
    return {
        "rate": rate,
        "baseline": round(baseline_docs_per_window, 2),
        "ratio": round(ratio, 2),
        "is_burst": ratio >= alert_ratio,
    }


# ---------------------------------------------------------------------------
# LLM Signal Extraction Helpers (Phase 1)
# ---------------------------------------------------------------------------

_DIRECTION_MAP = {"bullish": 1.0, "bearish": -1.0, "neutral": 0.0}
_CERTAINTY_MAP = {"speculative": 0.2, "rumored": 0.4, "expected": 0.7, "confirmed": 1.0}
_MAGNITUDE_MAP = {"incremental": 0.3, "significant": 0.6, "transformative": 1.0}

_VALID_DIRECTIONS = frozenset(_DIRECTION_MAP.keys())
_VALID_TIMEFRAMES = frozenset({"immediate", "near_term", "long_term", "unknown"})
_VALID_MAGNITUDES = frozenset(_MAGNITUDE_MAP.keys())
_VALID_CERTAINTIES = frozenset(_CERTAINTY_MAP.keys())
_VALID_CATALYST_TYPES = frozenset(
    {"earnings", "regulatory", "geopolitical", "macro", "corporate", "unknown"}
)


def direction_to_float(direction: str) -> float:
    """Map direction string to numeric value. Unknown defaults to 0.0."""
    try:
        return _DIRECTION_MAP.get(str(direction).strip().lower(), 0.0)
    except Exception:
        return 0.0


def certainty_to_float(certainty: str) -> float:
    """Map certainty level to weight. Unknown defaults to 0.2."""
    try:
        return _CERTAINTY_MAP.get(str(certainty).strip().lower(), 0.2)
    except Exception:
        return 0.2


def magnitude_to_float(magnitude: str) -> float:
    """Map magnitude to scaling factor. Unknown defaults to 0.3."""
    try:
        return _MAGNITUDE_MAP.get(str(magnitude).strip().lower(), 0.3)
    except Exception:
        return 0.3


def validate_signal_fields(raw: dict) -> dict:
    """
    Validate and normalize all 8 signal fields from LLM output.
    Never raises — returns safe defaults for any invalid/missing field.
    """
    defaults = {
        "direction": "neutral",
        "confidence": 0.0,
        "timeframe": "unknown",
        "magnitude": "incremental",
        "certainty": "speculative",
        "key_actors": [],
        "affected_sectors": [],
        "catalyst_type": "unknown",
    }
    try:
        if not isinstance(raw, dict):
            return dict(defaults)

        result = {}

        # direction
        d = str(raw.get("direction", "")).strip().lower()
        result["direction"] = d if d in _VALID_DIRECTIONS else defaults["direction"]

        # confidence — coerce to float, clamp [0.0, 1.0]
        try:
            c = float(raw.get("confidence", 0.0))
            result["confidence"] = max(0.0, min(1.0, c))
        except (TypeError, ValueError):
            result["confidence"] = defaults["confidence"]

        # timeframe
        t = str(raw.get("timeframe", "")).strip().lower()
        result["timeframe"] = t if t in _VALID_TIMEFRAMES else defaults["timeframe"]

        # magnitude
        m = str(raw.get("magnitude", "")).strip().lower()
        result["magnitude"] = m if m in _VALID_MAGNITUDES else defaults["magnitude"]

        # certainty
        ce = str(raw.get("certainty", "")).strip().lower()
        result["certainty"] = ce if ce in _VALID_CERTAINTIES else defaults["certainty"]

        # key_actors — coerce to list of strings, max 10 items, max 100 chars each
        result["key_actors"] = _coerce_string_list(
            raw.get("key_actors", []), max_items=10, max_item_len=100
        )

        # affected_sectors — coerce to list of strings, max 5 items, max 50 chars each
        result["affected_sectors"] = _coerce_string_list(
            raw.get("affected_sectors", []), max_items=5, max_item_len=50
        )

        # catalyst_type
        ct = str(raw.get("catalyst_type", "")).strip().lower()
        result["catalyst_type"] = ct if ct in _VALID_CATALYST_TYPES else defaults["catalyst_type"]

        return result

    except Exception:
        return dict(defaults)


def _coerce_string_list(val, max_items: int, max_item_len: int) -> list:
    """Coerce a value into a bounded list of strings."""
    import json as _json

    if isinstance(val, str):
        try:
            val = _json.loads(val)
        except (ValueError, TypeError):
            return [val[:max_item_len]] if val.strip() else []

    if not isinstance(val, list):
        return []

    result = []
    for item in val[:max_items]:
        if isinstance(item, str) and item.strip():
            result.append(item.strip()[:max_item_len])
        elif item is not None:
            s = str(item).strip()
            if s:
                result.append(s[:max_item_len])
    return result
