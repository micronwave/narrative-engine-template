"""
Phase 6: Directional Asset Impact Scoring.

Composes directional impact scores from all prior signal phases:
- Phase 1: LLM signal extraction (direction, confidence, certainty, magnitude)
- Phase 2: Source escalation metrics (tier, velocity, institutional pickup)
- Phase 3: Convergence pressure (direction consensus, weighted confidence)
- Phase 4: Catalyst proximity (earnings, FOMC, macro alignment)
- Phase 5: Learned Ns score (replaces hand-tuned weights)

Produces per-ticker directional impact: "this narrative is bearish for NVDA
with 0.72 confidence, 1-2 week horizon, based on convergence pressure +
institutional pickup + earnings proximity."
"""

import json
import logging

from signals import direction_to_float, certainty_to_float, magnitude_to_float

logger = logging.getLogger(__name__)

# Time horizon mapping from signal timeframe
_HORIZON_MAP = {
    "immediate": "1-3d",
    "near_term": "1-2w",
    "long_term": "1-3m",
}


def compute_directional_impact(
    narrative_id: str,
    ticker: str,
    similarity_score: float,
    signal: dict | None,
    convergence: dict | None,
    escalation: dict,
    catalyst: dict,
    asset_name: str = "",
) -> dict:
    """
    Compose directional impact from all available signal data.

    Args:
        narrative_id: The narrative ID.
        ticker: Ticker symbol.
        similarity_score: Original FAISS cosine similarity score.
        signal: From narrative_signals (Phase 1). None if unavailable.
        convergence: From ticker_convergence (Phase 3). None if unavailable.
        escalation: Narrative's source escalation data (Phase 2).
        catalyst: From compute_catalyst_proximity (Phase 4).
        asset_name: Human-readable asset name.

    Returns:
        Enriched dict with direction, impact_score, confidence, time_horizon,
        signal_components — a superset of the original linked_assets shape.
    """
    # Extract signal fields with safe defaults
    if signal and isinstance(signal, dict):
        direction = signal.get("direction", "neutral")
        sig_confidence = _safe_float(signal.get("confidence"), 0.0)
        certainty = signal.get("certainty", "speculative")
        magnitude = signal.get("magnitude", "incremental")
        timeframe = signal.get("timeframe", "unknown")
    else:
        direction = "neutral"
        sig_confidence = 0.0
        certainty = "speculative"
        magnitude = "incremental"
        timeframe = "unknown"

    # Extract convergence fields with safe defaults
    if convergence and isinstance(convergence, dict):
        conv_direction_consensus = _safe_float(convergence.get("direction_consensus"), 0.0)
        conv_count = int(convergence.get("convergence_count", 0))
        conv_pressure = _safe_float(convergence.get("pressure_score"), 0.0)
    else:
        conv_direction_consensus = 0.0
        conv_count = 0
        conv_pressure = 0.0

    # Extract catalyst fields with safe defaults
    if catalyst and isinstance(catalyst, dict):
        cat_proximity = _safe_float(catalyst.get("proximity_score"), 0.0)
        cat_type = catalyst.get("catalyst_type", "none")
        cat_days = catalyst.get("days_to_catalyst") or catalyst.get("days_to_earnings") or catalyst.get("days_to_fomc")
        cat_macro = _safe_float(catalyst.get("macro_alignment"), 0.0)
    else:
        cat_proximity = 0.0
        cat_type = "none"
        cat_days = None
        cat_macro = 0.0

    # Extract escalation fields with safe defaults
    if escalation and isinstance(escalation, dict):
        esc_tier = int(escalation.get("source_highest_tier") or escalation.get("highest_tier") or 5)
        esc_velocity = _safe_float(
            escalation.get("source_escalation_velocity") or escalation.get("escalation_velocity"), 0.0
        )
    else:
        esc_tier = 5
        esc_velocity = 0.0

    # Confidence: weighted average of four components
    certainty_val = certainty_to_float(certainty)
    confidence = (
        sig_confidence * 0.35
        + conv_direction_consensus * 0.25
        + certainty_val * 0.20
        + cat_proximity * 0.20
    )
    confidence = max(0.0, min(1.0, confidence))

    # Impact score: confidence * magnitude
    magnitude_val = magnitude_to_float(magnitude)
    impact_score = confidence * magnitude_val
    impact_score = max(0.0, min(1.0, impact_score))

    # Time horizon
    time_horizon = _HORIZON_MAP.get(timeframe, "1-2w")

    # Signal components summary
    signal_components = {
        "llm_direction": direction,
        "convergence_count": conv_count,
        "convergence_pressure": conv_pressure,
        "source_tier": esc_tier,
        "escalation_velocity": esc_velocity,
        "catalyst_type": cat_type,
        "days_to_catalyst": cat_days,
        "macro_alignment": cat_macro,
    }

    return {
        "ticker": ticker,
        "asset_name": asset_name,
        "similarity_score": similarity_score,
        "direction": direction,
        "impact_score": round(impact_score, 4),
        "confidence": round(confidence, 4),
        "time_horizon": time_horizon,
        "signal_components": signal_components,
    }


def enrich_linked_assets(
    narrative_id: str,
    raw_linked_assets: list[dict],
    repository,
) -> list[dict]:
    """
    Enrich raw linked_assets from AssetMapper with directional impact scores.

    Takes raw [{"ticker", "asset_name", "similarity_score"}] from AssetMapper.
    For each ticker, fetches signal, convergence, escalation, and catalyst data.
    Calls compute_directional_impact() for each.
    Returns enriched list sorted by impact_score DESC.
    """
    if not raw_linked_assets:
        return []

    # Fetch narrative signal (Phase 1)
    signal = None
    try:
        signal = repository.get_narrative_signal(narrative_id)
    except Exception:
        pass

    # Fetch narrative row for escalation data (Phase 2)
    narrative = None
    try:
        narrative = repository.get_narrative(narrative_id)
    except Exception:
        pass

    escalation = {}
    if narrative:
        escalation = {
            "source_highest_tier": narrative.get("source_highest_tier"),
            "source_escalation_velocity": narrative.get("source_escalation_velocity"),
        }

    # Fetch catalyst data from narrative row (Phase 4)
    catalyst = {}
    if narrative:
        catalyst = {
            "proximity_score": narrative.get("catalyst_proximity_score"),
            "catalyst_type": narrative.get("catalyst_type"),
            "days_to_catalyst": narrative.get("days_to_catalyst"),
            "macro_alignment": narrative.get("macro_alignment"),
        }

    enriched = []
    for asset in raw_linked_assets:
        if not isinstance(asset, dict):
            continue

        ticker = asset.get("ticker", "")
        if not ticker:
            continue

        # Fetch convergence for this ticker (Phase 3)
        convergence = None
        try:
            convergence = repository.get_ticker_convergence(ticker.upper())
        except Exception:
            pass

        result = compute_directional_impact(
            narrative_id=narrative_id,
            ticker=ticker,
            similarity_score=_safe_float(asset.get("similarity_score"), 0.0),
            signal=signal,
            convergence=convergence,
            escalation=escalation,
            catalyst=catalyst,
            asset_name=asset.get("asset_name", ""),
        )
        enriched.append(result)

    # Sort by impact_score DESC
    enriched.sort(key=lambda x: x.get("impact_score", 0.0), reverse=True)

    return enriched


def _safe_float(val, default: float = 0.0) -> float:
    """Coerce a value to float, returning default on failure or None."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default
