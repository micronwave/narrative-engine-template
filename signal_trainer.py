"""
Learned signal weights for narrative scoring.

Replaces hand-tuned compute_ns_score() weights with empirically learned weights
when sufficient training data (Mature/Declining narratives with price outcomes) exists.
Falls back to default hand-tuned weights when training data is insufficient.
"""

import json
import logging
import math
import os
import pickle
import time
from datetime import datetime, timezone

from safe_pickle import safe_load as _safe_load

logger = logging.getLogger(__name__)

# Feature names in fixed order — must match build_training_dataset() output
FEATURE_NAMES = [
    "velocity_windowed",
    "inflow_velocity",
    "cross_source_score",
    "cohesion",
    "intent_weight",
    "centrality",
    "entropy_normalized",
    "direction_float",
    "confidence",
    "certainty_float",
    "magnitude_float",
    "source_escalation_velocity",
    "convergence_exposure",
    "catalyst_proximity_score",
    "macro_alignment",
]


def _safe_float(val, default: float = 0.0) -> float:
    """Coerce a value to float, returning default on failure or None."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def build_training_dataset(repository) -> tuple[list[list[float]], list[int]]:
    """
    Query all narratives in stage Mature or Declining with non-null linked_assets.

    For each, extract tickers, fetch price history, compute label based on whether
    the ticker moved >2% in the narrative's direction within 7 days of peak velocity.

    Returns (X, y) where X is list of 15-feature vectors, y is list of labels.
    """
    from signals import direction_to_float, certainty_to_float, magnitude_to_float
    from stock_data import get_price_history
    from settings import Settings as _Settings
    _vocab_window = _Settings().ENTROPY_VOCAB_WINDOW
    _entropy_log_window = math.log(_vocab_window) if _vocab_window > 1 else 1.0

    # Get Mature/Declining narratives with price outcome data
    candidates = []
    for stage in ("Mature", "Declining"):
        try:
            stage_narratives = repository.get_narratives_by_stage(stage)
            candidates.extend(stage_narratives)
        except Exception:
            pass

    # Deduplicate by narrative_id
    seen = set()
    unique = []
    for n in candidates:
        nid = n.get("narrative_id")
        if nid and nid not in seen:
            seen.add(nid)
            unique.append(n)
    candidates = unique

    _stats = {
        "narratives_evaluated": len(candidates),
        "assets_examined": 0,
        "assets_filtered_text_mention": 0,
        "assets_filtered_zero_score": 0,
        "assets_filtered_topic": 0,
        "assets_retained": 0,
        "narratives_no_tickers": 0,
        "samples_accepted": 0,
    }

    X = []
    y = []

    for narrative in candidates:
        narrative_id = narrative.get("narrative_id")
        linked_assets_raw = narrative.get("linked_assets")
        if not linked_assets_raw:
            continue

        try:
            linked_assets = json.loads(linked_assets_raw) if isinstance(linked_assets_raw, str) else linked_assets_raw
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(linked_assets, list) or not linked_assets:
            continue

        # Get signal data
        signal = None
        try:
            signal = repository.get_narrative_signal(narrative_id)
        except Exception:
            pass

        direction = "neutral"
        confidence = 0.0
        certainty = "speculative"
        magnitude = "incremental"
        if signal:
            direction = signal.get("direction", "neutral")
            confidence = _safe_float(signal.get("confidence"), 0.0)
            certainty = signal.get("certainty", "speculative")
            magnitude = signal.get("magnitude", "incremental")

        dir_float = direction_to_float(direction)
        if dir_float == 0.0:
            # Neutral direction — can't determine if price moved "in the right direction"
            continue

        # Find peak velocity date from snapshots
        try:
            snapshots = repository.get_snapshot_history(narrative_id, days=90)
        except Exception:
            continue

        if not snapshots:
            continue

        peak_snapshot = max(snapshots, key=lambda s: _safe_float(s.get("velocity"), 0.0))
        peak_date_str = peak_snapshot.get("snapshot_date")
        if not peak_date_str:
            continue

        # Extract tickers — skip TOPIC: entries and text-mention fallback assets.
        # Fallback assets (source="text_mention", similarity_score=0.0) have no
        # embedding confidence, so including them in training labels introduces
        # price-move associations for narratives that may not actually reference
        # the linked ticker in a financially meaningful way.
        tickers = []
        for asset in linked_assets:
            _stats["assets_examined"] += 1
            if isinstance(asset, dict):
                if asset.get("source") == "text_mention":
                    _stats["assets_filtered_text_mention"] += 1
                    continue
                if _safe_float(asset.get("similarity_score"), 0.0) <= 0.0:
                    _stats["assets_filtered_zero_score"] += 1
                    continue
                t = asset.get("ticker", "")
            else:
                t = str(asset)
            ticker = t if isinstance(t, str) else str(t or "")
            if ticker and not ticker.startswith("TOPIC:"):
                tickers.append(ticker)
                _stats["assets_retained"] += 1
            elif ticker.startswith("TOPIC:"):
                _stats["assets_filtered_topic"] += 1

        if not tickers:
            _stats["narratives_no_tickers"] += 1
            continue

        # Check price movement for first valid ticker
        label = None
        for ticker in tickers[:3]:  # Check up to 3 tickers
            try:
                prices = get_price_history(ticker, days=90)
            except Exception:
                continue

            if not prices:
                continue

            # Find peak date index
            peak_idx = None
            for i, p in enumerate(prices):
                if p.get("date") == peak_date_str:
                    peak_idx = i
                    break

            if peak_idx is None:
                # Try closest date
                try:
                    peak_dt = datetime.fromisoformat(peak_date_str).date()
                    min_diff = 999
                    for i, p in enumerate(prices):
                        try:
                            pd = datetime.fromisoformat(p["date"]).date()
                            diff = abs((pd - peak_dt).days)
                            if diff < min_diff:
                                min_diff = diff
                                peak_idx = i
                        except Exception:
                            continue
                    if min_diff > 3:
                        peak_idx = None
                except Exception:
                    pass

            if peak_idx is None:
                continue

            # Check 7-day price movement from peak
            start_price = prices[peak_idx]["close"]
            end_idx = min(peak_idx + 7, len(prices) - 1)
            if end_idx <= peak_idx:
                continue

            end_price = prices[end_idx]["close"]
            pct_change = (end_price - start_price) / start_price * 100

            # Label: did price move >2% in predicted direction?
            if dir_float > 0 and pct_change > 2.0:
                label = 1
            elif dir_float < 0 and pct_change < -2.0:
                label = 1
            else:
                label = 0
            break

        if label is None:
            continue

        # Build 15-feature vector
        entropy_raw = narrative.get("entropy")
        if entropy_raw is not None:
            try:
                entropy_normalized = min(float(entropy_raw) / _entropy_log_window, 1.0)
            except (TypeError, ValueError):
                entropy_normalized = 0.0
        else:
            entropy_normalized = 0.0

        features = [
            _safe_float(narrative.get("velocity_windowed")),
            _safe_float(narrative.get("inflow_velocity")),
            _safe_float(narrative.get("cross_source_score")),
            _safe_float(narrative.get("cohesion")),
            _safe_float(narrative.get("intent_weight")),
            _safe_float(narrative.get("centrality")),
            entropy_normalized,
            dir_float,
            confidence,
            certainty_to_float(certainty),
            magnitude_to_float(magnitude),
            _safe_float(narrative.get("source_escalation_velocity")),
            _safe_float(narrative.get("convergence_exposure")),
            _safe_float(narrative.get("catalyst_proximity_score")),
            _safe_float(narrative.get("macro_alignment")),
        ]

        X.append(features)
        y.append(label)
        _stats["samples_accepted"] += 1

    logger.info(
        "build_training_dataset: evaluated=%d assets_examined=%d "
        "filtered_text_mention=%d filtered_zero_score=%d "
        "filtered_topic=%d assets_retained=%d no_tickers=%d "
        "accepted=%d (X=%d, y=%d)",
        _stats["narratives_evaluated"],
        _stats["assets_examined"],
        _stats["assets_filtered_text_mention"],
        _stats["assets_filtered_zero_score"],
        _stats["assets_filtered_topic"],
        _stats["assets_retained"],
        _stats["narratives_no_tickers"],
        _stats["samples_accepted"],
        len(X), len(y),
    )
    return X, y


def _get_default_weights() -> dict:
    """Return the hand-tuned weights model as fallback."""
    return {
        "method": "default",
        "weights": [0.0] * len(FEATURE_NAMES),
        "intercept": 0.0,
        "n_samples": 0,
    }


def train_signal_model(X: list, y: list, min_samples: int = 30) -> dict:
    """
    Train a logistic regression model on the feature vectors.

    If fewer than min_samples training examples, return default weights.
    On any sklearn failure, fall back to default weights.
    """
    if len(X) < min_samples:
        return {
            "method": "default",
            "weights": [0.0] * len(FEATURE_NAMES),
            "intercept": 0.0,
            "n_samples": len(X),
        }

    try:
        from sklearn.linear_model import LogisticRegression
        import numpy as np

        X_arr = np.array(X, dtype=np.float64)
        y_arr = np.array(y, dtype=np.int64)

        # Handle case where all labels are the same
        if len(set(y)) < 2:
            return {
                "method": "default",
                "weights": [0.0] * len(FEATURE_NAMES),
                "intercept": 0.0,
                "n_samples": len(X),
            }

        model = LogisticRegression(max_iter=1000, solver="lbfgs")
        model.fit(X_arr, y_arr)

        accuracy = float(model.score(X_arr, y_arr))

        return {
            "method": "learned",
            "weights": list(model.coef_[0]),
            "intercept": float(model.intercept_[0]),
            "n_samples": len(X),
            "accuracy": accuracy,
            "trained_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        logger.warning("Signal model training failed, using defaults: %s", exc)
        return {
            "method": "default",
            "weights": [0.0] * len(FEATURE_NAMES),
            "intercept": 0.0,
            "n_samples": len(X),
        }


def compute_learned_ns_score(features: dict, model: dict) -> float:
    """
    Compute Ns score using either learned or default weights.

    If model["method"] == "default": use hand-tuned formula from compute_ns_score().
    If model["method"] == "learned": compute sigmoid(dot(weights, feature_vector) + intercept).
    Clamp to [0.0, 1.0].
    """
    from signals import compute_ns_score

    if model.get("method") != "learned":
        # Fall back to hand-tuned formula
        return compute_ns_score(
            velocity=_safe_float(features.get("velocity_windowed")),
            intent_weight=_safe_float(features.get("intent_weight")),
            cross_source_score=_safe_float(features.get("cross_source_score")),
            cohesion=_safe_float(features.get("cohesion")),
            polarization=_safe_float(features.get("polarization")),
            centrality=_safe_float(features.get("centrality")),
            entropy=features.get("entropy"),
            entropy_vocab_window=features.get("entropy_vocab_window", 10),
        )

    # Learned model: build feature vector in correct order
    feature_vector = []
    for name in FEATURE_NAMES:
        feature_vector.append(_safe_float(features.get(name)))

    weights = model.get("weights", [])
    intercept = float(model.get("intercept", 0.0))

    if len(weights) != len(feature_vector):
        logger.warning(
            "Weight/feature length mismatch (%d vs %d), falling back to default",
            len(weights), len(feature_vector),
        )
        return compute_ns_score(
            velocity=_safe_float(features.get("velocity_windowed")),
            intent_weight=_safe_float(features.get("intent_weight")),
            cross_source_score=_safe_float(features.get("cross_source_score")),
            cohesion=_safe_float(features.get("cohesion")),
            polarization=_safe_float(features.get("polarization")),
            centrality=_safe_float(features.get("centrality")),
            entropy=features.get("entropy"),
            entropy_vocab_window=features.get("entropy_vocab_window", 10),
        )

    # sigmoid(dot(weights, features) + intercept)
    z = sum(w * f for w, f in zip(weights, feature_vector)) + intercept
    # Clamp z to avoid overflow in exp
    z = max(-500, min(500, z))
    score = 1.0 / (1.0 + math.exp(-z))

    return max(0.0, min(1.0, score))


def load_or_train_model(repository, model_path: str, retrain_days: int = 7, min_samples: int = 30) -> dict:
    """
    Load cached model if fresh, otherwise retrain.

    If model_path exists and is less than retrain_days old, load it.
    Otherwise, call build_training_dataset() + train_signal_model().
    Save result to model_path (pickle).
    On any failure, return default weights model.
    """
    # Try loading cached model
    try:
        if os.path.exists(model_path):
            file_age_days = (time.time() - os.path.getmtime(model_path)) / 86400
            if file_age_days < retrain_days:
                cached = _safe_load(model_path, allowed={
                    "builtins": {"dict", "list", "tuple", "str", "int", "float", "bool"},
                })
                if isinstance(cached, dict) and "method" in cached:
                    logger.info("Loaded cached signal model (method=%s, age=%.1fd)",
                                cached.get("method"), file_age_days)
                    return cached
    except Exception as exc:
        logger.warning("Failed to load cached signal model: %s", exc)

    # Train new model
    try:
        X, y = build_training_dataset(repository)
        model = train_signal_model(X, y, min_samples=min_samples)

        # Save to disk
        try:
            os.makedirs(os.path.dirname(model_path) or ".", exist_ok=True)
            with open(model_path, "wb") as f:
                pickle.dump(model, f)
            logger.info("Saved signal model to %s (method=%s, n=%d)",
                        model_path, model.get("method"), model.get("n_samples", 0))
        except Exception as exc:
            logger.warning("Failed to save signal model: %s", exc)

        return model

    except Exception as exc:
        logger.warning("Signal model training failed entirely: %s", exc)
        return _get_default_weights()
