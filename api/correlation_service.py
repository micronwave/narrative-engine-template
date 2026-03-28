"""
F6 — Velocity-Price Correlation Service

Computes Pearson correlation between narrative metrics and
price changes with configurable lead time.
"""

import math
import warnings

from scipy.stats import pearsonr


def compute_velocity_price_correlation(
    velocity_history: list[dict],
    price_history: list[dict],
    lead_days: int = 1,
) -> dict:
    """
    Computes Pearson correlation between velocity changes (day N)
    and price changes (day N + lead_days).

    velocity_history: [{date, velocity, ...}] ordered by date ascending
    price_history: [{date, close, change_pct}] ordered by date ascending
    lead_days: how many days velocity leads price (1 = next day)

    Returns dict with correlation, p_value, n_observations, is_significant,
    lead_days, and human-readable interpretation.
    """
    MIN_OBSERVATIONS = 30

    if lead_days < 0:
        return {
            "correlation": 0.0, "p_value": 1.0, "n_observations": 0,
            "is_significant": False, "lead_days": lead_days,
            "interpretation": "lead_days must be non-negative",
        }

    # Build date-indexed lookups (skip entries missing required keys)
    vel_by_date = {}
    for v in velocity_history:
        d = v.get("date")
        vel = v.get("velocity")
        if d is not None and vel is not None:
            vel_by_date[d] = vel

    price_by_date = {}
    for p in price_history:
        d = p.get("date")
        chg = p.get("change_pct")
        if d is not None and chg is not None:
            price_by_date[d] = chg

    # Align: for each velocity date, find the price date at +lead_days
    price_dates = sorted(price_by_date.keys())
    # O(n) index lookup via dict instead of O(n) list.index() per call
    price_date_to_idx = {d: i for i, d in enumerate(price_dates)}

    # Build pairs
    vel_values = []
    price_values = []

    for vd in sorted(vel_by_date.keys()):
        vd_idx = price_date_to_idx.get(vd)
        if vd_idx is not None:
            target_idx = vd_idx + lead_days
            if target_idx < len(price_dates):
                target_date = price_dates[target_idx]
                vel_values.append(vel_by_date[vd])
                price_values.append(price_by_date[target_date])

    n = len(vel_values)

    if n < 2:
        return {
            "correlation": 0.0,
            "p_value": 1.0,
            "n_observations": n,
            "is_significant": False,
            "lead_days": lead_days,
            "interpretation": f"Insufficient data ({n} observations, need {MIN_OBSERVATIONS}+)",
        }

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r, p = pearsonr(vel_values, price_values)
        if math.isnan(r):
            r, p = 0.0, 1.0
    except Exception:
        r, p = 0.0, 1.0

    is_significant = bool(p < 0.05 and n >= MIN_OBSERVATIONS)

    # Interpretation
    abs_r = abs(r)
    if n < MIN_OBSERVATIONS:
        interpretation = f"Collecting data — {n}/{MIN_OBSERVATIONS} days accumulated"
    elif abs_r < 0.1:
        interpretation = "No meaningful correlation detected"
    elif abs_r < 0.3:
        interpretation = f"Weak {'positive' if r > 0 else 'negative'} correlation (r={r:.3f})"
    elif abs_r < 0.5:
        interpretation = f"Moderate {'positive' if r > 0 else 'negative'} correlation (r={r:.3f})"
    elif abs_r < 0.7:
        interpretation = f"Strong {'positive' if r > 0 else 'negative'} correlation (r={r:.3f})"
    else:
        interpretation = f"Very strong {'positive' if r > 0 else 'negative'} correlation (r={r:.3f})"

    if is_significant:
        interpretation += " — statistically significant (p<0.05)"
    elif n >= MIN_OBSERVATIONS:
        interpretation += f" — not significant (p={p:.3f})"

    return {
        "correlation": round(float(r), 4),
        "p_value": round(float(p), 4),
        "n_observations": n,
        "is_significant": is_significant,
        "lead_days": lead_days,
        "interpretation": interpretation,
    }


# ---------------------------------------------------------------------------
# Generalized metric-price correlation (Phase 0 signal validation)
# ---------------------------------------------------------------------------

# Snapshot metrics available for correlation analysis.
SNAPSHOT_METRICS = [
    "velocity", "ns_score", "burst_ratio", "cohesion", "entropy",
    "polarization", "sentiment_mean", "sentiment_variance",
    "doc_count", "source_count", "intent_weight", "cross_source_score",
    "centrality", "velocity_windowed", "public_interest",
]


def compute_metric_price_correlation(
    metric_history: list[dict],
    price_history: list[dict],
    metric_key: str = "velocity",
    lead_days: int = 0,
    min_observations: int = 30,
) -> dict:
    """
    Generalized version of compute_velocity_price_correlation.
    Correlates any snapshot metric against price changes.

    metric_history: [{date, <metric_key>: value, ...}] ordered by date ascending
    price_history: [{date, close, change_pct}] ordered by date ascending
    metric_key: which field to extract from metric_history dicts
    lead_days: how many trading days the metric leads price (0 = same day)
    min_observations: minimum paired observations for significance

    Returns dict with correlation, p_value, n_observations, is_significant,
    lead_days, metric_name, and human-readable interpretation.
    """
    if lead_days < 0:
        return {
            "correlation": 0.0, "p_value": 1.0, "n_observations": 0,
            "is_significant": False, "lead_days": lead_days,
            "metric_name": metric_key,
            "interpretation": "lead_days must be non-negative",
        }

    # Build date-indexed lookups
    metric_by_date = {}
    for entry in metric_history:
        d = entry.get("date")
        val = entry.get(metric_key)
        if d is not None and val is not None:
            try:
                metric_by_date[d] = float(val)
            except (ValueError, TypeError):
                continue

    price_by_date = {}
    for p in price_history:
        d = p.get("date")
        chg = p.get("change_pct")
        if d is not None and chg is not None:
            try:
                price_by_date[d] = float(chg)
            except (ValueError, TypeError):
                continue

    # Align: for each metric date, find the price date at +lead_days
    price_dates = sorted(price_by_date.keys())
    price_date_to_idx = {d: i for i, d in enumerate(price_dates)}

    metric_values = []
    price_values = []

    for md in sorted(metric_by_date.keys()):
        md_idx = price_date_to_idx.get(md)
        if md_idx is not None:
            target_idx = md_idx + lead_days
            if target_idx < len(price_dates):
                metric_values.append(metric_by_date[md])
                price_values.append(price_by_date[price_dates[target_idx]])

    n = len(metric_values)

    if n < 2:
        return {
            "correlation": 0.0, "p_value": 1.0, "n_observations": n,
            "is_significant": False, "lead_days": lead_days,
            "metric_name": metric_key,
            "interpretation": f"Insufficient data ({n} observations, need {min_observations}+)",
        }

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r, p = pearsonr(metric_values, price_values)
        if math.isnan(r):
            r, p = 0.0, 1.0
    except Exception:
        r, p = 0.0, 1.0

    is_significant = bool(p < 0.05 and n >= min_observations)

    abs_r = abs(r)
    if n < min_observations:
        interpretation = f"Collecting data — {n}/{min_observations} days accumulated"
    elif abs_r < 0.1:
        interpretation = "No meaningful correlation detected"
    elif abs_r < 0.3:
        interpretation = f"Weak {'positive' if r > 0 else 'negative'} correlation (r={r:.3f})"
    elif abs_r < 0.5:
        interpretation = f"Moderate {'positive' if r > 0 else 'negative'} correlation (r={r:.3f})"
    elif abs_r < 0.7:
        interpretation = f"Strong {'positive' if r > 0 else 'negative'} correlation (r={r:.3f})"
    else:
        interpretation = f"Very strong {'positive' if r > 0 else 'negative'} correlation (r={r:.3f})"

    if is_significant:
        interpretation += " — statistically significant (p<0.05)"
    elif n >= min_observations:
        interpretation += f" — not significant (p={p:.3f})"

    return {
        "correlation": round(float(r), 4),
        "p_value": round(float(p), 4),
        "n_observations": n,
        "is_significant": is_significant,
        "lead_days": lead_days,
        "metric_name": metric_key,
        "interpretation": interpretation,
    }
