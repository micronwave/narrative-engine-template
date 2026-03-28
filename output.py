"""
The disclaimer field is mandatory and immutable on all output objects.
This system provides intelligence signals only and must never be used to
generate financial advice, buy/sell recommendations, or price targets.
"""

import json
import logging
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DISCLAIMER = "INTELLIGENCE ONLY — NOT FINANCIAL ADVICE. For informational purposes only."

_LIFECYCLE_FALLBACK = "Classification based on automated metrics."


def build_output_object(
    narrative: dict,
    linked_assets: list[dict],
    supporting_evidence: list[dict],
    lifecycle_reasoning: str,
    mutation_analysis: str | None,
    score_components: dict,
) -> dict:
    """
    Assemble a single narrative output object conforming to the output JSON schema.
    The DISCLAIMER is always injected regardless of caller-supplied data.
    """
    if not lifecycle_reasoning:
        lifecycle_reasoning = _LIFECYCLE_FALLBACK

    # Normalize supporting evidence and truncate excerpts to 280 chars.
    evidence_out: list[dict] = [
        {
            "source_url": e.get("source_url", ""),
            "source_domain": e.get("source_domain", ""),
            "published_at": e.get("published_at", ""),
            "author": e.get("author"),
            "excerpt": (e.get("excerpt") or "")[:280],
        }
        for e in supporting_evidence
    ]

    # source_attribution_metadata — domains must cover ALL unique domains from evidence.
    domains = sorted({e["source_domain"] for e in evidence_out if e["source_domain"]})
    date_values = [e["published_at"] for e in evidence_out if e.get("published_at")]
    date_range_start = min(date_values) if date_values else None
    date_range_end = max(date_values) if date_values else None

    # reasoning_trace: one entry per score component.
    reasoning_trace: list[dict] = [
        {"evidence": key, "source_url": "", "contribution": float(val or 0)}
        for key, val in score_components.items()
    ]

    return {
        "narrative_id": narrative["narrative_id"],
        "name": narrative.get("name") or "",
        "description": narrative.get("description") or "",
        "stage": narrative.get("stage", "Emerging"),
        "velocity": float(narrative.get("velocity") or 0.0),
        "velocity_windowed": float(narrative.get("velocity_windowed") or 0.0),
        "centrality": float(narrative.get("centrality") or 0.0),
        "is_catalyst": bool(narrative.get("is_catalyst", 0)),
        "is_coordinated": bool(narrative.get("is_coordinated", 0)),
        "coordination_penalty_applied": bool(narrative.get("is_coordinated", 0)),
        "suppressed": bool(narrative.get("suppressed", 0)),
        "human_review_required": bool(narrative.get("human_review_required", 0)),
        "narrative_strength_score": float(narrative.get("ns_score") or 0.0),
        "score_components": score_components,
        "entropy": float(narrative["entropy"]) if narrative.get("entropy") is not None else None,
        "intent_weight": float(narrative.get("intent_weight") or 0.0),
        "lifecycle_reasoning": lifecycle_reasoning,
        "mutation_analysis": mutation_analysis,
        "linked_assets": linked_assets,
        "cross_source_score": float(narrative.get("cross_source_score") or 0.0),
        "reasoning_trace": reasoning_trace,
        "supporting_evidence": evidence_out,
        "source_attribution_metadata": {
            "domains": domains,
            "total_document_count": int(narrative.get("document_count") or 0),
            "date_range_start": date_range_start,
            "date_range_end": date_range_end,
        },
        "disclaimer": DISCLAIMER,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
    }


def validate_output(output: dict) -> bool:
    """
    Validate an output object before emission.

    Checks (in order):
    1. disclaimer field is present and exactly matches DISCLAIMER constant.
    2. source_attribution_metadata.domains is non-empty when supporting_evidence is non-empty.
    3. narrative_id is a valid UUID.

    Returns True if valid. Logs ERROR and returns False if any check fails.
    """
    nid = output.get("narrative_id")

    # Check 1: disclaimer
    if output.get("disclaimer") != DISCLAIMER:
        logger.error(
            "validate_output: disclaimer missing or incorrect for narrative_id=%s", nid
        )
        return False

    # Check 2: domains non-empty when evidence present
    has_evidence = bool(output.get("supporting_evidence"))
    has_domains = bool(output.get("source_attribution_metadata", {}).get("domains"))
    if has_evidence and not has_domains:
        logger.error(
            "validate_output: source_attribution_metadata.domains is empty "
            "despite non-empty supporting_evidence for narrative_id=%s",
            nid,
        )
        return False

    # Check 3: valid UUID
    try:
        uuid.UUID(str(nid))
    except (ValueError, AttributeError):
        logger.error(
            "validate_output: narrative_id=%r is not a valid UUID", nid
        )
        return False

    return True


def write_outputs(outputs: list[dict], date: str) -> None:
    """
    Write validated narrative output objects to:
      ./data/outputs/{date}/narratives.json   (file)
      stdout                                  (print)

    If outputs is empty, emits [] and logs INFO.
    If a narrative fails validation it is excluded (pipeline does not crash).
    """
    if not outputs:
        logger.info("No active narratives to emit.")

    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
        raise ValueError(f"Invalid date format for output path: {date!r}")

    out_dir = Path("./data/outputs") / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "narratives.json"

    serialized = json.dumps(outputs, indent=2, ensure_ascii=False, default=str)

    tmp_fd, tmp_path = tempfile.mkstemp(dir=str(out_dir), suffix=".json.tmp")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            f.write(serialized)
        os.replace(tmp_path, str(out_path))
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    print(serialized.encode("utf-8", errors="replace").decode("utf-8"))
    logger.info("Emitted %d narrative(s) to %s", len(outputs), out_path)
