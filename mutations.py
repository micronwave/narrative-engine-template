"""
Narrative mutation tracking: daily snapshots and overnight change detection.
Snapshot fields map to the real `narratives` table column names:
  - stage         (not lifecycle_stage)
  - document_count (not doc_count)
  - narrative_id   (primary key)
"""

import json
import logging
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

from llm_client import LlmClient
from repository import SqliteRepository
from settings import Settings

MUTATION_THRESHOLDS = {
    "score_spike": 0.15,
    "velocity_reversal": True,
    "stage_change": True,
    "doc_surge": 2.0,
    "new_sonnet": True,
}


class MutationDetector:
    def __init__(self, settings: Settings, repository: SqliteRepository,
                 llm_client: LlmClient,
                 narrative_assigned_docs: dict[str, list[str]] | None = None,
                 pipeline_run_id: str | None = None,
                 mutation_analyses: dict[str, str] | None = None):
        self.settings = settings
        self.repository = repository
        self.llm_client = llm_client
        self.narrative_assigned_docs = narrative_assigned_docs or {}
        self.pipeline_run_id = pipeline_run_id
        self.mutation_analyses = mutation_analyses or {}
        self._evidence_cache: dict[str, list[dict]] = {}

    def take_daily_snapshot(self, narrative_id: str) -> str:
        """Saves current narrative state to narrative_snapshots. Returns snapshot_id."""
        narrative = self.repository.get_narrative(narrative_id)
        if not narrative:
            return ""

        today = datetime.now(timezone.utc).date().isoformat()

        # Check if a snapshot already exists for today (upsert)
        existing = self.repository.get_snapshot(narrative_id, today)
        snapshot_id = existing["id"] if existing else str(uuid.uuid4())

        snapshot = {
            "id": snapshot_id,
            "narrative_id": narrative_id,
            "snapshot_date": today,
            "ns_score": narrative.get("ns_score"),
            "velocity": narrative.get("velocity"),
            "entropy": narrative.get("entropy"),
            "cohesion": narrative.get("cohesion"),
            "polarization": narrative.get("polarization"),
            # narratives table uses document_count, snapshots store as doc_count
            "doc_count": narrative.get("document_count"),
            # narratives table uses stage, snapshots store as lifecycle_stage
            "lifecycle_stage": narrative.get("stage"),
            "haiku_label": narrative.get("name"),
            "haiku_description": narrative.get("description"),
            "sonnet_analysis": self.mutation_analyses.get(narrative_id),
            # Signal validation metrics
            "burst_ratio": narrative.get("burst_ratio"),
            "intent_weight": narrative.get("intent_weight"),
            "cross_source_score": narrative.get("cross_source_score"),
            "centrality": narrative.get("centrality"),
            "velocity_windowed": narrative.get("velocity_windowed"),
            "public_interest": narrative.get("public_interest"),
            "sentiment_mean": narrative.get("sentiment_mean"),
            "sentiment_variance": narrative.get("sentiment_variance"),
            "source_count": narrative.get("source_count"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        self.repository.save_snapshot(snapshot)
        return snapshot_id

    def detect_mutations(self, narrative_id: str) -> list[dict]:
        """Compares today's snapshot to yesterday's. Returns list of mutation dicts."""
        today = datetime.now(timezone.utc).date().isoformat()
        yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

        today_snap = self.repository.get_snapshot(narrative_id, today)
        yesterday_snap = self.repository.get_snapshot(narrative_id, yesterday)

        if not today_snap or not yesterday_snap:
            return []

        mutations = []

        # score_spike: abs change > 0.15
        t_score = float(today_snap.get("ns_score") or 0.0)
        y_score = float(yesterday_snap.get("ns_score") or 0.0)
        if abs(t_score - y_score) > MUTATION_THRESHOLDS["score_spike"]:
            prev_str = str(round(y_score, 3))
            new_str = str(round(t_score, 3))
            explanation = self.generate_template_explanation(
                narrative_id, "score_spike", prev_str, new_str
            )
            if explanation is None:
                explanation = self.generate_llm_explanation(
                    narrative_id, "score_spike", prev_str, new_str
                )
            mutations.append(self._save_mutation(
                narrative_id, "score_spike",
                str(y_score), str(t_score), abs(t_score - y_score), explanation
            ))

        # velocity_reversal: sign change and both non-zero
        t_vel = float(today_snap.get("velocity") or 0.0)
        y_vel = float(yesterday_snap.get("velocity") or 0.0)
        if t_vel != 0 and y_vel != 0 and (t_vel > 0) != (y_vel > 0):
            prev_str = str(round(y_vel, 3))
            new_str = str(round(t_vel, 3))
            explanation = self.generate_template_explanation(
                narrative_id, "velocity_reversal", prev_str, new_str
            )
            if explanation is None:
                explanation = self.generate_llm_explanation(
                    narrative_id, "velocity_reversal", prev_str, new_str
                )
            mutations.append(self._save_mutation(
                narrative_id, "velocity_reversal",
                str(y_vel), str(t_vel), abs(t_vel - y_vel), explanation
            ))

        # stage_change
        t_stage = today_snap.get("lifecycle_stage") or ""
        y_stage = yesterday_snap.get("lifecycle_stage") or ""
        if t_stage and y_stage and t_stage != y_stage:
            explanation = self.generate_template_explanation(
                narrative_id, "stage_change", y_stage, t_stage
            )
            mutations.append(self._save_mutation(
                narrative_id, "stage_change",
                y_stage, t_stage, 1.0, explanation
            ))

        # doc_surge: today > yesterday * 2.0
        t_docs = int(today_snap.get("doc_count") or 0)
        y_docs = int(yesterday_snap.get("doc_count") or 0)
        if y_docs > 0 and t_docs > y_docs * MUTATION_THRESHOLDS["doc_surge"]:
            explanation = self.generate_template_explanation(
                narrative_id, "doc_surge", str(y_docs), str(t_docs)
            )
            if explanation is None:
                explanation = self.generate_llm_explanation(
                    narrative_id, "doc_surge", str(y_docs), str(t_docs)
                )
            mutations.append(self._save_mutation(
                narrative_id, "doc_surge",
                str(y_docs), str(t_docs), t_docs / y_docs, explanation
            ))

        # new_sonnet: today has sonnet_analysis, yesterday did not
        t_sonnet = today_snap.get("sonnet_analysis")
        y_sonnet = yesterday_snap.get("sonnet_analysis")
        if t_sonnet and not y_sonnet:
            explanation = self.generate_template_explanation(
                narrative_id, "new_sonnet", "none", "sonnet analysis generated"
            )
            mutations.append(self._save_mutation(
                narrative_id, "new_sonnet",
                "none", "sonnet_analysis_generated", 1.0, explanation
            ))

        return mutations

    def _save_mutation(self, narrative_id: str, mutation_type: str,
                       prev: str, new: str, magnitude: float,
                       explanation: str) -> dict:
        mutation = {
            "id": str(uuid.uuid4()),
            "narrative_id": narrative_id,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "mutation_type": mutation_type,
            "previous_value": prev,
            "new_value": new,
            "magnitude": magnitude,
            "haiku_explanation": explanation,
            "contributing_documents": self._get_contributing_docs_json(narrative_id),
            "pipeline_run_id": self.pipeline_run_id,
        }
        self.repository.save_mutation(mutation)
        return mutation

    # --- Template Explanation Engine ---

    def generate_template_explanation(self, narrative_id: str, mutation_type: str,
                                      old_val: str, new_val: str) -> str | None:
        """Generate a template-based explanation. Returns None if ambiguous (falls to LLM)."""
        doc_ids = self.narrative_assigned_docs.get(narrative_id, [])
        doc_count = len(doc_ids)
        source_breakdown = self._build_source_breakdown(narrative_id, doc_ids)

        if mutation_type == "score_spike":
            try:
                old_f, new_f = float(old_val), float(new_val)
                pct = ((new_f - old_f) / abs(old_f) * 100) if old_f != 0 else 0
                direction = "increased" if new_f > old_f else "decreased"
                msg = f"Ns score {direction} from {old_f:.3f} to {new_f:.3f} ({pct:+.0f}%)."
                if doc_count:
                    msg += f" {doc_count} new documents ingested from {source_breakdown}."
                return msg
            except (ValueError, ZeroDivisionError):
                return None

        if mutation_type == "velocity_reversal":
            try:
                old_f, new_f = float(old_val), float(new_val)
                direction = "positive" if new_f > 0 else "negative"
                msg = f"Velocity reversed from {old_f:.3f} to {new_f:.3f} (now {direction})."
                if doc_count:
                    msg += f" {doc_count} new documents from {source_breakdown}."
                return msg
            except ValueError:
                return None

        if mutation_type == "stage_change":
            msg = f"Stage transitioned from {old_val} to {new_val}."
            narrative = self.repository.get_narrative(narrative_id)
            if narrative:
                doc_total = narrative.get("document_count") or 0
                vel = float(narrative.get("velocity") or 0)
                msg += f" Document count at {doc_total} with velocity at {vel:.4f}."
            return msg

        if mutation_type == "doc_surge":
            try:
                old_i, new_i = int(old_val), int(new_val)
                ratio = new_i / old_i if old_i > 0 else 0
                msg = f"Document count surged from {old_i} to {new_i} ({ratio:.1f}x)."
                if doc_count:
                    msg += f" New documents from {source_breakdown}."
                return msg
            except (ValueError, ZeroDivisionError):
                return None

        if mutation_type == "new_sonnet":
            return "Sonnet deep analysis generated for this narrative."

        logger.warning("Unknown mutation type %r — no template available", mutation_type)
        return None

    def _build_source_breakdown(self, narrative_id: str, doc_ids: list[str]) -> str:
        """Returns 'reuters.com (3), bloomberg.com (2)' style string."""
        if not doc_ids:
            return "unknown sources"
        evidence = self._get_cached_evidence(narrative_id, doc_ids)
        domain_counts = Counter(e.get("source_domain") or "unknown" for e in evidence)
        parts = [f"{domain} ({count})" for domain, count in domain_counts.most_common(5)]
        return ", ".join(parts) if parts else "mixed sources"

    def _get_contributing_docs_json(self, narrative_id: str) -> str | None:
        """Build JSON list of contributing documents for this mutation."""
        doc_ids = self.narrative_assigned_docs.get(narrative_id, [])
        if not doc_ids:
            return None
        evidence = self._get_cached_evidence(narrative_id, doc_ids)
        contrib = []
        for e in evidence:
            contrib.append({
                "doc_id": e.get("doc_id"),
                "source_domain": e.get("source_domain"),
                "excerpt_title": (e.get("excerpt") or "")[:80],
                "published_at": e.get("published_at"),
            })
        return json.dumps(contrib)

    def _get_cached_evidence(self, narrative_id: str, doc_ids: list[str]) -> list[dict]:
        """Cache document evidence per narrative to avoid redundant DB queries."""
        if narrative_id not in self._evidence_cache:
            self._evidence_cache[narrative_id] = (
                self.repository.get_document_evidence_by_ids(doc_ids)
            )
        return self._evidence_cache[narrative_id]

    # --- LLM Explanation (fallback for ambiguous cases) ---

    def generate_llm_explanation(self, narrative_id: str, mutation_type: str,
                                  old_val: str, new_val: str) -> str:
        """Calls Haiku to explain the mutation in 2-3 sentences."""
        narrative = self.repository.get_narrative(narrative_id)
        if not narrative:
            return "Analysis unavailable"

        prompt = f"""A financial narrative mutated overnight.

Narrative: {narrative["name"]}
Change: {mutation_type}
Before: {old_val}
After: {new_val}

In 2-3 sentences, explain what likely caused this change and what it means for investors."""

        return self.llm_client.call_haiku("mutation_explanation", narrative_id, prompt)

    def compare_snapshots(self, narrative_id: str, date1: str, date2: str) -> dict:
        """Returns side-by-side comparison of two snapshots."""
        snap1 = self.repository.get_snapshot(narrative_id, date1)
        snap2 = self.repository.get_snapshot(narrative_id, date2)
        narrative = self.repository.get_narrative(narrative_id)
        narrative_name = narrative["name"] if narrative else narrative_id

        differences = []
        if snap1 and snap2:
            for key in ["ns_score", "velocity", "lifecycle_stage", "doc_count",
                        "entropy", "cohesion", "polarization"]:
                v1 = snap1.get(key)
                v2 = snap2.get(key)
                if v1 != v2:
                    differences.append({"field": key, "date1_value": v1, "date2_value": v2})

        return {
            "narrative_name": narrative_name,
            "date1_data": snap1,
            "date2_data": snap2,
            "differences": differences,
        }

    def get_story_timeline(self, narrative_id: str, days: int = 7) -> list[dict]:
        """Returns daily snapshots for the last N days."""
        end_date = datetime.now(timezone.utc).date().isoformat()
        start_date = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
        return self.repository.get_snapshots_range(narrative_id, start_date, end_date)

    def generate_mutation_summary(self) -> dict:
        """Returns today's mutation summary for dashboard banner."""
        mutations = self.repository.get_mutations_today()

        if not mutations:
            return {
                "mutations_today": 0,
                "narratives_mutated": [],
                "most_significant": None,
                "sonnet_analyses_generated": 0,
            }

        narrative_cache: dict[str, dict | None] = {}
        narratives_mutated = []
        seen_ids: set = set()
        most_sig = None
        sonnet_count = 0

        for m in mutations:
            nid = m["narrative_id"]
            if nid not in narrative_cache:
                narrative_cache[nid] = self.repository.get_narrative(nid)
            narrative = narrative_cache[nid]

            if nid not in seen_ids:
                if narrative:
                    narratives_mutated.append(narrative["name"])
                seen_ids.add(nid)

            if m["mutation_type"] == "new_sonnet":
                sonnet_count += 1

            if most_sig is None or (m.get("magnitude") or 0) > (most_sig.get("magnitude") or 0):
                most_sig = {
                    "narrative_id": nid,
                    "narrative_name": narrative["name"] if narrative else nid,
                    "mutation_type": m["mutation_type"],
                    "magnitude": m.get("magnitude"),
                    "explanation": m.get("haiku_explanation"),
                }

        return {
            "mutations_today": len(mutations),
            "narratives_mutated": narratives_mutated,
            "most_significant": most_sig,
            "sonnet_analyses_generated": sonnet_count,
        }
