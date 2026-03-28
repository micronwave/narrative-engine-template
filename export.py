"""
Export Manager — JSON/CSV export and social share text generation.
"""

import csv
import io
import json
from datetime import datetime, date, timezone

from repository import SqliteRepository


class ExportManager:
    def __init__(self, repository: SqliteRepository, llm_client=None):
        self.repository = repository
        self.llm_client = llm_client

    def export_narratives_json(self, target_date: str = None) -> str:
        """Exports narratives as JSON string."""
        target_date = target_date or date.today().isoformat()
        narratives = self.repository.get_narratives_by_date(target_date)
        return json.dumps({
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "date": target_date,
            "narrative_count": len(narratives),
            "disclaimer": "INTELLIGENCE ONLY — NOT FINANCIAL ADVICE. For informational purposes only.",
            "narratives": narratives,
        }, indent=2)

    def export_narratives_csv(self, target_date: str = None) -> str:
        """Exports narratives as CSV string."""
        target_date = target_date or date.today().isoformat()
        narratives = self.repository.get_narratives_by_date(target_date)
        output = io.StringIO()
        fieldnames = [
            "narrative_id", "name", "description", "ns_score", "stage",
            "velocity", "entropy", "cohesion", "document_count", "linked_assets", "created_at",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for narrative in narratives:
            row = narrative.copy()
            linked = row.get("linked_assets", [])
            if isinstance(linked, str):
                try:
                    linked = json.loads(linked)
                except Exception:
                    linked = []
            row["linked_assets"] = ",".join(linked)
            writer.writerow(row)
        return output.getvalue()

    def generate_share_text(self, narrative_id: str, platform: str) -> str:
        """Generates platform-specific share text using Haiku."""
        narrative = self.repository.get_narrative(narrative_id)
        if not narrative:
            return ""

        platform_rules = {
            "twitter": "Max 280 characters. Use relevant hashtags like #FinTwit #Markets. Be punchy.",
            "linkedin": "Professional tone. 2-3 paragraphs. Include key metrics.",
            "discord": "Use markdown. Include score and linked assets in code blocks.",
        }
        if platform not in platform_rules:
            platform = "twitter"

        if not self.llm_client:
            return self._generate_fallback_share_text(narrative, platform)

        linked = narrative.get("linked_assets", [])
        if isinstance(linked, str):
            try:
                linked = json.loads(linked)
            except Exception:
                linked = []

        prompt = f"""Write a {platform} post about this financial narrative.

Name: {narrative['name']}
Description: {narrative.get('description', 'N/A')}
Ns Score: {narrative['ns_score']:.2f}
Stage: {narrative.get('stage', 'unknown')}
Linked Assets: {", ".join(linked)}

Rules: {platform_rules[platform]}

End with: "Not financial advice."

Write ONLY the post text."""

        result = self.llm_client.call_haiku("share_text", narrative_id, prompt)
        if result == "Analysis unavailable":
            return self._generate_fallback_share_text(narrative, platform)
        return result

    def _generate_fallback_share_text(self, narrative: dict, platform: str) -> str:
        """Generates share text without LLM."""
        name = narrative["name"]
        score = narrative["ns_score"]
        linked = narrative.get("linked_assets", [])
        if isinstance(linked, str):
            try:
                linked = json.loads(linked)
            except Exception:
                linked = []
        assets = ", ".join(linked[:5])

        if platform == "twitter":
            return f"Narrative Alert: {name}\n\nNs Score: {score:.2f}\nAssets: {assets}\n\n#FinTwit #Markets\n\nNot financial advice."
        elif platform == "linkedin":
            return f"""Market Narrative Alert: {name}

{narrative.get('description', '')}

Key Metrics:
- Narrative Score: {score:.2f}
- Linked Assets: {assets}

Not financial advice."""
        else:  # discord
            tickers = ", ".join(f"`{t}`" for t in linked)
            return f"""**{name}** | Ns: `{score:.2f}`

> {narrative.get('description', 'No description')}

**Linked Assets:** {tickers}

*Not financial advice*"""
