"""
Export Manager — JSON/CSV export and social share text generation.
"""

import json
from datetime import datetime, timezone

from prompt_utils import sanitize_for_prompt
from repository import SqliteRepository


def _tickers_from_linked(linked: list) -> list[str]:
    """Extract ticker strings from linked_assets (list of dicts or list of strings)."""
    if not linked:
        return []
    if isinstance(linked[0], dict):
        return [a["ticker"] for a in linked if isinstance(a, dict) and a.get("ticker")]
    return [str(t) for t in linked if t]


class ExportManager:
    def __init__(self, repository: SqliteRepository, llm_client=None):
        self.repository = repository
        self.llm_client = llm_client

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

        name = sanitize_for_prompt(narrative["name"], max_len=200)
        description = sanitize_for_prompt(narrative.get("description", "N/A"), max_len=500)
        stage = sanitize_for_prompt(narrative.get("stage", "unknown"), max_len=50)
        linked_assets = sanitize_for_prompt(", ".join(_tickers_from_linked(linked)), max_len=200)

        prompt = f"""Write a {platform} post about this financial narrative.

Name: {name}
Description: {description}
Ns Score: {narrative['ns_score']:.2f}
Stage: {stage}
Linked Assets: {linked_assets}

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
        assets = ", ".join(_tickers_from_linked(linked)[:5])

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
            tickers = ", ".join(f"`{t}`" for t in _tickers_from_linked(linked))
            return f"""**{name}** | Ns: `{score:.2f}`

> {narrative.get('description', 'No description')}

**Linked Assets:** {tickers}

*Not financial advice*"""
