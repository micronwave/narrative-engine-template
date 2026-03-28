"""
Haiku Chat System — persistent multi-turn Q&A for narratives and stocks.
"""

import json
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from llm_client import LlmClient
from repository import SqliteRepository
from settings import Settings

CHAT_TEMPLATES = {
    "compare_weekly": "Compare this narrative to how it looked 7 days ago. What changed and why does it matter?",
    "portfolio_impact": "How might this narrative affect a portfolio holding {ticker}? Be specific about risks and opportunities.",
    "summarize": "Summarize this narrative in 3 bullet points for someone who hasn't been following it.",
}

MAX_HISTORY_MESSAGES = 10


class ChatManager:
    def __init__(self, settings: Settings, repository: SqliteRepository, llm_client: LlmClient):
        self.settings = settings
        self.repository = repository
        self.llm_client = llm_client

    def create_session(self, user_id: str = "local", narrative_id: str = None, ticker: str = None) -> str:
        """Creates new chat session. Returns session_id."""
        session_id = str(uuid.uuid4())

        if narrative_id:
            narrative = self.repository.get_narrative(narrative_id)
            title = f"Chat: {narrative['name']}" if narrative else "Chat: Unknown Narrative"
        elif ticker:
            title = f"Chat: {ticker.upper()}"
        else:
            title = "General Chat"

        now = datetime.now(timezone.utc).isoformat()
        self.repository.create_chat_session({
            "id": session_id,
            "user_id": user_id,
            "narrative_id": narrative_id,
            "ticker": ticker,
            "title": title,
            "created_at": now,
            "updated_at": now,
            "is_active": 1,
        })
        return session_id

    def get_session(self, session_id: str) -> dict | None:
        """Gets session with messages list attached."""
        session = self.repository.get_chat_session(session_id)
        if session:
            session["messages"] = self.repository.get_chat_messages(session_id)
        return session

    def list_sessions(self, user_id: str = "local", limit: int = 20) -> list[dict]:
        """Lists user's chat sessions."""
        return self.repository.list_chat_sessions(user_id, limit)

    def send_message(self, session_id: str, user_message: str) -> dict:
        """Sends message, gets Haiku response, saves both. Returns response dict."""
        session = self.repository.get_chat_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        context = self._build_context(session)
        history = self.repository.get_chat_messages(session_id)[-MAX_HISTORY_MESSAGES:]

        messages = [
            {"role": m["role"], "content": m["content"]}
            for m in history
        ]
        messages.append({"role": "user", "content": user_message})

        system_prompt = (
            "You are a financial research assistant analyzing market narratives. "
            "Be concise, data-driven, and actionable. When giving investment opinions, "
            "include appropriate disclaimers.\n\n" + context
        )

        response = self.llm_client.call_haiku_chat(system_prompt, messages)
        now = datetime.now(timezone.utc).isoformat()

        self.repository.save_chat_message({
            "id": str(uuid.uuid4()),
            "session_id": session_id,
            "role": "user",
            "content": user_message,
            "tokens_used": 0,
            "cost": 0.0,
            "created_at": now,
        })
        self.repository.save_chat_message({
            "id": str(uuid.uuid4()),
            "session_id": session_id,
            "role": "assistant",
            "content": response["content"],
            "tokens_used": response["tokens"],
            "cost": response["cost"],
            "created_at": now,
        })
        self.repository.update_chat_session_timestamp(session_id)

        return {
            "content": response["content"],
            "tokens": response["tokens"],
            "cost": response["cost"],
            "session_id": session_id,
        }

    def apply_template(self, session_id: str, template_name: str, **kwargs) -> dict:
        """Applies a named template and sends it as the next message."""
        if template_name not in CHAT_TEMPLATES:
            raise ValueError(f"Unknown template: {template_name}. Available: {list(CHAT_TEMPLATES)}")
        message = CHAT_TEMPLATES[template_name].format_map(defaultdict(str, kwargs))
        return self.send_message(session_id, message)

    def get_templates(self) -> dict:
        """Returns available templates."""
        return CHAT_TEMPLATES

    def _build_context(self, session: dict) -> str:
        """Builds context string for system prompt from session's linked narrative/ticker."""
        parts = []

        if session.get("narrative_id"):
            narrative = self.repository.get_narrative(session["narrative_id"])
            if narrative:
                linked = narrative.get("linked_assets") or "[]"
                if isinstance(linked, str):
                    try:
                        linked = ", ".join(json.loads(linked))
                    except Exception:
                        linked = linked
                parts.append(
                    f"Current Narrative Context:\n"
                    f"Name: {narrative.get('name', 'Unknown')}\n"
                    f"Description: {narrative.get('description', 'N/A')}\n"
                    f"Ns Score: {float(narrative.get('ns_score') or 0):.2f}\n"
                    f"Lifecycle Stage: {narrative.get('stage', 'unknown')}\n"
                    f"Linked Assets: {linked}\n"
                    f"Document Count: {narrative.get('document_count', 0)}"
                )

        if session.get("ticker"):
            ticker = session["ticker"]
            narratives = self.repository.get_narratives_for_ticker(ticker)
            if narratives:
                parts.append(
                    f"Stock Context: {ticker}\n"
                    f"Linked Narratives: {len(narratives)}\n"
                    f"Top Narrative: {narratives[0]['name']}"
                )

        if not parts:
            parts.append("General financial analysis context. No specific narrative or stock selected.")

        return "\n\n".join(parts)
