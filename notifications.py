"""
Notification Manager — rules-based alerting on narrative and stock events.
"""

import json
import uuid
from datetime import datetime, timezone

from repository import SqliteRepository

RULE_TYPES = {
    "ns_above": "Narrative Ns score rises above threshold",
    "ns_below": "Narrative Ns score falls below threshold",
    "new_narrative": "New narrative emerges linked to ticker",
    "mutation": "Narrative mutates significantly",
    "stage_change": "Narrative changes lifecycle stage",
    "catalyst": "Narrative flagged as catalyst",
}

VALID_TARGET_TYPES = {"narrative", "ticker"}


class NotificationManager:
    def __init__(self, repository: SqliteRepository):
        self.repository = repository

    def create_rule(self, user_id: str, rule_type: str, target_type: str,
                    target_id: str = None, threshold: float = None) -> str:
        """Creates notification rule. Returns rule_id."""
        if rule_type not in RULE_TYPES:
            raise ValueError(f"Invalid rule_type. Must be one of: {list(RULE_TYPES.keys())}")
        if target_type not in VALID_TARGET_TYPES:
            raise ValueError(f"Invalid target_type. Must be one of: {sorted(VALID_TARGET_TYPES)}")
        rule_id = str(uuid.uuid4())
        self.repository.create_notification_rule({
            "id": rule_id,
            "user_id": user_id,
            "rule_type": rule_type,
            "target_type": target_type,
            "target_id": target_id,
            "threshold": threshold,
            "enabled": 1,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        return rule_id

    def toggle_rule(self, rule_id: str, enabled: bool) -> None:
        """Enables or disables rule."""
        self.repository.update_notification_rule_enabled(rule_id, enabled)

    def delete_rule(self, rule_id: str) -> None:
        """Deletes rule."""
        self.repository.delete_notification_rule(rule_id)

    def list_rules(self, user_id: str = "local") -> list[dict]:
        """Lists user's rules."""
        return self.repository.list_notification_rules(user_id)

    def check_rules(self) -> list[dict]:
        """Checks all enabled rules and creates notifications. Returns triggered list."""
        triggered = []
        rules = self.repository.get_enabled_notification_rules()
        for rule in rules:
            if self.repository.has_notification_today(rule["id"]):
                continue
            notifications = self._check_rule(rule)
            for notification in notifications:
                self.repository.create_notification(notification)
                triggered.append(notification)
        return triggered

    def _check_rule(self, rule: dict) -> list[dict]:
        """Checks single rule. Returns list of notification dicts (empty if not triggered)."""
        rule_type = rule["rule_type"]
        target_id = rule["target_id"]
        threshold = rule["threshold"]

        if rule_type == "ns_above" and target_id:
            narrative = self.repository.get_narrative(target_id)
            if narrative and threshold is not None and float(narrative.get("ns_score") or 0) > threshold:
                return [self._create_notification(
                    rule,
                    f"Narrative Alert: {narrative['name']}",
                    f"Ns score rose to {float(narrative['ns_score'] or 0):.2f} (threshold: {threshold})",
                    f"/narrative/{target_id}",
                )]

        elif rule_type == "ns_below" and target_id:
            narrative = self.repository.get_narrative(target_id)
            if narrative and threshold is not None and float(narrative.get("ns_score") or 0) < threshold:
                return [self._create_notification(
                    rule,
                    f"Narrative Alert: {narrative['name']}",
                    f"Ns score dropped to {float(narrative['ns_score'] or 0):.2f} (threshold: {threshold})",
                    f"/narrative/{target_id}",
                )]

        elif rule_type == "new_narrative" and target_id:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            new_narratives = self.repository.get_narratives_created_on_date(today)
            results = []
            for narrative in new_narratives:
                linked = narrative.get("linked_assets", [])
                if isinstance(linked, str):
                    try:
                        linked = json.loads(linked)
                    except Exception:
                        linked = []
                if target_id.upper() in linked:
                    nid = narrative["narrative_id"]
                    results.append(self._create_notification(
                        rule,
                        f"New Narrative: {narrative['name']}",
                        f"Affects your watched ticker {target_id}",
                        f"/narrative/{nid}",
                    ))
            return results

        elif rule_type == "mutation" and target_id:
            mutations = self.repository.get_mutations_today_for_narrative(target_id)
            if mutations:
                narrative = self.repository.get_narrative(target_id)
                return [self._create_notification(
                    rule,
                    f"Narrative Mutated: {narrative['name'] if narrative else 'Unknown'}",
                    f"Detected {len(mutations)} mutation(s)",
                    f"/narrative/{target_id}",
                )]

        elif rule_type == "stage_change" and target_id:
            mutations = self.repository.get_mutations_today_for_narrative(target_id)
            stage_changes = [m for m in mutations if m["mutation_type"] == "stage_change"]
            if stage_changes:
                narrative = self.repository.get_narrative(target_id)
                m = stage_changes[0]
                return [self._create_notification(
                    rule,
                    f"Stage Change: {narrative['name'] if narrative else 'Unknown'}",
                    f"Changed from {m['previous_value']} to {m['new_value']}",
                    f"/narrative/{target_id}",
                )]

        elif rule_type == "catalyst" and target_id:
            narrative = self.repository.get_narrative(target_id)
            if narrative and narrative.get("is_catalyst"):
                return [self._create_notification(
                    rule,
                    f"Catalyst Detected: {narrative['name']}",
                    "Narrative flagged as market catalyst",
                    f"/narrative/{target_id}",
                )]

        return []

    def _create_notification(self, rule: dict, title: str, message: str, link: str) -> dict:
        """Creates notification dict."""
        return {
            "id": str(uuid.uuid4()),
            "user_id": rule["user_id"],
            "rule_id": rule["id"],
            "title": title,
            "message": message,
            "link": link,
            "is_read": 0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def get_notifications(self, user_id: str = "local", unread_only: bool = False) -> list[dict]:
        """Gets user's notifications."""
        return self.repository.get_notifications(user_id, unread_only)

    def mark_read(self, notification_id: str) -> None:
        """Marks notification as read."""
        self.repository.mark_notification_read(notification_id)

    def mark_all_read(self, user_id: str = "local") -> None:
        """Marks all user's notifications as read."""
        self.repository.mark_all_notifications_read(user_id)

    def get_rule_types(self) -> dict:
        """Returns available rule types."""
        return RULE_TYPES
