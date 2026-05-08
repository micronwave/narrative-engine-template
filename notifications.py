"""
Notification Manager — rules-based alerting on narrative and stock events.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from repository import SqliteRepository

logger = logging.getLogger(__name__)

try:
    from stock_data import get_price_history as _get_price_history
except ImportError:
    def _get_price_history(symbol: str, days: int = 30, **kwargs) -> list:  # type: ignore[misc]
        return []

RULE_TYPES = {
    "ns_above": "Narrative Ns score rises above threshold",
    "ns_below": "Narrative Ns score falls below threshold",
    "new_narrative": "New narrative emerges linked to ticker",
    "mutation": "Narrative mutates significantly",
    "stage_change": "Narrative changes lifecycle stage",
    "catalyst": "Narrative flagged as catalyst",
    # Phase 6 — price and technical alerts
    "price_above": "Price rises above threshold",
    "price_below": "Price falls below threshold",
    "pct_change": "Price changes by threshold% since previous close",
    "rsi_overbought": "RSI rises above threshold (default 70)",
    "rsi_oversold": "RSI falls below threshold (default 30)",
    "macd_crossover": "MACD line crosses above signal line (bullish)",
}

VALID_TARGET_TYPES = {"narrative", "ticker"}


# ---------------------------------------------------------------------------
# Technical indicator helpers (module-level, no external deps)
# ---------------------------------------------------------------------------

def _ema_series(values: list, period: int) -> list:
    """Exponential moving average series. Returns len(values) - period + 1 values."""
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1.0 - k))
    return result


def _compute_rsi(closes: list, period: int = 14) -> "float | None":
    """Simple RSI using SMA seed for first average."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-period:]
    avg_gain = sum(d for d in recent if d > 0) / period
    avg_loss = sum(-d for d in recent if d < 0) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def _compute_macd_crossover(closes: list, fast: int = 12, slow: int = 26, signal_period: int = 9) -> "bool | None":
    """Returns True if a bullish MACD crossover occurred at the latest bar."""
    if len(closes) < slow + signal_period + 1:
        return None
    fast_emas = _ema_series(closes, fast)
    slow_emas = _ema_series(closes, slow)
    aligned_fast = fast_emas[slow - fast:]
    macd_line = [f - s for f, s in zip(aligned_fast, slow_emas)]
    sig_emas = _ema_series(macd_line, signal_period)
    if len(sig_emas) < 2:
        return None
    prev_diff = macd_line[-2] - sig_emas[-2]
    curr_diff = macd_line[-1] - sig_emas[-1]
    return prev_diff < 0 and curr_diff > 0


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

    def check_rules(self, securities: "dict | None" = None) -> list[dict]:
        """Checks all enabled rules and creates notifications. Returns triggered list.

        Args:
            securities: optional dict mapping ticker symbol -> security dict
                        (with 'price'/'current_price' keys) for price-based alerts.
        """
        from api.services.notification_channels import DiscordWebhookChannel, EmailChannel, WebhookChannel
        from settings import settings as _settings

        triggered = []
        rules = self.repository.get_enabled_notification_rules()
        for rule in rules:
            if self.repository.has_notification_today(rule["id"]):
                continue
            notifications = self._check_rule(rule, securities=securities)
            for notification in notifications:
                self.repository.create_notification(notification)
                triggered.append(notification)
                title = notification["title"]
                message = notification["message"]
                metadata = {"rule_id": notification["rule_id"], "link": notification["link"]}
                if _settings.DISCORD_WEBHOOK_ENABLED and _settings.DISCORD_WEBHOOK_URL:
                    DiscordWebhookChannel(_settings.DISCORD_WEBHOOK_URL).send(title, message, metadata)
                if _settings.SMTP_HOST and _settings.SMTP_TO:
                    EmailChannel(
                        _settings.SMTP_HOST, _settings.SMTP_PORT,
                        _settings.SMTP_FROM, _settings.SMTP_TO, _settings.SMTP_PASSWORD
                    ).send(title, message, metadata)
                if _settings.NOTIFICATION_WEBHOOK_URL:
                    WebhookChannel(_settings.NOTIFICATION_WEBHOOK_URL).send(title, message, metadata)
        return triggered

    def _check_rule(self, rule: dict, securities: "dict | None" = None) -> list[dict]:
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

        # --- Phase 6: price and technical alerts ---

        elif rule_type == "price_above" and target_id and securities is not None:
            sec = securities.get(target_id.upper())
            if sec and threshold is not None:
                price = float(sec.get("price") or sec.get("current_price") or 0.0)
                if price > threshold:
                    return [self._create_notification(
                        rule,
                        f"Price Alert: {target_id.upper()}",
                        f"Price ${price:.2f} rose above ${threshold:.2f}",
                        f"/brief/{target_id.upper()}",
                    )]

        elif rule_type == "price_below" and target_id and securities is not None:
            sec = securities.get(target_id.upper())
            if sec and threshold is not None:
                price = float(sec.get("price") or sec.get("current_price") or 0.0)
                if price > 0 and price < threshold:
                    return [self._create_notification(
                        rule,
                        f"Price Alert: {target_id.upper()}",
                        f"Price ${price:.2f} fell below ${threshold:.2f}",
                        f"/brief/{target_id.upper()}",
                    )]

        elif rule_type == "pct_change" and target_id:
            history = _get_price_history(target_id.upper(), days=5)
            if len(history) >= 2 and threshold is not None:
                prev = history[-2]["close"]
                curr = history[-1]["close"]
                if prev > 0:
                    pct = (curr - prev) / prev * 100.0
                    if abs(pct) >= abs(threshold):
                        direction = "rose" if pct > 0 else "fell"
                        return [self._create_notification(
                            rule,
                            f"Price Change: {target_id.upper()}",
                            f"Price {direction} {abs(pct):.1f}% (threshold: {threshold:.1f}%)",
                            f"/brief/{target_id.upper()}",
                        )]

        elif rule_type == "rsi_overbought" and target_id:
            history = _get_price_history(target_id.upper(), days=30)
            closes = [h["close"] for h in history]
            rsi = _compute_rsi(closes)
            thr = threshold if threshold is not None else 70.0
            if rsi is not None and rsi > thr:
                return [self._create_notification(
                    rule,
                    f"RSI Overbought: {target_id.upper()}",
                    f"RSI at {rsi:.1f} (threshold: {thr:.0f})",
                    f"/brief/{target_id.upper()}",
                )]

        elif rule_type == "rsi_oversold" and target_id:
            history = _get_price_history(target_id.upper(), days=30)
            closes = [h["close"] for h in history]
            rsi = _compute_rsi(closes)
            thr = threshold if threshold is not None else 30.0
            if rsi is not None and rsi < thr:
                return [self._create_notification(
                    rule,
                    f"RSI Oversold: {target_id.upper()}",
                    f"RSI at {rsi:.1f} (threshold: {thr:.0f})",
                    f"/brief/{target_id.upper()}",
                )]

        elif rule_type == "macd_crossover" and target_id:
            history = _get_price_history(target_id.upper(), days=90)
            closes = [h["close"] for h in history]
            crossover = _compute_macd_crossover(closes)
            if crossover:
                return [self._create_notification(
                    rule,
                    f"MACD Crossover: {target_id.upper()}",
                    "Bullish MACD crossover detected",
                    f"/brief/{target_id.upper()}",
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

    def mark_read(self, notification_id: str, user_id: str | None = None) -> None:
        """Marks notification as read."""
        self.repository.mark_notification_read(notification_id, user_id)

    def mark_all_read(self, user_id: str = "local") -> None:
        """Marks all user's notifications as read."""
        self.repository.mark_all_notifications_read(user_id)

