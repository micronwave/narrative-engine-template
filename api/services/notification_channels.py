"""
Notification channel implementations for dispatching alerts to external systems.

Channels:
  InAppChannel        — default, delegates to NotificationManager (in-DB)
  DiscordWebhookChannel — sends embeds to a Discord channel via webhook URL
  EmailChannel        — sends plain-text alerts via SMTP
  WebhookChannel      — generic HTTP POST (Slack-compatible or custom)
"""

import logging
import smtplib
import ssl
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from email.message import EmailMessage

logger = logging.getLogger(__name__)

try:
    import requests as _requests
except ImportError:
    _requests = None  # type: ignore


class NotificationChannel(ABC):
    @abstractmethod
    def send(self, title: str, message: str, metadata: dict) -> bool:
        """Dispatch a notification. Returns True on success."""
        ...


class InAppChannel(NotificationChannel):
    """Default in-app channel — already handled by NotificationManager."""

    def send(self, title: str, message: str, metadata: dict) -> bool:
        # In-app notifications are persisted by NotificationManager.check_rules().
        # This channel is a no-op placeholder for interface completeness.
        return True


class DiscordWebhookChannel(NotificationChannel):
    """Send alert embeds to a Discord channel via webhook URL."""

    def __init__(self, webhook_url: str):
        self._url = webhook_url

    def send(self, title: str, message: str, metadata: dict) -> bool:
        if _requests is None:
            logger.warning("requests library not available; Discord notification skipped")
            return False
        color = 0xFF0000 if metadata.get("severity") == "critical" else 0x0099FF
        payload = {
            "embeds": [
                {
                    "title": title,
                    "description": message,
                    "color": color,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ]
        }
        try:
            resp = _requests.post(self._url, json=payload, timeout=10)
            return resp.status_code == 204
        except Exception as exc:
            logger.warning("DiscordWebhookChannel.send failed: %s", exc)
            return False


class EmailChannel(NotificationChannel):
    """Send plain-text alert emails via SMTP."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        from_addr: str,
        to_addr: str,
        password: str = "",
    ):
        self._host = smtp_host
        self._port = smtp_port
        self._from = from_addr
        self._to = to_addr
        self._password = password

    def send(self, title: str, message: str, metadata: dict) -> bool:
        try:
            msg = EmailMessage()
            msg["Subject"] = f"[Narrative Engine Alert] {title}"
            msg["From"] = self._from
            msg["To"] = self._to
            body = f"{title}\n\n{message}"
            if metadata:
                body += f"\n\nMetadata: {metadata}"
            msg.set_content(body)

            context = ssl.create_default_context()
            with smtplib.SMTP(self._host, self._port, timeout=15) as smtp:
                smtp.ehlo()
                smtp.starttls(context=context)
                if self._password:
                    smtp.login(self._from, self._password)
                smtp.send_message(msg)
            return True
        except Exception as exc:
            logger.warning("EmailChannel.send failed: %s", exc)
            return False


class WebhookChannel(NotificationChannel):
    """Generic HTTP POST webhook (Slack-compatible or custom endpoints)."""

    def __init__(self, url: str, headers: dict | None = None):
        self._url = url
        self._headers = headers or {}

    def send(self, title: str, message: str, metadata: dict) -> bool:
        if _requests is None:
            logger.warning("requests library not available; webhook notification skipped")
            return False
        payload = {"title": title, "message": message, **metadata}
        try:
            resp = _requests.post(self._url, json=payload, headers=self._headers, timeout=10)
            return resp.ok
        except Exception as exc:
            logger.warning("WebhookChannel.send failed: %s", exc)
            return False
