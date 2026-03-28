"""
Circuit breaker for data adapters.

Tracks consecutive failures per adapter. After 5 consecutive failures,
disables the adapter for 5 minutes. Logs state transitions.
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)

_FAILURE_THRESHOLD = 5
_RECOVERY_TIMEOUT = 300  # 5 minutes


class CircuitBreaker:
    """Per-adapter circuit breaker with failure counting and auto-recovery."""

    def __init__(self, name: str):
        self.name = name
        self._consecutive_failures = 0
        self._open_since: float = 0.0
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        with self._lock:
            if self._consecutive_failures < _FAILURE_THRESHOLD:
                return False
            elapsed = time.time() - self._open_since
            if elapsed >= _RECOVERY_TIMEOUT:
                logger.info("%s circuit CLOSED (re-enabled after %ds)", self.name, int(elapsed))
                self._consecutive_failures = 0
                self._open_since = 0.0
                return False
            return True

    def record_success(self) -> None:
        with self._lock:
            if self._consecutive_failures > 0:
                self._consecutive_failures = 0
                self._open_since = 0.0

    def record_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures == _FAILURE_THRESHOLD:
                self._open_since = time.time()
                logger.warning(
                    "%s circuit OPEN (%d consecutive failures, disabled for %ds)",
                    self.name,
                    _FAILURE_THRESHOLD,
                    _RECOVERY_TIMEOUT,
                )
