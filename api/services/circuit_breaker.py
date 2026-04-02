"""
Circuit breaker for data adapters.

Tracks consecutive failures per adapter. After 10 consecutive failures from
2+ distinct sources, disables the adapter for 5 minutes. Logs state transitions.
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)

_FAILURE_THRESHOLD = 10  # increased from 5
_MIN_FAILURE_SOURCES = 2  # require failures from 2+ sources
_RECOVERY_TIMEOUT = 300  # 5 minutes


class CircuitBreaker:
    """Per-adapter circuit breaker with failure counting and auto-recovery."""

    def __init__(self, name: str):
        self.name = name
        self._consecutive_failures = 0
        self._failure_sources: set = set()
        self._open_since: float = 0.0
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        with self._lock:
            if self._consecutive_failures < _FAILURE_THRESHOLD:
                return False
            if len(self._failure_sources) < _MIN_FAILURE_SOURCES:
                return False
            elapsed = time.time() - self._open_since
            if elapsed >= _RECOVERY_TIMEOUT:
                logger.info("%s circuit CLOSED (re-enabled after %ds)", self.name, int(elapsed))
                self._consecutive_failures = 0
                self._failure_sources.clear()
                self._open_since = 0.0
                return False
            return True

    def record_success(self) -> None:
        with self._lock:
            if self._consecutive_failures > 0:
                self._consecutive_failures = 0
                self._failure_sources.clear()
                self._open_since = 0.0

    def record_failure(self, source: str = "unknown") -> None:
        with self._lock:
            self._consecutive_failures += 1
            self._failure_sources.add(source)
            if (self._consecutive_failures >= _FAILURE_THRESHOLD
                    and len(self._failure_sources) >= _MIN_FAILURE_SOURCES
                    and self._open_since == 0.0):
                self._open_since = time.time()
                logger.warning(
                    "%s circuit OPEN (%d failures from %d sources, disabled for %ds)",
                    self.name,
                    self._consecutive_failures,
                    len(self._failure_sources),
                    _RECOVERY_TIMEOUT,
                )

    def force_close(self) -> None:
        """Manual override: close the circuit immediately."""
        with self._lock:
            self._consecutive_failures = 0
            self._failure_sources.clear()
            self._open_since = 0.0
            logger.info("%s circuit FORCE-CLOSED by operator", self.name)
