import time
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)


class CircuitBreakerOpenError(Exception):
    pass


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_failure_time: float | None = None

    def call(self, func: Callable, *args, **kwargs) -> Any:
        if self.state == "OPEN":
            if self.last_failure_time and (time.time() - self.last_failure_time > self.recovery_timeout):
                logger.info(f"CircuitBreaker [{self.name}] transitioning to HALF_OPEN")
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpenError(
                    f"CircuitBreaker [{self.name}] is OPEN. Rejecting call."
                )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except CircuitBreakerOpenError:
            raise
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        if self.state == "HALF_OPEN":
            logger.info(f"CircuitBreaker [{self.name}] closing after successful HALF_OPEN call")
        self.failure_count = 0
        self.state = "CLOSED"

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            if self.state != "OPEN":
                logger.warning(
                    f"CircuitBreaker [{self.name}] OPENING after {self.failure_count} failures"
                )
            self.state = "OPEN"

    @property
    def is_open(self) -> bool:
        return self.state == "OPEN"

    @property
    def is_closed(self) -> bool:
        return self.state == "CLOSED"
