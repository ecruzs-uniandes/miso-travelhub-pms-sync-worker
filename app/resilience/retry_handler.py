import random
import logging
from app.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class RetryHandler:
    def __init__(self, max_retries: int = None, backoff_base: int = None):
        self.max_retries = max_retries or settings.max_retries
        self.backoff_base = backoff_base or settings.retry_backoff_base

    def should_retry(self, retry_count: int) -> bool:
        return retry_count < self.max_retries

    def get_delay(self, retry_count: int) -> float:
        jitter = random.uniform(0, 1)
        return (self.backoff_base ** retry_count) + jitter

    def get_next_retry_count(self, retry_count: int) -> int:
        return retry_count + 1

    def log_retry(self, retry_count: int, error: Exception, event_id: str):
        delay = self.get_delay(retry_count)
        logger.warning(
            f"Retrying event {event_id} (attempt {retry_count + 1}/{self.max_retries})"
            f" after {delay:.2f}s. Error: {error}"
        )

    def log_max_retries_exceeded(self, retry_count: int, event_id: str):
        logger.error(
            f"Event {event_id} exceeded max retries ({self.max_retries}). Marking as failed."
        )
