import time
import pytest
from unittest.mock import MagicMock, patch

from app.resilience.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError


@pytest.fixture
def cb():
    return CircuitBreaker(failure_threshold=5, recovery_timeout=30, name="test")


def test_closed_allows_calls(cb):
    result = cb.call(lambda: "ok")
    assert result == "ok"
    assert cb.state == "CLOSED"


def test_opens_after_threshold_failures(cb):
    def failing_func():
        raise ValueError("fail")

    for _ in range(5):
        with pytest.raises(ValueError):
            cb.call(failing_func)

    assert cb.state == "OPEN"
    assert cb.failure_count >= 5


def test_open_rejects_calls(cb):
    def failing_func():
        raise ValueError("fail")

    for _ in range(5):
        with pytest.raises(ValueError):
            cb.call(failing_func)

    with pytest.raises(CircuitBreakerOpenError):
        cb.call(lambda: "should not run")


def test_half_open_after_timeout(cb):
    def failing_func():
        raise ValueError("fail")

    for _ in range(5):
        with pytest.raises(ValueError):
            cb.call(failing_func)

    assert cb.state == "OPEN"

    cb.last_failure_time = time.time() - 31

    cb.call(lambda: "ok")
    assert cb.state == "CLOSED"


def test_closes_on_success_after_half_open(cb):
    def failing_func():
        raise ValueError("fail")

    for _ in range(5):
        with pytest.raises(ValueError):
            cb.call(failing_func)

    cb.last_failure_time = time.time() - 31

    result = cb.call(lambda: "success")
    assert result == "success"
    assert cb.state == "CLOSED"
    assert cb.failure_count == 0
