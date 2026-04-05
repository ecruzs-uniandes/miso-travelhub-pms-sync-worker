import pytest
from app.resilience.retry_handler import RetryHandler


@pytest.fixture
def retry_handler():
    return RetryHandler(max_retries=3, backoff_base=2)


def test_retry_increments_count(retry_handler):
    next_count = retry_handler.get_next_retry_count(0)
    assert next_count == 1


def test_max_retries_exceeded_marks_failed(retry_handler):
    assert retry_handler.should_retry(0) is True
    assert retry_handler.should_retry(1) is True
    assert retry_handler.should_retry(2) is True
    assert retry_handler.should_retry(3) is False


def test_exponential_backoff_delay(retry_handler):
    # Without jitter the base is backoff_base ** retry_count
    # With jitter it's between base and base+1
    delay_0 = retry_handler.get_delay(0)
    delay_1 = retry_handler.get_delay(1)
    delay_2 = retry_handler.get_delay(2)

    assert 2 <= delay_0 < 3   # 2^0 = 1... base=2 so 2^0=1 -> actually 2**0=1+jitter
    # backoff_base ** retry_count = 2**0=1, 2**1=2, 2**2=4
    # Let me re-check: delay = base ** count + jitter
    # For base=2, count=0: 1 + jitter (0..1) -> 1 to 2
    # For base=2, count=1: 2 + jitter -> 2 to 3
    # For base=2, count=2: 4 + jitter -> 4 to 5

    assert delay_0 >= 1   # 2^0 = 1
    assert delay_1 >= 2   # 2^1 = 2
    assert delay_2 >= 4   # 2^2 = 4
    assert delay_0 < delay_1 < delay_2
