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

    # backoff_base ** retry_count + jitter(0..1)
    # count=0: 1 + jitter -> [1, 2)
    # count=1: 2 + jitter -> [2, 3)
    # count=2: 4 + jitter -> [4, 5)
    assert 1 <= delay_0 < 2
    assert delay_1 >= 2   # 2^1 = 2
    assert delay_2 >= 4   # 2^2 = 4
    assert delay_0 < delay_1 < delay_2
