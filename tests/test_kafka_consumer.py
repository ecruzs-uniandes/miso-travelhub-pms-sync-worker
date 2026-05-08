import json
import uuid
from unittest.mock import patch, MagicMock

import pytest

from app.resilience.retry_handler import NonRetryableError


@pytest.fixture
def consumer_loop():
    with patch("app.worker.kafka_consumer.Consumer") as MockConsumer, patch(
        "app.worker.kafka_consumer.Producer"
    ) as MockProducer:
        from app.worker.kafka_consumer import KafkaConsumerLoop

        loop = KafkaConsumerLoop()
        loop.consumer = MagicMock()
        loop.producer = MagicMock()
        yield loop


def _make_msg(payload: dict | None, error=None):
    msg = MagicMock()
    if error is not None:
        msg.error.return_value = error
    else:
        msg.error.return_value = None
    msg.value.return_value = (
        json.dumps(payload).encode("utf-8") if payload is not None else b"not-json"
    )
    return msg


def test_start_loops_until_stopped(consumer_loop):
    msg_no_error = _make_msg({
        "event_id": "evt-1",
        "event_type": "availability_update",
        "hotel_id": str(uuid.uuid4()),
        "pms_provider": "sabre",
        "data": {},
    })

    poll_calls = {"n": 0}

    def fake_poll(timeout):
        poll_calls["n"] += 1
        if poll_calls["n"] == 1:
            return None
        if poll_calls["n"] == 2:
            return msg_no_error
        consumer_loop._running = False
        return None

    consumer_loop.consumer.poll.side_effect = fake_poll
    with patch.object(consumer_loop, "_process_message") as proc:
        consumer_loop.start()

    assert poll_calls["n"] >= 3
    proc.assert_called_once_with(msg_no_error)
    consumer_loop.consumer.subscribe.assert_called_once()


def test_start_handles_kafka_error(consumer_loop):
    from confluent_kafka import KafkaError

    err_msg = MagicMock()
    kafka_err = MagicMock()
    kafka_err.code.return_value = KafkaError._PARTITION_EOF
    err_msg.error.return_value = kafka_err

    other_err_msg = MagicMock()
    other_err = MagicMock()
    other_err.code.return_value = -999
    other_err_msg.error.return_value = other_err

    calls = {"n": 0}

    def fake_poll(timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            return err_msg
        if calls["n"] == 2:
            return other_err_msg
        consumer_loop._running = False
        return None

    consumer_loop.consumer.poll.side_effect = fake_poll
    consumer_loop.start()
    assert calls["n"] >= 3


def test_stop_closes_consumer(consumer_loop):
    consumer_loop._running = True
    consumer_loop.stop()
    assert consumer_loop._running is False
    consumer_loop.consumer.close.assert_called_once()


def test_process_message_invalid_json_skipped(consumer_loop):
    msg = MagicMock()
    msg.value.return_value = b"not-json"

    consumer_loop._process_message(msg)
    consumer_loop.consumer.commit.assert_called_once_with(msg)


def test_process_message_success_commits(consumer_loop):
    payload = {
        "event_id": "evt-ok",
        "event_type": "availability_update",
        "hotel_id": str(uuid.uuid4()),
        "pms_provider": "sabre",
        "data": {},
    }
    msg = _make_msg(payload)

    fake_session = MagicMock()
    fake_handler = MagicMock()

    with patch("app.worker.kafka_consumer.SessionLocal", return_value=fake_session), patch(
        "app.worker.kafka_consumer.CommandHandler", return_value=fake_handler
    ):
        consumer_loop._process_message(msg)

    fake_handler.process.assert_called_once()
    consumer_loop.consumer.commit.assert_called_once_with(msg)
    fake_session.close.assert_called_once()


def test_process_message_non_retryable_commits_without_retry(consumer_loop):
    payload = {
        "event_id": "evt-nr",
        "event_type": "availability_update",
        "hotel_id": str(uuid.uuid4()),
        "pms_provider": "sabre",
        "data": {},
    }
    msg = _make_msg(payload)

    fake_session = MagicMock()
    fake_handler = MagicMock()
    fake_handler.process.side_effect = NonRetryableError("bad")

    with patch("app.worker.kafka_consumer.SessionLocal", return_value=fake_session), patch(
        "app.worker.kafka_consumer.CommandHandler", return_value=fake_handler
    ):
        consumer_loop._process_message(msg)

    consumer_loop.consumer.commit.assert_called_once_with(msg)
    consumer_loop.producer.produce.assert_not_called()


def test_process_message_retryable_republishes(consumer_loop):
    payload = {
        "event_id": "evt-retry",
        "event_type": "availability_update",
        "hotel_id": str(uuid.uuid4()),
        "pms_provider": "sabre",
        "retry_count": 0,
        "data": {},
    }
    msg = _make_msg(payload)

    fake_session = MagicMock()
    fake_handler = MagicMock()
    fake_handler.process.side_effect = RuntimeError("transient")
    fake_sync_svc = MagicMock()

    with patch("app.worker.kafka_consumer.SessionLocal", return_value=fake_session), patch(
        "app.worker.kafka_consumer.CommandHandler", return_value=fake_handler
    ), patch("app.worker.kafka_consumer.SyncEventService", return_value=fake_sync_svc):
        consumer_loop._process_message(msg)

    consumer_loop.producer.produce.assert_called_once()
    consumer_loop.producer.flush.assert_called_once()
    fake_sync_svc.increment_retry_count.assert_called_once()
    consumer_loop.consumer.commit.assert_called_once_with(msg)


def test_process_message_max_retries_marks_failed(consumer_loop):
    payload = {
        "event_id": "evt-dlq",
        "event_type": "availability_update",
        "hotel_id": str(uuid.uuid4()),
        "pms_provider": "sabre",
        "retry_count": 99,
        "data": {},
    }
    msg = _make_msg(payload)

    fake_session = MagicMock()
    fake_handler = MagicMock()
    fake_handler.process.side_effect = RuntimeError("still failing")
    fake_sync_svc = MagicMock()

    with patch("app.worker.kafka_consumer.SessionLocal", return_value=fake_session), patch(
        "app.worker.kafka_consumer.CommandHandler", return_value=fake_handler
    ), patch("app.worker.kafka_consumer.SyncEventService", return_value=fake_sync_svc):
        consumer_loop._process_message(msg)

    fake_sync_svc.update_status.assert_called_once()
    args, kwargs = fake_sync_svc.update_status.call_args
    assert kwargs.get("status") == "failed"
    consumer_loop.producer.produce.assert_not_called()
    consumer_loop.consumer.commit.assert_called_once_with(msg)
