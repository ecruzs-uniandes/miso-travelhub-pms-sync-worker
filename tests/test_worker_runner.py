import asyncio
import json
import uuid
from datetime import date, datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from app.worker import worker_runner


@pytest.mark.asyncio
async def test_run_worker_db_poll_when_kafka_disabled():
    poll_called = asyncio.Event()

    async def fake_poll():
        poll_called.set()

    with patch.object(worker_runner.settings, "kafka_enabled", False), patch.object(
        worker_runner, "_run_db_poll_mode", new=fake_poll
    ):
        await asyncio.wait_for(worker_runner.run_worker(), timeout=1.0)

    assert poll_called.is_set()


@pytest.mark.asyncio
async def test_run_worker_starts_kafka_consumer_and_handles_cancel():
    consumer_instance = MagicMock()

    def fake_start():
        import time
        time.sleep(0.5)

    consumer_instance.start.side_effect = fake_start

    with patch.object(worker_runner.settings, "kafka_enabled", True), patch(
        "app.worker.kafka_consumer.KafkaConsumerLoop", return_value=consumer_instance
    ):
        task = asyncio.create_task(worker_runner.run_worker())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    consumer_instance.stop.assert_called()


@pytest.mark.asyncio
async def test_run_worker_propagates_unexpected_exception():
    consumer_instance = MagicMock()
    consumer_instance.start.side_effect = RuntimeError("boom")

    with patch.object(worker_runner.settings, "kafka_enabled", True), patch(
        "app.worker.kafka_consumer.KafkaConsumerLoop", return_value=consumer_instance
    ):
        with pytest.raises(RuntimeError):
            await worker_runner.run_worker()

    consumer_instance.stop.assert_called()


@pytest.mark.asyncio
async def test_db_poll_processes_queued_event():
    fake_pms_property = MagicMock()
    fake_pms_property.hotel_id = uuid.uuid4()
    fake_pms_property.pms_provider = "sabre"
    fake_pms_property.pms_property_id = "SABRE-001"

    fake_event = MagicMock()
    fake_event.id = "evt-poll-1"
    fake_event.event_type = "availability_update"
    fake_event.payload = {"room_id": str(uuid.uuid4()), "fecha": "2025-01-01"}
    fake_event.pms_property = fake_pms_property

    captured = {}

    class FakeHandler:
        def __init__(self, _db):
            captured["db"] = _db

        def process(self, command):
            captured["command"] = command
            raise asyncio.CancelledError()

    fake_session = MagicMock()
    fake_query = MagicMock()
    fake_query.filter.return_value.limit.return_value.all.return_value = [fake_event]
    fake_session.query.return_value = fake_query

    with patch("app.database.SessionLocal", return_value=fake_session), patch(
        "app.worker.command_handler.CommandHandler", FakeHandler
    ):
        with pytest.raises(asyncio.CancelledError):
            await worker_runner._run_db_poll_mode()

    assert "command" in captured
    assert captured["command"].event_type == "availability_update"


@pytest.mark.asyncio
async def test_db_poll_handles_inner_exception_and_keeps_running():
    fake_session = MagicMock()
    fake_session.query.side_effect = RuntimeError("db boom")

    sleeps = {"count": 0}

    real_sleep = asyncio.sleep

    async def fake_sleep(_):
        sleeps["count"] += 1
        if sleeps["count"] >= 1:
            raise asyncio.CancelledError()
        await real_sleep(0)

    with patch("app.database.SessionLocal", return_value=fake_session), patch(
        "app.worker.worker_runner.asyncio.sleep", new=fake_sleep
    ):
        with pytest.raises(asyncio.CancelledError):
            await worker_runner._run_db_poll_mode()

    assert sleeps["count"] >= 1
