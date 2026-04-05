import uuid
import pytest
from unittest.mock import patch, MagicMock

from app.schemas.sync_command import SyncCommand
from app.worker.command_handler import CommandHandler
from app.resilience.retry_handler import NonRetryableError
from app.models.sync_event import SyncEvent


def make_command(event_id, event_type, hotel_id, pms_property_id):
    return SyncCommand(
        event_id=event_id,
        event_type=event_type,
        hotel_id=hotel_id,
        pms_provider="sabre",
        pms_property_id=pms_property_id,
        data={},
    )


def test_routes_to_correct_strategy(db, hotel, pms_property, sync_event):
    sync_event.event_type = "availability_update"
    db.commit()

    command = make_command(
        event_id=sync_event.id,
        event_type="availability_update",
        hotel_id=hotel.id,
        pms_property_id=pms_property.pms_property_id,
    )

    with patch("app.strategies.availability_update.AvailabilityUpdateStrategy.execute") as mock_exec:
        mock_exec.return_value = None
        handler = CommandHandler(db)
        handler.process(command)

    mock_exec.assert_called_once()


def test_unknown_event_type_raises_error(db, hotel, pms_property, sync_event):
    command = make_command(
        event_id=sync_event.id,
        event_type="unknown_type",
        hotel_id=hotel.id,
        pms_property_id=pms_property.pms_property_id,
    )

    handler = CommandHandler(db)
    with pytest.raises(NonRetryableError):
        handler.process(command)


def test_updates_sync_event_status(db, hotel, pms_property, sync_event):
    command = make_command(
        event_id=sync_event.id,
        event_type="availability_update",
        hotel_id=hotel.id,
        pms_property_id=pms_property.pms_property_id,
    )

    with patch("app.strategies.availability_update.AvailabilityUpdateStrategy.execute") as mock_exec:
        mock_exec.return_value = None
        handler = CommandHandler(db)
        handler.process(command)

    db.refresh(sync_event)
    assert sync_event.status == "completed"
