import uuid
import time
import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

from app.schemas.sync_command import SyncCommand
from app.strategies.availability_update import AvailabilityUpdateStrategy
from app.models.availability import Availability


def make_command(hotel_id, room_id, dates_data):
    return SyncCommand(
        event_id=uuid.uuid4(),
        event_type="availability_update",
        hotel_id=hotel_id,
        pms_provider="sabre",
        pms_property_id="SABRE-001",
        data={
            "room_mappings": {
                "PMS-001": str(room_id),
            },
            "dates": dates_data,
        },
    )


def test_availability_update_creates_records(db, hotel, room):
    command = make_command(
        hotel_id=hotel.id,
        room_id=room.id,
        dates_data=[
            {"pms_room_id": "PMS-001", "fecha": "2025-07-01", "unidades_disponibles": 3},
        ],
    )

    with patch("app.strategies.availability_update.NotificationClient"):
        strategy = AvailabilityUpdateStrategy()
        strategy.execute(command, db)

    record = db.query(Availability).filter(
        Availability.room_id == room.id,
        Availability.fecha == date(2025, 7, 1),
    ).first()

    assert record is not None
    assert record.unidades_disponibles == 3


def test_availability_update_upserts_existing(db, hotel, room, availability):
    assert availability.unidades_disponibles == 5

    command = make_command(
        hotel_id=hotel.id,
        room_id=room.id,
        dates_data=[
            {"pms_room_id": "PMS-001", "fecha": str(availability.fecha), "unidades_disponibles": 10},
        ],
    )

    with patch("app.strategies.availability_update.NotificationClient"):
        strategy = AvailabilityUpdateStrategy()
        strategy.execute(command, db)

    db.refresh(availability)
    assert availability.unidades_disponibles == 10


def test_availability_update_batch_performance(db, hotel, room):
    dates_data = [
        {
            "pms_room_id": "PMS-001",
            "fecha": str(date(2025, 8, 1) + timedelta(days=i)),
            "unidades_disponibles": i % 5,
        }
        for i in range(100)
    ]

    command = make_command(hotel_id=hotel.id, room_id=room.id, dates_data=dates_data)

    start = time.time()
    with patch("app.strategies.availability_update.NotificationClient"):
        strategy = AvailabilityUpdateStrategy()
        strategy.execute(command, db)
    elapsed = time.time() - start

    count = db.query(Availability).filter(Availability.room_id == room.id).count()
    assert count == 100
    assert elapsed < 2.0, f"Batch took too long: {elapsed:.2f}s"


def test_availability_conflict_detected(db, hotel, room, mock_notification_client):
    command = make_command(
        hotel_id=hotel.id,
        room_id=room.id,
        dates_data=[
            {"pms_room_id": "PMS-001", "fecha": "2025-09-01", "unidades_disponibles": 1},
        ],
    )

    from app.models.availability import Availability as Av
    conflict_av = Av(
        id=uuid.uuid4(),
        room_id=room.id,
        fecha=date(2025, 9, 1),
        unidades_disponibles=1,
        unidades_reservadas=3,
        fuente_actualizacion="booking",
    )
    db.add(conflict_av)
    db.commit()

    with patch("app.strategies.availability_update.NotificationClient", return_value=mock_notification_client):
        strategy = AvailabilityUpdateStrategy()
        strategy.execute(command, db)

    mock_notification_client.notify_conflict.assert_called()
