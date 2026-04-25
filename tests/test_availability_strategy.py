import uuid
import time
from datetime import date, timedelta
from unittest.mock import patch

from app.schemas.sync_command import SyncCommand
from app.strategies.availability_update import AvailabilityUpdateStrategy
from app.models.availability import Availability


def make_command(hotel_id, room_id, dates_data):
    """Construye un SyncCommand con el payload format ACTUAL.

    El nuevo formato (post alineacion con webhook real):
      data.room_id: str UUID directo
      data.dates[i].date: ISO string
      data.dates[i].available_units: int
    """
    return SyncCommand(
        event_id=str(uuid.uuid4()),
        event_type="availability_update",
        hotel_id=hotel_id,
        pms_provider="sabre",
        pms_property_id="SABRE-001",
        data={
            "room_id": str(room_id),
            "room_type": "Standard",
            "dates": dates_data,
        },
    )


def test_availability_update_creates_records(db, hotel, room):
    command = make_command(
        hotel_id=hotel.id,
        room_id=room.id,
        dates_data=[
            {"date": "2025-07-01", "available_units": 3},
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
    assert record.fuente_actualizacion == "pms_webhook"


def test_availability_update_upserts_existing(db, hotel, room, availability):
    assert availability.unidades_disponibles == 5

    command = make_command(
        hotel_id=hotel.id,
        room_id=room.id,
        dates_data=[
            {"date": str(availability.fecha), "available_units": 10},
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
            "date": str(date(2025, 8, 1) + timedelta(days=i)),
            "available_units": i % 5,
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
            {"date": "2025-09-01", "available_units": 1},
        ],
    )

    conflict_av = Availability(
        id=uuid.uuid4(),
        room_id=room.id,
        fecha=date(2025, 9, 1),
        unidades_disponibles=1,
        unidades_reservadas=3,
        fuente_actualizacion="booking",
    )
    db.add(conflict_av)
    db.commit()

    with patch(
        "app.strategies.availability_update.NotificationClient",
        return_value=mock_notification_client,
    ):
        strategy = AvailabilityUpdateStrategy()
        strategy.execute(command, db)

    mock_notification_client.notify_conflict.assert_called()


def test_availability_update_skips_when_room_id_missing(db, hotel, room):
    command = SyncCommand(
        event_id=str(uuid.uuid4()),
        event_type="availability_update",
        hotel_id=hotel.id,
        pms_provider="sabre",
        pms_property_id="SABRE-001",
        data={
            # sin room_id - strategy debe skip sin error
            "dates": [{"date": "2025-10-01", "available_units": 5}],
        },
    )

    with patch("app.strategies.availability_update.NotificationClient"):
        strategy = AvailabilityUpdateStrategy()
        strategy.execute(command, db)

    count = db.query(Availability).filter(Availability.room_id == room.id).count()
    assert count == 0
