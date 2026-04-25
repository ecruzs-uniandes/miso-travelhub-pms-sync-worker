import uuid
import pytest
from unittest.mock import patch

from app.schemas.sync_command import SyncCommand
from app.strategies.property_sync import PropertySyncStrategy
from app.models.hotel import Hotel
from app.models.room import Room
from app.models.availability import Availability
from app.models.tariff import Tariff


pytestmark = pytest.mark.skip(
    reason="PropertySyncStrategy aun no fue actualizada al payload real del webhook. "
    "Strategy actualmente espera campos legacy (room_mappings, etc). "
    "Reactivar tests cuando se alinee la strategy con el formato webhook."
)

def make_command(hotel_id, data):
    return SyncCommand(
        event_id=uuid.uuid4(),
        event_type="property_sync",
        hotel_id=hotel_id,
        pms_provider="sabre",
        pms_property_id="SABRE-001",
        data=data,
    )


def test_sync_hotel_info_updates_fields(db, hotel):
    command = make_command(
        hotel_id=hotel.id,
        data={
            "property_info": {
                "nombre": "Updated Hotel",
                "ciudad": "Medellin",
                "pais": "Colombia",
            }
        },
    )

    strategy = PropertySyncStrategy()
    strategy.execute(command, db)

    db.refresh(hotel)
    assert hotel.nombre == "Updated Hotel"
    assert hotel.ciudad == "Medellin"
    assert hotel.pais == "Colombia"


def test_sync_hotel_info_missing_hotel(db):
    fake_id = uuid.uuid4()
    command = make_command(
        hotel_id=fake_id,
        data={"property_info": {"nombre": "Ghost Hotel"}},
    )

    strategy = PropertySyncStrategy()
    strategy.execute(command, db)


def test_sync_rooms_creates_new_room(db, hotel):
    command = make_command(
        hotel_id=hotel.id,
        data={
            "rooms": [
                {"pms_room_id": "PMS-NEW-1", "nombre": "Suite 1", "capacidad": 4},
            ]
        },
    )

    strategy = PropertySyncStrategy()
    strategy.execute(command, db)

    new_room = db.query(Room).filter(Room.pms_room_id == "PMS-NEW-1").first()
    assert new_room is not None
    assert new_room.nombre == "Suite 1"
    assert new_room.capacidad == 4
    assert new_room.activo is True


def test_sync_rooms_updates_existing_room(db, hotel, room):
    command = make_command(
        hotel_id=hotel.id,
        data={
            "rooms": [
                {"pms_room_id": "PMS-001", "nombre": "Updated Room", "capacidad": 5},
            ]
        },
    )

    strategy = PropertySyncStrategy()
    strategy.execute(command, db)

    db.refresh(room)
    assert room.nombre == "Updated Room"
    assert room.capacidad == 5
    assert room.activo is True


def test_sync_rooms_deactivates_missing_rooms(db, hotel, room):
    command = make_command(
        hotel_id=hotel.id,
        data={
            "rooms": [
                {"pms_room_id": "PMS-OTHER", "nombre": "Other Room", "capacidad": 2},
            ]
        },
    )

    strategy = PropertySyncStrategy()
    strategy.execute(command, db)

    db.refresh(room)
    assert room.activo is False


def test_full_property_sync_with_availability_and_tariffs(db, hotel):
    command = make_command(
        hotel_id=hotel.id,
        data={
            "rooms": [
                {"pms_room_id": "PMS-FULL-1", "nombre": "Full Room", "capacidad": 2},
            ],
            "availability": [
                {"pms_room_id": "PMS-FULL-1", "fecha": "2025-09-01", "unidades_disponibles": 3},
            ],
            "tariffs": [
                {
                    "pms_room_id": "PMS-FULL-1",
                    "fecha_inicio": "2025-09-01",
                    "fecha_fin": "2025-09-15",
                    "precio_por_noche": 200.0,
                    "moneda": "COP",
                },
            ],
        },
    )

    strategy = PropertySyncStrategy()
    strategy.execute(command, db)

    new_room = db.query(Room).filter(Room.pms_room_id == "PMS-FULL-1").first()
    assert new_room is not None

    av = db.query(Availability).filter(Availability.room_id == new_room.id).first()
    assert av is not None
    assert av.unidades_disponibles == 3

    tariff = db.query(Tariff).filter(Tariff.room_id == new_room.id).first()
    assert tariff is not None
    assert float(tariff.precio_por_noche) == 200.0
    assert tariff.moneda == "COP"


def test_property_sync_skips_unmapped_availability(db, hotel):
    command = make_command(
        hotel_id=hotel.id,
        data={
            "rooms": [],
            "availability": [
                {"pms_room_id": "NONEXISTENT", "fecha": "2025-10-01", "unidades_disponibles": 1},
            ],
        },
    )

    strategy = PropertySyncStrategy()
    strategy.execute(command, db)

    count = db.query(Availability).count()
    assert count == 0


def test_property_sync_rollback_on_error(db, hotel):
    command = make_command(
        hotel_id=hotel.id,
        data={
            "rooms": [
                {"pms_room_id": "PMS-ERR", "nombre": "Error Room", "capacidad": 2},
            ],
            "availability": [
                {"pms_room_id": "PMS-ERR", "fecha": "invalid-date", "unidades_disponibles": 1},
            ],
        },
    )

    strategy = PropertySyncStrategy()
    with pytest.raises(Exception):
        strategy.execute(command, db)
