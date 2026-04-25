import uuid
import pytest
from datetime import date

from app.schemas.sync_command import SyncCommand
from app.strategies.rate_update import RateUpdateStrategy
from app.models.tariff import Tariff


pytestmark = pytest.mark.skip(
    reason="RateUpdateStrategy aun no fue actualizada al payload real del webhook. "
    "Strategy actualmente espera campos legacy (room_mappings, etc). "
    "Reactivar tests cuando se alinee la strategy con el formato webhook."
)

def make_rate_command(hotel_id, room_id, rates):
    return SyncCommand(
        event_id=uuid.uuid4(),
        event_type="rate_update",
        hotel_id=hotel_id,
        pms_provider="sabre",
        pms_property_id="SABRE-001",
        data={
            "room_mappings": {"PMS-001": str(room_id)},
            "rates": rates,
        },
    )


def test_rate_update_creates_tariff(db, hotel, room):
    command = make_rate_command(
        hotel_id=hotel.id,
        room_id=room.id,
        rates=[
            {
                "pms_room_id": "PMS-001",
                "fecha_inicio": "2025-07-01",
                "fecha_fin": "2025-07-15",
                "precio_por_noche": 120.0,
                "moneda": "USD",
            }
        ],
    )

    strategy = RateUpdateStrategy()
    strategy.execute(command, db)

    tariff = db.query(Tariff).filter(Tariff.room_id == room.id).first()
    assert tariff is not None
    assert float(tariff.precio_por_noche) == 120.0
    assert tariff.moneda == "USD"


def test_rate_update_overwrites_existing(db, hotel, room):
    from app.models.tariff import Tariff as T

    existing = T(
        id=uuid.uuid4(),
        room_id=room.id,
        fecha_inicio=date(2025, 7, 1),
        fecha_fin=date(2025, 7, 15),
        precio_por_noche=100.0,
        moneda="USD",
    )
    db.add(existing)
    db.commit()

    command = make_rate_command(
        hotel_id=hotel.id,
        room_id=room.id,
        rates=[
            {
                "pms_room_id": "PMS-001",
                "fecha_inicio": "2025-07-01",
                "fecha_fin": "2025-07-15",
                "precio_por_noche": 150.0,
                "moneda": "USD",
            }
        ],
    )

    strategy = RateUpdateStrategy()
    strategy.execute(command, db)

    db.refresh(existing)
    assert float(existing.precio_por_noche) == 150.0
