import uuid
import pytest
from datetime import date
from unittest.mock import MagicMock

from app.services.conflict_resolver import ConflictResolver
from app.models.availability import Availability


def make_availability(room_id, fecha, disponibles, reservadas):
    av = Availability()
    av.id = uuid.uuid4()
    av.room_id = room_id
    av.fecha = fecha
    av.unidades_disponibles = disponibles
    av.unidades_reservadas = reservadas
    return av


def test_overbooking_detected_and_flagged():
    mock_client = MagicMock()
    resolver = ConflictResolver(notification_client=mock_client)

    room_id = uuid.uuid4()
    conflict = make_availability(room_id, date(2025, 7, 1), disponibles=0, reservadas=2)

    result = resolver.resolve(hotel_id=str(uuid.uuid4()), conflicts=[conflict])

    assert len(result) == 1
    assert result[0]["conflict_type"] == "critical_zero_availability"
    mock_client.notify_conflict.assert_called_once()


def test_normal_update_no_conflict():
    mock_client = MagicMock()
    resolver = ConflictResolver(notification_client=mock_client)

    room_id = uuid.uuid4()
    normal = make_availability(room_id, date(2025, 7, 1), disponibles=5, reservadas=2)

    result = resolver.resolve(hotel_id=str(uuid.uuid4()), conflicts=[normal])

    assert result[0]["conflict_type"] == "none"
    mock_client.notify_conflict.assert_not_called()


def test_conflict_notification_sent():
    mock_client = MagicMock()
    resolver = ConflictResolver(notification_client=mock_client)

    room_id = uuid.uuid4()
    hotel_id = str(uuid.uuid4())

    overbooking = make_availability(room_id, date(2025, 7, 1), disponibles=1, reservadas=3)
    resolver.resolve(hotel_id=hotel_id, conflicts=[overbooking])

    mock_client.notify_conflict.assert_called_once_with(
        hotel_id=hotel_id,
        conflict_type="pms_sync_conflict",
        details=mock_client.notify_conflict.call_args.kwargs["details"],
        recipients=["hotel_admin"],
    )
