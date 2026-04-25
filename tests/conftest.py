import uuid
import pytest
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import MagicMock

from app.database import Base
from app.models.hotel import Hotel
from app.models.room import Room
from app.models.availability import Availability
from app.models.pms_property import PmsProperty
from app.models.sync_event import SyncEvent


TEST_DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture(scope="function")
def engine():
    eng = create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=eng)
    yield eng
    Base.metadata.drop_all(bind=eng)


@pytest.fixture(scope="function")
def db(engine):
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestSession()
    yield session
    session.close()


@pytest.fixture
def hotel(db):
    h = Hotel(
        id=uuid.uuid4(),
        nombre="Test Hotel",
        ciudad="Bogota",
        pais="Colombia",
        activo=True,
    )
    db.add(h)
    db.commit()
    db.refresh(h)
    return h


@pytest.fixture
def room(db, hotel):
    r = Room(
        id=uuid.uuid4(),
        hotel_id=hotel.id,
        nombre="Room 101",
        capacidad=2,
        pms_room_id="PMS-001",
        activo=True,
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


@pytest.fixture
def pms_property(db, hotel):
    p = PmsProperty(
        id=uuid.uuid4(),
        hotel_id=hotel.id,
        pms_provider="sabre",
        pms_property_id="SABRE-001",
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


@pytest.fixture
def sync_event(db, hotel):
    """Creates a SyncEvent matching the real PG schema:
    event_id (str unique), hotel_id, pms_provider, event_type, payload_hash,
    status, retry_count (int)."""
    event = SyncEvent(
        id=uuid.uuid4(),
        event_id="evt-test-001",
        pms_provider="sabre",
        hotel_id=hotel.id,
        event_type="availability_update",
        payload_hash="abc123",
        status="queued",
        retry_count=0,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


@pytest.fixture
def availability(db, room):
    av = Availability(
        id=uuid.uuid4(),
        room_id=room.id,
        fecha=date(2025, 6, 1),
        unidades_disponibles=5,
        unidades_reservadas=2,
        fuente_actualizacion="pms_webhook",
    )
    db.add(av)
    db.commit()
    db.refresh(av)
    return av


@pytest.fixture
def mock_notification_client():
    client = MagicMock()
    client.notify_conflict = MagicMock()
    client.notify_error = MagicMock()
    client.notify_sync_complete = MagicMock()
    return client
