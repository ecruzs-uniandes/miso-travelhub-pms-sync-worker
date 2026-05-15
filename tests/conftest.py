import uuid
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models.hotel import Hotel
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
        id=str(uuid.uuid4()),
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
