import uuid
import pytest
from datetime import datetime, timezone

from app.services.sync_event_service import SyncEventService


pytestmark = pytest.mark.skip(reason="TODO: actualizar tests tras alineacion de modelos SQLAlchemy con schema real PostgreSQL")

def test_update_status_completed_sets_processed_at(db, pms_property, sync_event):
    service = SyncEventService(db)
    service.update_status(sync_event.id, status="completed")

    db.refresh(sync_event)
    assert sync_event.status == "completed"
    assert sync_event.processed_at is not None


def test_update_status_failed_with_error_message(db, pms_property, sync_event):
    service = SyncEventService(db)
    service.update_status(sync_event.id, status="failed", error_message="timeout")

    db.refresh(sync_event)
    assert sync_event.status == "failed"
    assert sync_event.error_message == "timeout"
    assert sync_event.processed_at is not None


def test_update_status_with_explicit_processed_at(db, pms_property, sync_event):
    service = SyncEventService(db)
    ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    service.update_status(sync_event.id, status="completed", processed_at=ts)

    db.refresh(sync_event)
    assert sync_event.processed_at is not None


def test_update_status_event_not_found(db):
    service = SyncEventService(db)
    service.update_status(uuid.uuid4(), status="completed")


def test_update_pms_property_last_sync(db, hotel, pms_property):
    service = SyncEventService(db)
    service.update_pms_property_last_sync(hotel.id, "sabre")

    db.refresh(pms_property)
    assert pms_property.last_sync_at is not None


def test_update_pms_property_last_sync_not_found(db, hotel):
    service = SyncEventService(db)
    service.update_pms_property_last_sync(hotel.id, "nonexistent_provider")


def test_increment_retry_count(db, pms_property, sync_event):
    service = SyncEventService(db)
    service.increment_retry_count(sync_event.id, 2)

    db.refresh(sync_event)
    assert sync_event.retry_count == "2"


def test_increment_retry_count_event_not_found(db):
    service = SyncEventService(db)
    service.increment_retry_count(uuid.uuid4(), 1)
