from datetime import datetime, timezone

from app.services.sync_event_service import SyncEventService


def test_update_status_completed_sets_processed_at(db, hotel, sync_event):
    service = SyncEventService(db)
    service.update_status(sync_event.event_id, status="completed")

    db.refresh(sync_event)
    assert sync_event.status == "completed"
    assert sync_event.processed_at is not None


def test_update_status_failed_logs_error_message(db, hotel, sync_event, caplog):
    """error_message ya no es columna en la tabla — el service la loggea."""
    import logging
    service = SyncEventService(db)

    with caplog.at_level(logging.ERROR, logger="app.services.sync_event_service"):
        service.update_status(sync_event.event_id, status="failed", error_message="timeout")

    db.refresh(sync_event)
    assert sync_event.status == "failed"
    assert sync_event.processed_at is not None
    # error_message va a logs, no a la DB
    assert any("timeout" in r.message for r in caplog.records)


def test_update_status_with_explicit_processed_at(db, hotel, sync_event):
    service = SyncEventService(db)
    ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    service.update_status(sync_event.event_id, status="completed", processed_at=ts)

    db.refresh(sync_event)
    assert sync_event.processed_at is not None


def test_update_status_event_not_found_no_raise(db):
    service = SyncEventService(db)
    # No debe lanzar — solo loggea warning
    service.update_status("evt-nonexistent", status="completed")


def test_update_pms_property_last_sync(db, hotel, pms_property):
    service = SyncEventService(db)
    service.update_pms_property_last_sync(hotel.id, "sabre")

    db.refresh(pms_property)
    assert pms_property.last_sync_at is not None


def test_update_pms_property_last_sync_not_found(db, hotel):
    service = SyncEventService(db)
    # No debe lanzar
    service.update_pms_property_last_sync(hotel.id, "nonexistent_provider")


def test_increment_retry_count(db, hotel, sync_event):
    """retry_count ahora es Integer (no string)."""
    service = SyncEventService(db)
    service.increment_retry_count(sync_event.event_id, 2)

    db.refresh(sync_event)
    assert sync_event.retry_count == 2


def test_increment_retry_count_event_not_found_no_raise(db):
    service = SyncEventService(db)
    service.increment_retry_count("evt-nonexistent", 1)
