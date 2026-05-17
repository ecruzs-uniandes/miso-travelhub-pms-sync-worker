import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.models.sync_event import SyncEvent
from app.models.pms_property import PmsProperty

logger = logging.getLogger(__name__)


class SyncEventService:
    def __init__(self, db: Session):
        self.db = db

    def _find(self, event_id) -> SyncEvent | None:
        return (
            self.db.query(SyncEvent)
            .filter(SyncEvent.event_id == str(event_id))
            .first()
        )

    def update_status(
        self,
        event_id,
        status: str,
        error_message: str = None,
        processed_at: datetime = None,
    ):
        event = self._find(event_id)
        if not event:
            logger.warning(f"SyncEvent {event_id} not found, skipping status update")
            return

        event.status = status
        if processed_at:
            event.processed_at = processed_at
        elif status in ("completed", "failed"):
            event.processed_at = datetime.now(timezone.utc)

        if error_message:
            logger.error(f"SyncEvent {event_id} error: {error_message}")

        self.db.commit()
        logger.debug(f"SyncEvent {event_id} status updated to '{status}'")

    def update_pms_property_last_sync(self, hotel_id: str, pms_provider: str):
        pms_property = (
            self.db.query(PmsProperty)
            .filter(
                PmsProperty.hotel_id == hotel_id,
                PmsProperty.pms_provider == pms_provider,
            )
            .first()
        )
        if pms_property:
            pms_property.last_sync_at = datetime.now(timezone.utc)
            self.db.commit()
            logger.debug(f"Updated last_sync_at for hotel {hotel_id} / provider {pms_provider}")

    def increment_retry_count(self, event_id, new_count: int):
        event = self._find(event_id)
        if event:
            event.retry_count = int(new_count)
            self.db.commit()
