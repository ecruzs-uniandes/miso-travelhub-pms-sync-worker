import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.schemas.sync_command import SyncCommand
from app.strategies.availability_update import AvailabilityUpdateStrategy
from app.strategies.rate_update import RateUpdateStrategy
from app.strategies.property_sync import PropertySyncStrategy
from app.services.sync_event_service import SyncEventService
from app.resilience.retry_handler import NonRetryableError

logger = logging.getLogger(__name__)

STRATEGY_MAP = {
    "availability_update": AvailabilityUpdateStrategy(),
    "rate_update": RateUpdateStrategy(),
    "property_sync": PropertySyncStrategy(),
}


class CommandHandler:
    def __init__(self, db: Session):
        self.db = db
        self.sync_event_service = SyncEventService(db)

    def process(self, command: SyncCommand) -> None:
        strategy = STRATEGY_MAP.get(command.event_type)
        if not strategy:
            raise NonRetryableError(f"Unknown event_type: {command.event_type}")

        logger.info(
            f"Processing command: event_id={command.event_id}, "
            f"event_type={command.event_type}, hotel={command.hotel_id}"
        )

        self.sync_event_service.update_status(command.event_id, status="processing")

        try:
            strategy.execute(command, self.db)
            self.sync_event_service.update_status(
                command.event_id,
                status="completed",
                processed_at=datetime.now(timezone.utc),
            )
            self.sync_event_service.update_pms_property_last_sync(
                command.hotel_id, command.pms_provider
            )
            logger.info(f"Command {command.event_id} completed successfully")

        except Exception as e:
            logger.error(f"Command {command.event_id} failed: {e}", exc_info=True)
            self.sync_event_service.update_status(
                command.event_id,
                status="failed",
                error_message=str(e),
            )
            raise
