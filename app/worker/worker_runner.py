import asyncio
import logging
from app.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


async def run_worker():
    if not settings.kafka_enabled:
        logger.warning("KAFKA_ENABLED=false — running in DB-poll fallback mode")
        await _run_db_poll_mode()
        return

    logger.info("Starting Kafka consumer worker...")

    loop = asyncio.get_event_loop()

    from app.worker.kafka_consumer import KafkaConsumerLoop
    consumer_loop = KafkaConsumerLoop()

    try:
        await loop.run_in_executor(None, consumer_loop.start)
    except asyncio.CancelledError:
        logger.info("Worker task cancelled, shutting down consumer...")
        consumer_loop.stop()
    except Exception as e:
        logger.error(f"Worker crashed: {e}", exc_info=True)
        consumer_loop.stop()
        raise


async def _run_db_poll_mode():
    from app.database import SessionLocal
    from app.models.sync_event import SyncEvent
    from app.schemas.sync_command import SyncCommand
    from app.worker.command_handler import CommandHandler
    import json

    logger.info("DB poll mode: polling sync_events every 5 seconds")

    while True:
        try:
            db = SessionLocal()
            try:
                events = (
                    db.query(SyncEvent)
                    .filter(SyncEvent.status == "queued")
                    .limit(50)
                    .all()
                )
                for event in events:
                    try:
                        payload = event.payload or {}
                        pms_property = event.pms_property
                        command = SyncCommand(
                            event_id=event.id,
                            event_type=event.event_type,
                            hotel_id=pms_property.hotel_id,
                            pms_provider=pms_property.pms_provider,
                            pms_property_id=pms_property.pms_property_id,
                            data=payload,
                        )
                        handler = CommandHandler(db)
                        handler.process(command)
                    except Exception as e:
                        logger.error(f"DB poll mode: error processing event {event.id}: {e}")
            finally:
                db.close()
        except Exception as e:
            logger.error(f"DB poll mode outer error: {e}")

        await asyncio.sleep(5)
