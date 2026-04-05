import json
import logging
from confluent_kafka import Consumer, KafkaError, Producer
from app.config import get_settings
from app.schemas.sync_command import SyncCommand
from app.worker.command_handler import CommandHandler
from app.database import SessionLocal
from app.resilience.retry_handler import RetryHandler, RetryableError, NonRetryableError
from app.services.sync_event_service import SyncEventService

logger = logging.getLogger(__name__)

settings = get_settings()


class KafkaConsumerLoop:
    def __init__(self):
        self.settings = settings
        self.retry_handler = RetryHandler()
        self._running = False

        self.consumer = Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
        })

        self.producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
        })

    def start(self):
        self._running = True
        self.consumer.subscribe([self.settings.kafka_topic_pms_sync])
        logger.info(
            f"Kafka consumer started. Topic: {self.settings.kafka_topic_pms_sync}, "
            f"Group: {self.settings.kafka_consumer_group}"
        )

        while self._running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("End of partition reached")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            self._process_message(msg)

    def stop(self):
        self._running = False
        self.consumer.close()
        logger.info("Kafka consumer stopped")

    def _process_message(self, msg):
        try:
            raw = msg.value().decode("utf-8")
            payload = json.loads(raw)
            command = SyncCommand(**payload)
        except Exception as e:
            logger.error(f"Failed to deserialize message: {e}. Skipping.")
            self.consumer.commit(msg)
            return

        db = SessionLocal()
        try:
            handler = CommandHandler(db)
            handler.process(command)
            self.consumer.commit(msg)

        except NonRetryableError as e:
            logger.error(f"NonRetryable error for event {command.event_id}: {e}")
            self.consumer.commit(msg)

        except Exception as e:
            if self.retry_handler.should_retry(command.retry_count):
                next_count = self.retry_handler.get_next_retry_count(command.retry_count)
                self.retry_handler.log_retry(command.retry_count, e, str(command.event_id))
                self._republish(command, next_count, db)
            else:
                self.retry_handler.log_max_retries_exceeded(command.retry_count, str(command.event_id))
                sync_svc = SyncEventService(db)
                sync_svc.update_status(
                    command.event_id,
                    status="failed",
                    error_message=str(e),
                )
            self.consumer.commit(msg)

        finally:
            db.close()

    def _republish(self, command: SyncCommand, retry_count: int, db):
        updated_command = command.model_copy(update={"retry_count": retry_count})
        payload = updated_command.model_dump_json()

        sync_svc = SyncEventService(db)
        sync_svc.increment_retry_count(command.event_id, retry_count)

        self.producer.produce(
            self.settings.kafka_topic_pms_sync,
            value=payload.encode("utf-8"),
        )
        self.producer.flush()
        logger.info(
            f"Republished event {command.event_id} with retry_count={retry_count}"
        )
