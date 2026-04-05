import logging
import httpx
from typing import List
from app.config import get_settings
from app.resilience.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError

logger = logging.getLogger(__name__)

settings = get_settings()


class NotificationClient:
    def __init__(self):
        self.base_url = settings.notification_service_url
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=settings.cb_failure_threshold,
            recovery_timeout=settings.cb_recovery_timeout,
            name="notification-service",
        )

    def notify_conflict(
        self,
        hotel_id: str,
        conflict_type: str,
        details: dict,
        recipients: List[str],
    ):
        payload = {
            "type": conflict_type,
            "hotel_id": hotel_id,
            "details": details,
            "recipients": recipients,
        }
        self._send(payload)

    def notify_error(self, hotel_id: str, error_message: str):
        payload = {
            "type": "pms_sync_error",
            "hotel_id": hotel_id,
            "details": {"error": error_message},
            "recipients": ["platform_admin"],
        }
        self._send(payload)

    def notify_sync_complete(self, hotel_id: str):
        payload = {
            "type": "pms_sync_complete",
            "hotel_id": hotel_id,
            "details": {},
            "recipients": ["hotel_admin"],
        }
        self._send(payload)

    def _send(self, payload: dict):
        def _do_request():
            with httpx.Client(timeout=5.0) as client:
                response = client.post(
                    f"{self.base_url}/api/v1/notifications/internal",
                    json=payload,
                )
                response.raise_for_status()
                return response

        try:
            self.circuit_breaker.call(_do_request)
            logger.info(f"Notification sent: type={payload.get('type')}, hotel={payload.get('hotel_id')}")
        except CircuitBreakerOpenError:
            logger.warning(
                f"Notification service circuit breaker is OPEN. "
                f"Skipping notification: {payload.get('type')} for hotel {payload.get('hotel_id')}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to send notification ({payload.get('type')}): {e}. Continuing."
            )
