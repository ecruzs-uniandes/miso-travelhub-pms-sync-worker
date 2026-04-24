import pytest
from unittest.mock import patch, MagicMock
from app.services.notification_client import NotificationClient
from app.resilience.circuit_breaker import CircuitBreakerOpenError


@pytest.fixture
def client():
    with patch("app.services.notification_client.settings") as mock_settings:
        mock_settings.notification_service_url = "http://test:8001"
        mock_settings.cb_failure_threshold = 5
        mock_settings.cb_recovery_timeout = 30
        return NotificationClient()


def test_notify_conflict_sends_payload(client):
    with patch.object(client.circuit_breaker, "call") as mock_call:
        client.notify_conflict(
            hotel_id="h1",
            conflict_type="overbooking",
            details={"room": "101"},
            recipients=["admin"],
        )
        mock_call.assert_called_once()


def test_notify_error_sends_payload(client):
    with patch.object(client.circuit_breaker, "call") as mock_call:
        client.notify_error(hotel_id="h1", error_message="sync failed")
        mock_call.assert_called_once()


def test_notify_sync_complete_sends_payload(client):
    with patch.object(client.circuit_breaker, "call") as mock_call:
        client.notify_sync_complete(hotel_id="h1")
        mock_call.assert_called_once()


def test_send_handles_circuit_breaker_open(client):
    with patch.object(
        client.circuit_breaker, "call", side_effect=CircuitBreakerOpenError("open")
    ):
        client.notify_error(hotel_id="h1", error_message="test")


def test_send_handles_generic_exception(client):
    with patch.object(
        client.circuit_breaker, "call", side_effect=ConnectionError("refused")
    ):
        client.notify_error(hotel_id="h1", error_message="test")
