import asyncio
from unittest.mock import patch, AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.main import app, lifespan, health


def test_health_endpoint():
    client = TestClient(app)
    with patch("app.main.create_tables"), patch("app.main.run_worker", new=AsyncMock()):
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "service": "pms-sync-worker"}


@pytest.mark.asyncio
async def test_health_handler_returns_payload():
    result = await health()
    assert result == {"status": "healthy", "service": "pms-sync-worker"}


@pytest.mark.asyncio
async def test_lifespan_starts_and_cancels_worker():
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def fake_worker():
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with patch("app.main.create_tables") as ct, patch(
        "app.main.run_worker", new=fake_worker
    ):
        async with lifespan(app):
            await asyncio.wait_for(started.wait(), timeout=1.0)
            ct.assert_called_once()

    assert cancelled.is_set()
