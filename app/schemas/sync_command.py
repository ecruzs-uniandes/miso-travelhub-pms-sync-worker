from pydantic import BaseModel, Field
from typing import Any, Optional
from datetime import datetime


class SyncCommand(BaseModel):
    command_id: Optional[str] = None
    event_id: str
    event_type: str
    # Canonical TravelHub (refactor 2026-05-14): hotel.id es varchar, no UUID.
    # Algunos productores PMS envían IDs no-UUID (eg "HB-MDE-001" mapeado a un
    # varchar arbitrario en hotel.id), por eso se acepta como string libre.
    hotel_id: str
    pms_provider: str
    pms_property_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    retry_count: int = 0
    data: dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = None
    created_at: Optional[datetime] = None
