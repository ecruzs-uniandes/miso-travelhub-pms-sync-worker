from pydantic import BaseModel, Field, field_validator
from typing import Any, Optional
from datetime import datetime


class SyncCommand(BaseModel):
    command_id: Optional[str] = None
    event_id: str
    event_type: str
    # Canonical TravelHub (refactor 2026-05-14): hotel.id es varchar, no UUID.
    # Algunos productores PMS envían IDs no-UUID (eg "HB-MDE-001" mapeado a un
    # varchar arbitrario en hotel.id), por eso se acepta como string libre.
    # El field_validator abajo tolera que el caller pase UUID (típico cuando se
    # construye el comando desde un row SQLAlchemy con columna UUID legacy o
    # cuando el productor envía la representación canónica con guiones).
    hotel_id: str
    pms_provider: str

    @field_validator("hotel_id", mode="before")
    @classmethod
    def _coerce_hotel_id(cls, v):
        return str(v) if v is not None else v
    pms_property_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    retry_count: int = 0
    data: dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = None
    created_at: Optional[datetime] = None
