from pydantic import BaseModel, Field
from typing import Any, Optional
from uuid import UUID
from datetime import datetime


class SyncCommand(BaseModel):
    command_id: Optional[str] = None
    event_id: str
    event_type: str
    hotel_id: UUID
    pms_provider: str
    pms_property_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    retry_count: int = 0
    data: dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = None
    created_at: Optional[datetime] = None
