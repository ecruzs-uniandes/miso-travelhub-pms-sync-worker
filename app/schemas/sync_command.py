from pydantic import BaseModel, Field
from typing import Any, Optional
from uuid import UUID
import uuid


class SyncCommand(BaseModel):
    event_id: UUID = Field(default_factory=uuid.uuid4)
    event_type: str
    hotel_id: UUID
    pms_provider: str
    pms_property_id: str
    retry_count: int = 0
    data: dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = None
