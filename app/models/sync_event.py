import uuid
from sqlalchemy import Column, String, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class SyncEvent(Base):
    __tablename__ = "sync_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pms_property_id = Column(UUID(as_uuid=True), ForeignKey("pms_properties.id"), nullable=False)
    event_type = Column(String(100), nullable=False)
    status = Column(String(50), default="queued")
    payload = Column(JSONB)
    error_message = Column(Text)
    retry_count = Column(String(10), default="0")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    pms_property = relationship("PmsProperty", back_populates="sync_events")
