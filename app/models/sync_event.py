import uuid
from sqlalchemy import Column, String, DateTime, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.database import Base


class SyncEvent(Base):
    __tablename__ = "sync_events"
    __table_args__ = {"extend_existing": True}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id = Column(String(255), unique=True, nullable=False)
    pms_provider = Column(String(100), nullable=False)
    hotel_id = Column(UUID(as_uuid=True), ForeignKey("hotels.id"))
    event_type = Column(String(50), nullable=False)
    payload_hash = Column(String(64))
    status = Column(String(20), default="received")
    retry_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    processed_at = Column(DateTime)
