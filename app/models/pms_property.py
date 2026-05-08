import uuid
from sqlalchemy import Column, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class PmsProperty(Base):
    __tablename__ = "pms_properties"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hotel_id = Column(UUID(as_uuid=True), ForeignKey("hotels.id"), nullable=False)
    pms_provider = Column(String(100), nullable=False)
    pms_property_id = Column(String(255), nullable=False)
    last_sync_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    hotel = relationship("Hotel", back_populates="pms_properties")

    __table_args__ = (
        UniqueConstraint("hotel_id", "pms_provider", name="uq_pms_property_hotel_provider"),
    )
