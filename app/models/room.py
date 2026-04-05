import uuid
from sqlalchemy import Column, String, Text, Boolean, Integer, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class Room(Base):
    __tablename__ = "rooms"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hotel_id = Column(UUID(as_uuid=True), ForeignKey("hotels.id"), nullable=False)
    nombre = Column(String(255), nullable=False)
    descripcion = Column(Text)
    capacidad = Column(Integer, default=2)
    activo = Column(Boolean, default=True)
    pms_room_id = Column(String(255))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    hotel = relationship("Hotel", back_populates="rooms")
    availabilities = relationship("Availability", back_populates="room")
    tariffs = relationship("Tariff", back_populates="room")
