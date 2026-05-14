import uuid
from sqlalchemy import Column, Integer, Date, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class Availability(Base):
    __tablename__ = "availability"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # FK a habitacion canónica (varchar id)
    habitacionId = Column("habitacionId", String, ForeignKey("habitacion.id"), nullable=False)
    fecha = Column(Date, nullable=False)
    unidades_disponibles = Column(Integer, default=0)
    unidades_reservadas = Column(Integer, default=0)
    ultima_actualizacion = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    fuente_actualizacion = Column(String(50), default="pms_webhook")

    habitacion = relationship("Habitacion", back_populates="availabilities")

    __table_args__ = (
        UniqueConstraint("habitacionId", "fecha", name="uq_availability_habitacion_fecha"),
    )
