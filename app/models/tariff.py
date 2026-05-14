import uuid
from sqlalchemy import Column, Numeric, Date, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class Tariff(Base):
    __tablename__ = "tariffs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # FK a habitacion canónica (varchar id)
    habitacionId = Column("habitacionId", String, ForeignKey("habitacion.id"), nullable=False)
    fecha_inicio = Column(Date, nullable=False)
    fecha_fin = Column(Date, nullable=False)
    precio_por_noche = Column(Numeric(10, 2), nullable=False)
    moneda = Column(String(10), default="USD")
    fuente = Column(String(50), default="pms_webhook")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    habitacion = relationship("Habitacion", back_populates="tariffs")
