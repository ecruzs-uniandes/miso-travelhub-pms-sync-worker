"""Disponibilidad — modelo canónico TravelHub (antes 'availability')."""
import uuid
from sqlalchemy import Column, Integer, Date, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


def _new_disponibilidad_id() -> str:
    return str(uuid.uuid4())


class Disponibilidad(Base):
    __tablename__ = "disponibilidad"

    id = Column(String, primary_key=True, default=_new_disponibilidad_id)
    habitacionId = Column(
        "habitacionId", String, ForeignKey("habitacion.id"), nullable=False
    )
    fecha = Column(Date, nullable=False)
    unidadesDisponibles = Column("unidadesDisponibles", Integer, nullable=False, default=0)
    unidadesReservadas = Column("unidadesReservadas", Integer, nullable=False, default=0)
    ultimaActualizacion = Column(
        "ultimaActualizacion",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
    fuenteActualizacion = Column("fuenteActualizacion", String(50), default="pms_webhook")

    habitacion = relationship("Habitacion", back_populates="disponibilidades")

    __table_args__ = (
        UniqueConstraint(
            "habitacionId", "fecha", name="uq_disponibilidad_habitacion_fecha"
        ),
    )
