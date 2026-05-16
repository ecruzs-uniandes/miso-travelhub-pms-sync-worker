"""Tarifa — modelo canónico TravelHub.

pms-sync-worker hace UPSERT a la tabla canónica `tarifa` cuando recibe eventos
PMS de tipo `rate_update`. Esquema EXACTO según DEV:
  id, habitacionId: varchar
  precioBase, descuento: double precision
  moneda: varchar
  fechaInicio, fechaFin: timestamptz

Sin columnas estado / created_at / updated_at en la canónica.
"""
import uuid
from sqlalchemy import Column, DateTime, Float, ForeignKey, String
from sqlalchemy.orm import relationship
from app.database import Base


def _new_tarifa_id() -> str:
    return str(uuid.uuid4())


class Tarifa(Base):
    __tablename__ = "tarifa"

    id = Column(String, primary_key=True, default=_new_tarifa_id)
    habitacionId = Column(
        "habitacionId", String, ForeignKey("habitacion.id"), nullable=False
    )
    precioBase = Column("precioBase", Float, nullable=False)
    moneda = Column(String, nullable=False)
    fechaInicio = Column("fechaInicio", DateTime(timezone=True), nullable=False)
    fechaFin = Column("fechaFin", DateTime(timezone=True), nullable=False)
    descuento = Column(Float, nullable=False, default=0.0)

    habitacion = relationship("Habitacion", back_populates="tarifas")
