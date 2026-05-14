"""Hotel — modelo canónico del proyecto.

pms-sync actualiza algunos campos vía webhook PMS (nombre, direccion, ciudad,
pais) en `_sync_hotel_info`. La canónica tiene más columnas (latitud, longitud,
estrellas, etc.) — las mapeamos para consistencia aunque no las actualicemos.
"""
from sqlalchemy import Boolean, Column, Float, Integer, String
from sqlalchemy.orm import relationship
from app.database import Base


class Hotel(Base):
    __tablename__ = "hotel"

    id = Column(String, primary_key=True)
    nombre = Column(String)
    direccion = Column(String)
    ciudad = Column(String)
    pais = Column(String)
    latitud = Column(Float)
    longitud = Column(Float)
    estrellas = Column(Integer)
    pmsProveedor = Column("pmsProveedor", String)
    activo = Column(Boolean)

    habitaciones = relationship("Habitacion", back_populates="hotel")
    pms_properties = relationship("PmsProperty", back_populates="hotel")
