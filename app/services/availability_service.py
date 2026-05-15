"""Upsert disponibilidad en la tabla canónica `disponibilidad`.

Esquema EXACTO según modelo canónico (varchar id/habitacionId, camelCase cols
unidadesDisponibles/unidadesReservadas/ultimaActualizacion/fuenteActualizacion).
"""
import logging
from datetime import date
from typing import List

from sqlalchemy.orm import Session

from app.models.disponibilidad import Disponibilidad

logger = logging.getLogger(__name__)


class AvailabilityService:
    def __init__(self, db: Session):
        self.db = db

    def upsert_batch(self, entries: List[dict]) -> List[Disponibilidad]:
        if not entries:
            return []

        results = []
        for entry in entries:
            habitacion_id = str(entry["habitacionId"])
            fecha = entry["fecha"]

            existing = (
                self.db.query(Disponibilidad)
                .filter(
                    Disponibilidad.habitacionId == habitacion_id,
                    Disponibilidad.fecha == fecha,
                )
                .first()
            )

            if existing:
                existing.unidadesDisponibles = int(entry["unidadesDisponibles"])
                existing.fuenteActualizacion = entry.get("fuente", "pms_webhook")
                results.append(existing)
            else:
                record = Disponibilidad(
                    habitacionId=habitacion_id,
                    fecha=fecha,
                    unidadesDisponibles=int(entry["unidadesDisponibles"]),
                    unidadesReservadas=int(entry.get("unidadesReservadas", 0)),
                    fuenteActualizacion=entry.get("fuente", "pms_webhook"),
                )
                self.db.add(record)
                results.append(record)

        self.db.commit()
        logger.info(f"Upserted {len(entries)} disponibilidad records")
        return results

    def get_conflicts(self, habitacion_id: str, fechas: List[date]) -> List[Disponibilidad]:
        records = (
            self.db.query(Disponibilidad)
            .filter(
                Disponibilidad.habitacionId == str(habitacion_id),
                Disponibilidad.fecha.in_(fechas),
                Disponibilidad.unidadesDisponibles < Disponibilidad.unidadesReservadas,
            )
            .all()
        )
        return records
