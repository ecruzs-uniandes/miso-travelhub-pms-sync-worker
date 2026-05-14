import logging
import uuid
from datetime import date
from typing import List

from sqlalchemy.orm import Session

from app.models.availability import Availability

logger = logging.getLogger(__name__)


class AvailabilityService:
    def __init__(self, db: Session):
        self.db = db

    def upsert_batch(self, entries: List[dict]) -> List[Availability]:
        if not entries:
            return []

        results = []
        for entry in entries:
            habitacion_id = str(entry["habitacionId"])
            fecha = entry["fecha"]

            existing = (
                self.db.query(Availability)
                .filter(
                    Availability.habitacionId == habitacion_id,
                    Availability.fecha == fecha,
                )
                .first()
            )

            if existing:
                existing.unidades_disponibles = entry["unidades_disponibles"]
                existing.fuente_actualizacion = entry.get("fuente", "pms_webhook")
                results.append(existing)
            else:
                record = Availability(
                    id=uuid.uuid4(),
                    habitacionId=habitacion_id,
                    fecha=fecha,
                    unidades_disponibles=entry["unidades_disponibles"],
                    unidades_reservadas=entry.get("unidades_reservadas", 0),
                    fuente_actualizacion=entry.get("fuente", "pms_webhook"),
                )
                self.db.add(record)
                results.append(record)

        self.db.commit()
        logger.info(f"Upserted {len(entries)} availability records")
        return results

    def get_conflicts(self, habitacion_id: str, fechas: List[date]) -> List[Availability]:
        records = (
            self.db.query(Availability)
            .filter(
                Availability.habitacionId == str(habitacion_id),
                Availability.fecha.in_(fechas),
                Availability.unidades_disponibles < Availability.unidades_reservadas,
            )
            .all()
        )
        return records
