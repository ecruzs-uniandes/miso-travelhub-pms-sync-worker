import logging
import uuid
from datetime import date
from typing import List
from uuid import UUID
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.models.availability import Availability

logger = logging.getLogger(__name__)


class AvailabilityService:
    def __init__(self, db: Session):
        self.db = db

    def upsert_batch(self, entries: List[dict]) -> List[Availability]:
        if not entries:
            return []

        upsert_sql = text("""
            INSERT INTO availability (id, room_id, fecha, unidades_disponibles, unidades_reservadas, ultima_actualizacion, fuente_actualizacion)
            VALUES (:id, :room_id, :fecha, :disponibles, :reservadas, now(), :fuente)
            ON CONFLICT (room_id, fecha)
            DO UPDATE SET
                unidades_disponibles = EXCLUDED.unidades_disponibles,
                ultima_actualizacion = now(),
                fuente_actualizacion = EXCLUDED.fuente_actualizacion
        """)

        params = [
            {
                "id": str(uuid.uuid4()),
                "room_id": str(entry["room_id"]),
                "fecha": entry["fecha"],
                "disponibles": entry["unidades_disponibles"],
                "reservadas": entry.get("unidades_reservadas", 0),
                "fuente": entry.get("fuente", "pms_webhook"),
            }
            for entry in entries
        ]

        self.db.execute(upsert_sql, params)
        self.db.commit()

        room_ids = list({str(e["room_id"]) for e in entries})
        fechas = [e["fecha"] for e in entries]

        result = (
            self.db.query(Availability)
            .filter(
                Availability.room_id.in_(room_ids),
                Availability.fecha.in_(fechas),
            )
            .all()
        )
        logger.info(f"Upserted {len(entries)} availability records")
        return result

    def get_conflicts(self, room_id: UUID, fechas: List[date]) -> List[Availability]:
        records = (
            self.db.query(Availability)
            .filter(
                Availability.room_id == room_id,
                Availability.fecha.in_(fechas),
                Availability.unidades_disponibles < Availability.unidades_reservadas,
            )
            .all()
        )
        return records
