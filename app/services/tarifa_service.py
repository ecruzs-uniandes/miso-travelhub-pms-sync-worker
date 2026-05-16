"""Upsert tarifas en la tabla canónica `tarifa`.

Alineado al esquema EXACTO de DEV (varchar id/habitacionId, double precision
precioBase/descuento, timestamptz fechaInicio/fechaFin). No hay columna `estado`
ni `created_at` en la canónica — la auditoría histórica vive en inventory.
"""
import logging
from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from app.models.tarifa import Tarifa

logger = logging.getLogger(__name__)


def _to_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


class TarifaService:
    def __init__(self, db: Session):
        self.db = db

    def upsert_tarifa(
        self,
        habitacion_id: str,
        fecha_inicio,
        fecha_fin,
        precio: float,
        moneda: str = "USD",
        descuento: float = 0.0,
    ) -> Tarifa:
        fecha_inicio_dt = _to_datetime(fecha_inicio)
        fecha_fin_dt = _to_datetime(fecha_fin)

        existing = (
            self.db.query(Tarifa)
            .filter(
                Tarifa.habitacionId == habitacion_id,
                Tarifa.fechaInicio == fecha_inicio_dt,
                Tarifa.fechaFin == fecha_fin_dt,
            )
            .first()
        )

        if existing:
            existing.precioBase = float(precio)
            existing.moneda = moneda
            existing.descuento = float(descuento)
            self.db.commit()
            self.db.refresh(existing)
            return existing

        tarifa = Tarifa(
            habitacionId=habitacion_id,
            fechaInicio=fecha_inicio_dt,
            fechaFin=fecha_fin_dt,
            precioBase=float(precio),
            moneda=moneda,
            descuento=float(descuento),
        )
        self.db.add(tarifa)
        self.db.commit()
        self.db.refresh(tarifa)
        logger.info(
            "Upserted tarifa habitacion=%s %s..%s @ %s %s",
            habitacion_id, fecha_inicio_dt, fecha_fin_dt, precio, moneda,
        )
        return tarifa

    def upsert_batch(self, entries: List[dict]) -> List[Tarifa]:
        results = []
        for entry in entries:
            t = self.upsert_tarifa(
                habitacion_id=entry["habitacionId"],
                fecha_inicio=entry["fechaInicio"],
                fecha_fin=entry["fechaFin"],
                precio=entry["precioBase"],
                moneda=entry.get("moneda", "USD"),
                descuento=entry.get("descuento", 0.0),
            )
            results.append(t)
        return results
