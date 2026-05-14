import logging
import uuid as uuid_mod
from typing import List

from sqlalchemy.orm import Session

from app.models.tariff import Tariff

logger = logging.getLogger(__name__)


class TariffService:
    def __init__(self, db: Session):
        self.db = db

    def upsert_tariff(
        self,
        habitacion_id: str,
        fecha_inicio,
        fecha_fin,
        precio: float,
        moneda: str = "USD",
    ) -> Tariff:
        existing = (
            self.db.query(Tariff)
            .filter(
                Tariff.habitacionId == habitacion_id,
                Tariff.fecha_inicio == fecha_inicio,
                Tariff.fecha_fin == fecha_fin,
            )
            .first()
        )

        if existing:
            existing.precio_por_noche = precio
            existing.moneda = moneda
            existing.fuente = "pms_webhook"
            self.db.commit()
            self.db.refresh(existing)
            return existing

        tariff = Tariff(
            id=uuid_mod.uuid4(),
            habitacionId=habitacion_id,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            precio_por_noche=precio,
            moneda=moneda,
            fuente="pms_webhook",
        )
        self.db.add(tariff)
        self.db.commit()
        self.db.refresh(tariff)
        logger.info(f"Upserted tariff for habitacion {habitacion_id}: {fecha_inicio} - {fecha_fin} @ {precio} {moneda}")
        return tariff

    def upsert_batch(self, entries: List[dict]) -> List[Tariff]:
        results = []
        for entry in entries:
            t = self.upsert_tariff(
                habitacion_id=entry["habitacionId"],
                fecha_inicio=entry["fecha_inicio"],
                fecha_fin=entry["fecha_fin"],
                precio=entry["precio_por_noche"],
                moneda=entry.get("moneda", "USD"),
            )
            results.append(t)
        return results
