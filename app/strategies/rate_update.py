import logging
from datetime import date

from sqlalchemy.orm import Session

from app.schemas.sync_command import SyncCommand
from app.services.tariff_service import TariffService
from app.strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class RateUpdateStrategy(BaseStrategy):
    """
    Actualiza tarifas en `tariffs` por habitacion.

    Después del refactor a habitacion canónica, NO podemos resolver `pms_room_id`
    contra `habitacion` (la canónica no tiene esa columna). El caller debe
    proveer `habitacion_id` directamente en `room_mappings` (clave = pms_room_id,
    valor = habitacion.id varchar) o agregar el mapping en el campo nuevo
    `room_mappings`. Si no hay mapping, el rate se skip-ea.
    """

    def execute(self, command: SyncCommand, db: Session) -> None:
        data = command.data
        rates = data.get("rates", [])
        room_mappings = data.get("room_mappings", {})
        hotel_id = str(command.hotel_id)

        tariff_service = TariffService(db)
        entries = []

        for rate in rates:
            pms_room_id = rate.get("pms_room_id")
            habitacion_id = room_mappings.get(pms_room_id)

            if not habitacion_id:
                logger.warning(
                    "Cannot resolve habitacion for pms_room_id=%s, hotel=%s — "
                    "needs room_mappings entry. Skipping.",
                    pms_room_id, hotel_id,
                )
                continue

            fecha_inicio = (
                date.fromisoformat(rate["fecha_inicio"])
                if isinstance(rate["fecha_inicio"], str)
                else rate["fecha_inicio"]
            )
            fecha_fin = (
                date.fromisoformat(rate["fecha_fin"])
                if isinstance(rate["fecha_fin"], str)
                else rate["fecha_fin"]
            )

            entries.append({
                "habitacionId": habitacion_id,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "precio_por_noche": rate.get("precio_por_noche", 0),
                "moneda": rate.get("moneda", "USD"),
            })

        if entries:
            tariff_service.upsert_batch(entries)

        logger.info(
            "RateUpdate completed: hotel=%s, %d tariffs processed",
            hotel_id, len(entries),
        )
