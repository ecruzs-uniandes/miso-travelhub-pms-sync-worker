import logging
from datetime import datetime

from sqlalchemy.orm import Session

from app.schemas.sync_command import SyncCommand
from app.services.tarifa_service import TarifaService
from app.strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


def _parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


class RateUpdateStrategy(BaseStrategy):
    """
    Actualiza tarifas canónicas en `tarifa` por habitación.

    El caller debe proveer `room_mappings` (pms_room_id → habitacion.id varchar).
    Si no hay mapping para un pms_room_id, el rate se omite con warning.

    Payload `rates[*]` esperado:
      pms_room_id   → resolvedo a habitacionId via room_mappings
      precio_base | precio_por_noche  → precioBase
      moneda                          → moneda
      fecha_inicio | fechaInicio      → fechaInicio (str ISO o datetime)
      fecha_fin    | fechaFin         → fechaFin
      descuento                       → descuento (default 0.0)
    """

    def execute(self, command: SyncCommand, db: Session) -> None:
        data = command.data
        rates = data.get("rates", [])
        room_mappings = data.get("room_mappings", {})
        hotel_id = str(command.hotel_id)

        tarifa_service = TarifaService(db)
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

            fecha_inicio = _parse_dt(rate.get("fechaInicio") or rate["fecha_inicio"])
            fecha_fin = _parse_dt(rate.get("fechaFin") or rate["fecha_fin"])

            entries.append({
                "habitacionId": habitacion_id,
                "fechaInicio": fecha_inicio,
                "fechaFin": fecha_fin,
                "precioBase": rate.get("precioBase") or rate.get("precio_base") or rate.get("precio_por_noche", 0),
                "moneda": rate.get("moneda", "USD"),
                "descuento": rate.get("descuento", 0.0),
            })

        if entries:
            tarifa_service.upsert_batch(entries)

        logger.info(
            "RateUpdate completed: hotel=%s, %d tarifas processed",
            hotel_id, len(entries),
        )
