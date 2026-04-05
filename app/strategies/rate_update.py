import logging
from datetime import date
from sqlalchemy.orm import Session
from app.schemas.sync_command import SyncCommand
from app.strategies.base_strategy import BaseStrategy
from app.services.tariff_service import TariffService
from app.models.room import Room

logger = logging.getLogger(__name__)


class RateUpdateStrategy(BaseStrategy):
    def execute(self, command: SyncCommand, db: Session) -> None:
        data = command.data
        rates = data.get("rates", [])
        room_mappings = data.get("room_mappings", {})
        hotel_id = str(command.hotel_id)

        tariff_service = TariffService(db)
        entries = []

        for rate in rates:
            pms_room_id = rate.get("pms_room_id")
            room_id = self._resolve_room_id(db, hotel_id, pms_room_id, room_mappings)

            if not room_id:
                logger.warning(
                    f"Cannot resolve room for pms_room_id={pms_room_id}, hotel={hotel_id}. Skipping."
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
                "room_id": room_id,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "precio_por_noche": rate.get("precio_por_noche", 0),
                "moneda": rate.get("moneda", "USD"),
            })

        if entries:
            tariff_service.upsert_batch(entries)

        logger.info(
            f"RateUpdate completed: hotel={hotel_id}, {len(entries)} tariffs processed"
        )

    def _resolve_room_id(self, db: Session, hotel_id: str, pms_room_id: str, room_mappings: dict) -> str | None:
        if pms_room_id in room_mappings:
            return room_mappings[pms_room_id]

        room = (
            db.query(Room)
            .filter(Room.hotel_id == hotel_id, Room.pms_room_id == pms_room_id)
            .first()
        )
        if room:
            return str(room.id)

        return None
