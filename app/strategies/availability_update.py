import logging
from datetime import date
from sqlalchemy.orm import Session
from app.schemas.sync_command import SyncCommand
from app.strategies.base_strategy import BaseStrategy
from app.services.availability_service import AvailabilityService
from app.services.conflict_resolver import ConflictResolver
from app.services.notification_client import NotificationClient
from app.models.room import Room

logger = logging.getLogger(__name__)


class AvailabilityUpdateStrategy(BaseStrategy):
    def execute(self, command: SyncCommand, db: Session) -> None:
        data = command.data
        room_mappings = data.get("room_mappings", {})
        date_entries = data.get("dates", [])

        hotel_id = str(command.hotel_id)
        availability_service = AvailabilityService(db)
        notification_client = NotificationClient()
        conflict_resolver = ConflictResolver(notification_client)

        upsert_entries = []
        conflict_check: dict[str, list[date]] = {}

        for entry in date_entries:
            pms_room_id = entry.get("pms_room_id")
            room_id = self._resolve_room_id(db, hotel_id, pms_room_id, room_mappings)

            if not room_id:
                logger.warning(
                    f"Cannot resolve room for pms_room_id={pms_room_id}, hotel={hotel_id}. Skipping."
                )
                continue

            fecha = date.fromisoformat(entry["fecha"]) if isinstance(entry["fecha"], str) else entry["fecha"]
            unidades_disponibles = entry.get("unidades_disponibles", 0)

            upsert_entries.append({
                "room_id": room_id,
                "fecha": fecha,
                "unidades_disponibles": unidades_disponibles,
                "fuente": "pms_webhook",
            })

            if room_id not in conflict_check:
                conflict_check[room_id] = []
            conflict_check[room_id].append(fecha)

        if upsert_entries:
            availability_service.upsert_batch(upsert_entries)

        for room_id, fechas in conflict_check.items():
            conflicts = availability_service.get_conflicts(room_id, fechas)
            if conflicts:
                logger.warning(f"Found {len(conflicts)} conflicts for room {room_id}")
                conflict_resolver.resolve(hotel_id, conflicts)

        logger.info(
            f"AvailabilityUpdate completed: hotel={hotel_id}, {len(upsert_entries)} records processed"
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
