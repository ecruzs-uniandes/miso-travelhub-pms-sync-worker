import logging
from datetime import date

from sqlalchemy.orm import Session

from app.schemas.sync_command import SyncCommand
from app.services.availability_service import AvailabilityService
from app.services.conflict_resolver import ConflictResolver
from app.services.notification_client import NotificationClient
from app.strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class AvailabilityUpdateStrategy(BaseStrategy):
    """
    Actualiza availability por habitacion.

    El payload usa la key `habitacion_id` (varchar, canónica). Si llega legacy
    como `room_id` se acepta por compatibilidad transicional pero ya debería
    ser `habitacion_id` en producción.
    """

    def execute(self, command: SyncCommand, db: Session) -> None:
        data = command.data
        hotel_id = str(command.hotel_id)

        habitacion_id = data.get("habitacion_id") or data.get("room_id")
        date_entries = data.get("dates", [])

        if not habitacion_id:
            logger.warning(f"No habitacion_id in payload for event {command.event_id}. Skipping.")
            return

        availability_service = AvailabilityService(db)
        notification_client = NotificationClient()
        conflict_resolver = ConflictResolver(notification_client)

        upsert_entries = []
        fechas_list = []

        for entry in date_entries:
            fecha_raw = entry.get("date") or entry.get("fecha")
            if not fecha_raw:
                continue
            fecha = date.fromisoformat(fecha_raw) if isinstance(fecha_raw, str) else fecha_raw

            units = entry.get("available_units")
            if units is None:
                units = entry.get("unidades_disponibles", 0)

            upsert_entries.append({
                "habitacionId": habitacion_id,
                "fecha": fecha,
                "unidades_disponibles": int(units),
                "fuente": "pms_webhook",
            })
            fechas_list.append(fecha)

        if upsert_entries:
            availability_service.upsert_batch(upsert_entries)

        if fechas_list:
            conflicts = availability_service.get_conflicts(habitacion_id, fechas_list)
            if conflicts:
                logger.warning(f"Found {len(conflicts)} conflicts for habitacion {habitacion_id}")
                conflict_resolver.resolve(hotel_id, conflicts)

        logger.info(
            f"AvailabilityUpdate completed: hotel={hotel_id}, "
            f"habitacion={habitacion_id}, {len(upsert_entries)} records processed"
        )
