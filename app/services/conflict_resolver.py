import logging
from typing import List
from app.models.disponibilidad import Disponibilidad
from app.services.notification_client import NotificationClient

logger = logging.getLogger(__name__)


class ConflictResolver:
    def __init__(self, notification_client: NotificationClient):
        self.notification_client = notification_client

    def resolve(self, hotel_id: str, conflicts: List[Disponibilidad]) -> List[dict]:
        resolved = []
        for record in conflicts:
            conflict_info = self._analyze_conflict(hotel_id, record)
            resolved.append(conflict_info)
            self._handle_conflict(hotel_id, record, conflict_info)
        return resolved

    def _analyze_conflict(self, hotel_id: str, record: Disponibilidad) -> dict:
        disponibles = record.unidadesDisponibles
        reservadas = record.unidadesReservadas

        if disponibles == 0 and reservadas > 0:
            conflict_type = "critical_zero_availability"
            severity = "critical"
        elif reservadas > disponibles:
            conflict_type = "overbooking"
            severity = "high"
        else:
            conflict_type = "none"
            severity = "low"

        return {
            "habitacionId": record.habitacionId,
            "fecha": str(record.fecha),
            "unidadesDisponibles": disponibles,
            "unidadesReservadas": reservadas,
            "conflict_type": conflict_type,
            "severity": severity,
        }

    def _handle_conflict(self, hotel_id: str, record: Disponibilidad, conflict_info: dict):
        conflict_type = conflict_info["conflict_type"]

        if conflict_type == "critical_zero_availability":
            logger.error(
                f"CRITICAL CONFLICT: hotel={hotel_id}, habitacion={record.habitacionId}, "
                f"fecha={record.fecha} — PMS reports 0 but {record.unidadesReservadas} reservations exist"
            )
            self.notification_client.notify_conflict(
                hotel_id=hotel_id,
                conflict_type="pms_sync_conflict",
                details=conflict_info,
                recipients=["hotel_admin", "platform_admin"],
            )

        elif conflict_type == "overbooking":
            logger.warning(
                f"OVERBOOKING DETECTED: hotel={hotel_id}, habitacion={record.habitacionId}, "
                f"fecha={record.fecha} — available={record.unidadesDisponibles}, "
                f"reserved={record.unidadesReservadas}"
            )
            self.notification_client.notify_conflict(
                hotel_id=hotel_id,
                conflict_type="pms_sync_conflict",
                details=conflict_info,
                recipients=["hotel_admin"],
            )
