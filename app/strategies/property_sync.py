import logging

from sqlalchemy.orm import Session

from app.models.hotel import Hotel
from app.schemas.sync_command import SyncCommand
from app.strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)


class PropertySyncStrategy(BaseStrategy):
    """
    Sincroniza datos PMS contra la BD canónica.

    Después del refactor a habitacion (modelo canónico):
    - `_sync_hotel_info` sigue funcionando: actualiza nombre/direccion/ciudad/pais
      en `hotel` canónica (la canónica tiene esas columnas).
    - `_sync_rooms` (anterior _sync_rooms con upsert por pms_room_id) NO funciona:
      la canónica `habitacion` no tiene `pms_room_id`, `nombre`, `capacidad`, `activo`.
      Se requiere definir un contrato con search-service (owner de la tabla) o
      una tabla auxiliar de mapping pms_room_id ↔ habitacion.id.
      Por ahora se omite — se loguea warning. Las availability/tariff que
      llegan en el mismo payload se omiten también (sin mapping).
    """

    def execute(self, command: SyncCommand, db: Session) -> None:
        data = command.data
        hotel_id = str(command.hotel_id)

        try:
            if "property_info" in data:
                self._sync_hotel_info(db, hotel_id, data["property_info"])

            rooms_data = data.get("rooms")
            availability_data = data.get("availability")
            tariffs_data = data.get("tariffs")

            if rooms_data:
                logger.warning(
                    "property_sync.rooms upsert disabled — canonical habitacion "
                    "schema lacks pms_room_id / nombre / capacidad / activo. "
                    "Coordinate with search-service to define sync contract."
                )

            if availability_data:
                logger.warning(
                    "property_sync.availability skipped — depends on rooms mapping "
                    "which is currently disabled."
                )

            if tariffs_data:
                logger.warning(
                    "property_sync.tariffs skipped — depends on rooms mapping "
                    "which is currently disabled."
                )

            db.commit()
            logger.info(f"PropertySync completed for hotel={hotel_id}")

        except Exception:
            db.rollback()
            raise

    def _sync_hotel_info(self, db: Session, hotel_id: str, property_info: dict):
        hotel = db.query(Hotel).filter(Hotel.id == hotel_id).first()
        if not hotel:
            logger.warning(f"Hotel {hotel_id} not found, skipping property_info update")
            return

        if "nombre" in property_info:
            hotel.nombre = property_info["nombre"]
        if "direccion" in property_info:
            hotel.direccion = property_info["direccion"]
        if "ciudad" in property_info:
            hotel.ciudad = property_info["ciudad"]
        if "pais" in property_info:
            hotel.pais = property_info["pais"]

        db.flush()
