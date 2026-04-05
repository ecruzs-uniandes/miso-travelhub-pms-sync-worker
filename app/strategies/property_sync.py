import logging
import uuid
from datetime import date
from sqlalchemy.orm import Session
from app.schemas.sync_command import SyncCommand
from app.strategies.base_strategy import BaseStrategy
from app.services.availability_service import AvailabilityService
from app.services.tariff_service import TariffService
from app.models.hotel import Hotel
from app.models.room import Room

logger = logging.getLogger(__name__)


class PropertySyncStrategy(BaseStrategy):
    def execute(self, command: SyncCommand, db: Session) -> None:
        data = command.data
        hotel_id = str(command.hotel_id)

        try:
            if "property_info" in data:
                self._sync_hotel_info(db, hotel_id, data["property_info"])

            room_id_map = {}
            if "rooms" in data:
                room_id_map = self._sync_rooms(db, hotel_id, data["rooms"])

            if "availability" in data:
                availability_service = AvailabilityService(db)
                av_entries = []
                for entry in data["availability"]:
                    pms_room_id = entry.get("pms_room_id")
                    room_id = room_id_map.get(pms_room_id)
                    if room_id:
                        fecha = date.fromisoformat(entry["fecha"]) if isinstance(entry["fecha"], str) else entry["fecha"]
                        av_entries.append({
                            "room_id": room_id,
                            "fecha": fecha,
                            "unidades_disponibles": entry.get("unidades_disponibles", 0),
                            "fuente": "pms_webhook",
                        })
                if av_entries:
                    availability_service.upsert_batch(av_entries)

            if "tariffs" in data:
                tariff_service = TariffService(db)
                tariff_entries = []
                for rate in data["tariffs"]:
                    pms_room_id = rate.get("pms_room_id")
                    room_id = room_id_map.get(pms_room_id)
                    if room_id:
                        tariff_entries.append({
                            "room_id": room_id,
                            "fecha_inicio": date.fromisoformat(rate["fecha_inicio"]) if isinstance(rate["fecha_inicio"], str) else rate["fecha_inicio"],
                            "fecha_fin": date.fromisoformat(rate["fecha_fin"]) if isinstance(rate["fecha_fin"], str) else rate["fecha_fin"],
                            "precio_por_noche": rate.get("precio_por_noche", 0),
                            "moneda": rate.get("moneda", "USD"),
                        })
                if tariff_entries:
                    tariff_service.upsert_batch(tariff_entries)

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

    def _sync_rooms(self, db: Session, hotel_id: str, rooms_data: list) -> dict:
        room_id_map = {}
        incoming_pms_ids = {r["pms_room_id"] for r in rooms_data if "pms_room_id" in r}

        existing_rooms = db.query(Room).filter(Room.hotel_id == hotel_id).all()

        for room in existing_rooms:
            if room.pms_room_id and room.pms_room_id not in incoming_pms_ids:
                room.activo = False

        for room_data in rooms_data:
            pms_room_id = room_data.get("pms_room_id")
            existing = next(
                (r for r in existing_rooms if r.pms_room_id == pms_room_id), None
            )

            if existing:
                existing.nombre = room_data.get("nombre", existing.nombre)
                existing.capacidad = room_data.get("capacidad", existing.capacidad)
                existing.activo = True
                room_id_map[pms_room_id] = str(existing.id)
            else:
                new_room = Room(
                    id=uuid.uuid4(),
                    hotel_id=hotel_id,
                    nombre=room_data.get("nombre", f"Room {pms_room_id}"),
                    capacidad=room_data.get("capacidad", 2),
                    pms_room_id=pms_room_id,
                    activo=True,
                )
                db.add(new_room)
                db.flush()
                room_id_map[pms_room_id] = str(new_room.id)

        return room_id_map
