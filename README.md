# pms-sync-worker

Worker de sincronización PMS para **TravelHub** — MISW4501 Grupo 9 — Uniandes.

Consume comandos del topic Kafka `pms-sync-queue` y los procesa contra PostgreSQL: actualiza disponibilidad, tarifas y estado de sincronización.

---

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Endpoints HTTP](#endpoints-http)
- [Contrato Kafka — SyncCommand](#contrato-kafka--synccommand)
  - [availability_update](#availability_update)
  - [rate_update](#rate_update)
  - [property_sync](#property_sync)
- [Procesamiento de mensajes](#procesamiento-de-mensajes)
- [Resolución de conflictos](#resolución-de-conflictos)
- [Resiliencia](#resiliencia)
- [Variables de entorno](#variables-de-entorno)
- [Correr localmente](#correr-localmente)
- [Tests](#tests)
- [Deploy a Cloud Run](#deploy-a-cloud-run)
- [Colección Postman](#colección-postman)

---

## Arquitectura

```
pms-integration-services
        │
        │  produce SyncCommand
        ▼
  Kafka topic: pms-sync-queue
        │
        │  consume (manual commit)
        ▼
  pms-sync-worker
   ├── CommandHandler (Strategy Router)
   │     ├── AvailabilityUpdateStrategy → UPSERT availability
   │     ├── RateUpdateStrategy         → UPSERT tariffs
   │     └── PropertySyncStrategy       → Sync completo (transacción)
   │
   ├── ConflictResolver → detecta overbooking → NotificationClient
   ├── CircuitBreaker   → protege BD y notification-services
   └── RetryHandler     → exponential backoff, re-publica en Kafka
```

**Patrones aplicados:**
- **Command** (GoF): cada mensaje Kafka es un `SyncCommand` ejecutable con retry y trazabilidad
- **Strategy** (GoF): procesadores intercambiables por `event_type`
- **Circuit Breaker**: protección ante fallos de BD o servicios downstream
- **Observer**: notifica conflictos a `notification-services`

**ASR cubierto:** AH004 — sincronizar 1,200 propiedades en ≤ 2 minutos con reintentos e idempotencia.

---

## Endpoints HTTP

El worker expone un único endpoint HTTP (puerto 8000) para que Cloud Run pueda hacer health checks.

### `GET /health`

Verifica que el servicio está activo.

**Request:**
```
GET http://localhost:8000/health
```

**Response `200 OK`:**
```json
{
  "status": "healthy",
  "service": "pms-sync-worker"
}
```

---

## Contrato Kafka — SyncCommand

Todos los mensajes en el topic `pms-sync-queue` siguen este esquema base:

```json
{
  "event_id": "uuid-v4",
  "event_type": "availability_update | rate_update | property_sync",
  "hotel_id": "uuid-v4",
  "pms_provider": "sabre | opera | cloudbeds | ...",
  "pms_property_id": "string",
  "retry_count": 0,
  "correlation_id": "opcional-para-trazabilidad",
  "data": { ... }
}
```

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `event_id` | UUID | Sí | Identificador único del evento. Garantiza idempotencia. |
| `event_type` | string | Sí | Tipo de operación: `availability_update`, `rate_update`, `property_sync` |
| `hotel_id` | UUID | Sí | ID del hotel en TravelHub |
| `pms_provider` | string | Sí | Nombre del proveedor PMS |
| `pms_property_id` | string | Sí | ID de la propiedad en el PMS externo |
| `retry_count` | int | No (default: 0) | Número de reintentos previos |
| `correlation_id` | string | No | ID para correlacionar con el request HTTP original |
| `data` | object | Sí | Payload específico según `event_type` |

---

### `availability_update`

Actualiza la disponibilidad de habitaciones para fechas específicas.

**Payload `data`:**
```json
{
  "room_mappings": {
    "PMS-ROOM-001": "uuid-room-travelhub"
  },
  "dates": [
    {
      "pms_room_id": "PMS-ROOM-001",
      "fecha": "2025-07-01",
      "unidades_disponibles": 5
    }
  ]
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `room_mappings` | object | Mapeo `pms_room_id → room_id TravelHub`. Opcional si las rooms ya tienen `pms_room_id` en BD. |
| `dates[].pms_room_id` | string | ID de la habitación en el PMS |
| `dates[].fecha` | string (ISO 8601) | Fecha de disponibilidad (`YYYY-MM-DD`) |
| `dates[].unidades_disponibles` | int | Unidades disponibles reportadas por el PMS |

**SQL ejecutado:**
```sql
INSERT INTO availability (id, room_id, fecha, unidades_disponibles, unidades_reservadas, ultima_actualizacion, fuente_actualizacion)
VALUES (gen_random_uuid(), :room_id, :fecha, :disponibles, 0, now(), 'pms_webhook')
ON CONFLICT (room_id, fecha)
DO UPDATE SET
    unidades_disponibles = EXCLUDED.unidades_disponibles,
    ultima_actualizacion = now(),
    fuente_actualizacion = 'pms_webhook';
```

**Ejemplo completo:**
```json
{
  "event_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "event_type": "availability_update",
  "hotel_id": "550e8400-e29b-41d4-a716-446655440000",
  "pms_provider": "sabre",
  "pms_property_id": "SABRE-001",
  "retry_count": 0,
  "data": {
    "room_mappings": {
      "PMS-ROOM-001": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    },
    "dates": [
      {"pms_room_id": "PMS-ROOM-001", "fecha": "2025-07-01", "unidades_disponibles": 5},
      {"pms_room_id": "PMS-ROOM-001", "fecha": "2025-07-02", "unidades_disponibles": 4},
      {"pms_room_id": "PMS-ROOM-001", "fecha": "2025-07-03", "unidades_disponibles": 3}
    ]
  }
}
```

---

### `rate_update`

Actualiza tarifas (precio por noche) para rangos de fechas.

**Payload `data`:**
```json
{
  "room_mappings": {
    "PMS-ROOM-001": "uuid-room-travelhub"
  },
  "rates": [
    {
      "pms_room_id": "PMS-ROOM-001",
      "fecha_inicio": "2025-07-01",
      "fecha_fin": "2025-07-31",
      "precio_por_noche": 120.00,
      "moneda": "USD"
    }
  ]
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `rates[].pms_room_id` | string | ID de la habitación en el PMS |
| `rates[].fecha_inicio` | string (ISO 8601) | Inicio del período de vigencia |
| `rates[].fecha_fin` | string (ISO 8601) | Fin del período de vigencia |
| `rates[].precio_por_noche` | float | Precio por noche en la moneda indicada |
| `rates[].moneda` | string | Código ISO 4217 (`USD`, `COP`, `EUR`) |

**Comportamiento UPSERT:**
- Si existe `(room_id, fecha_inicio, fecha_fin)` → actualiza `precio_por_noche`
- Si no existe → inserta nuevo registro

**Ejemplo completo:**
```json
{
  "event_id": "b2c3d4e5-0000-0000-0000-000000000001",
  "event_type": "rate_update",
  "hotel_id": "550e8400-e29b-41d4-a716-446655440000",
  "pms_provider": "sabre",
  "pms_property_id": "SABRE-001",
  "retry_count": 0,
  "data": {
    "room_mappings": {"PMS-ROOM-001": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
    "rates": [
      {
        "pms_room_id": "PMS-ROOM-001",
        "fecha_inicio": "2025-12-20",
        "fecha_fin": "2026-01-05",
        "precio_por_noche": 280.00,
        "moneda": "USD"
      }
    ]
  }
}
```

---

### `property_sync`

Sincronización completa de una propiedad. Ejecuta en una sola transacción.

**Payload `data`:**
```json
{
  "property_info": {
    "nombre": "Hotel Boutique Candelaria",
    "direccion": "Calle 12 #4-72",
    "ciudad": "Bogotá",
    "pais": "Colombia"
  },
  "rooms": [
    {"pms_room_id": "OPERA-101", "nombre": "Habitación Estándar", "capacidad": 2}
  ],
  "availability": [
    {"pms_room_id": "OPERA-101", "fecha": "2025-07-01", "unidades_disponibles": 1}
  ],
  "tariffs": [
    {
      "pms_room_id": "OPERA-101",
      "fecha_inicio": "2025-07-01",
      "fecha_fin": "2025-07-31",
      "precio_por_noche": 95.00,
      "moneda": "USD"
    }
  ]
}
```

Todos los campos de `data` son opcionales. Solo se procesan los que están presentes.

**Flujo de ejecución:**
1. `property_info` → actualiza campos del hotel si cambiaron
2. `rooms` → crea nuevas habitaciones, desactiva las que ya no existen en el PMS
3. `availability` → UPSERT en tabla `availability`
4. `tariffs` → UPSERT en tabla `tariffs`
5. `COMMIT` — si cualquier paso falla → `ROLLBACK` completo

---

## Procesamiento de mensajes

```
Kafka message
     │
     ▼
deserialize → SyncCommand
     │
     ▼
CommandHandler.process(command)
     │
     ├── update sync_events.status = "processing"
     │
     ├── strategy.execute(command, db)
     │        │
     │        ├── success → status = "completed"
     │        │             update pms_properties.last_sync_at
     │        │             consumer.commit(msg)
     │        │
     │        └── error
     │               ├── retry_count < MAX_RETRIES (3)
     │               │       → re-publish with retry_count+1
     │               │       → consumer.commit(msg)
     │               │
     │               └── retry_count >= MAX_RETRIES
     │                       → status = "failed"
     │                       → consumer.commit(msg)
     │
     └── NonRetryableError (event_type desconocido)
             → status = "failed"
             → consumer.commit(msg)  ← sin retry
```

**Manual commit:** el offset Kafka solo se commitea DESPUÉS de procesar (éxito o fallo definitivo). Si el worker muere durante el procesamiento, el mensaje se re-entrega.

---

## Resolución de conflictos

Cuando `unidades_disponibles < unidades_reservadas` después de un `availability_update`:

| Escenario | Tipo | Acción |
|---|---|---|
| PMS reporta `disponibles=0` pero hay reservas activas | `critical_zero_availability` | Notifica a `hotel_admin` + `platform_admin`. NO cancela reservas. |
| PMS reporta `disponibles=N` pero `reservadas > disponibles` | `overbooking` | Notifica a `hotel_admin`. |
| PMS reporta `disponibles > reservadas` | `none` | Actualización normal, sin notificación. |

La cancelación de reservas la decide un humano o `booking-services`. Este worker **solo notifica**.

---

## Resiliencia

### Circuit Breaker

Protege llamadas a PostgreSQL y a `notification-services`.

| Estado | Descripción |
|---|---|
| `CLOSED` | Funcionamiento normal. Cuenta fallos consecutivos. |
| `OPEN` | Después de `CB_FAILURE_THRESHOLD` (5) fallos → rechaza llamadas. |
| `HALF_OPEN` | Después de `CB_RECOVERY_TIMEOUT` (30s) → permite 1 llamada de prueba. |

Transición `HALF_OPEN → CLOSED` si la prueba es exitosa. `HALF_OPEN → OPEN` si falla.

### Retry con Exponential Backoff

| Intento | Delay mínimo | Delay máximo |
|---|---|---|
| 1 (retry_count=0→1) | 1s | 2s |
| 2 (retry_count=1→2) | 2s | 3s |
| 3 (retry_count=2→3) | 4s | 5s |
| > 3 | — | Marca como `failed` |

Fórmula: `delay = RETRY_BACKOFF_BASE ** retry_count + random(0, 1)`

Los mensajes que exceden `MAX_RETRIES` se marcan como `failed` en `sync_events`. En producción deberían ir a un topic Dead Letter Queue (`pms-sync-dlq`).

---

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `DATABASE_HOST` | `localhost` | Host de PostgreSQL |
| `DATABASE_PORT` | `5432` | Puerto de PostgreSQL |
| `DATABASE_NAME` | `travelhub` | Nombre de la base de datos |
| `DATABASE_USER` | `travelhub_app` | Usuario de BD |
| `DATABASE_PASSWORD` | — | Contraseña de BD |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers Kafka |
| `KAFKA_TOPIC_PMS_SYNC` | `pms-sync-queue` | Topic a consumir |
| `KAFKA_CONSUMER_GROUP` | `pms-sync-worker-group` | Consumer group ID |
| `KAFKA_ENABLED` | `true` | `false` activa modo fallback por polling de BD |
| `NOTIFICATION_SERVICE_URL` | `http://localhost:8001` | URL del servicio de notificaciones |
| `MAX_RETRIES` | `3` | Máximo de reintentos por mensaje |
| `RETRY_BACKOFF_BASE` | `2` | Base del backoff exponencial (segundos) |
| `CB_FAILURE_THRESHOLD` | `5` | Fallos antes de abrir el circuit breaker |
| `CB_RECOVERY_TIMEOUT` | `30` | Segundos antes de intentar HALF_OPEN |

Copiar `.env.example` a `.env` y completar los valores.

---

## Correr localmente

```bash
# 1. Crear entorno virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar variables de entorno
cp .env.example .env
# Editar .env con los valores correctos

# 4. Correr el servicio
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# 5. Verificar health
curl http://localhost:8000/health
```

### Modo sin Kafka (desarrollo)

Configurar `KAFKA_ENABLED=false` en `.env`. El worker consultará directamente la tabla `sync_events` buscando registros con `status='queued'` cada 5 segundos.

---

## Tests

```bash
# Correr todos los tests
pytest tests/ -v

# Con cobertura
pytest tests/ -v --cov=app --cov-report=term-missing

# Test específico
pytest tests/test_circuit_breaker.py -v
pytest tests/test_availability_strategy.py -v
```

### Suite de tests

| Archivo | Tests | Qué valida |
|---|---|---|
| `test_availability_strategy.py` | 4 | UPSERT, upsert existente, batch performance (<2s para 100 registros), detección de conflictos |
| `test_rate_strategy.py` | 2 | Inserción de tarifa nueva, actualización de tarifa existente |
| `test_conflict_resolver.py` | 3 | Overbooking detectado, actualización normal sin conflicto, notificación enviada |
| `test_command_handler.py` | 3 | Routing a estrategia correcta, error para event_type desconocido, actualización de status |
| `test_retry_handler.py` | 3 | Incremento de retry_count, max retries excedido, delays de backoff |
| `test_circuit_breaker.py` | 5 | CLOSED permite llamadas, apertura tras N fallos, OPEN rechaza, HALF_OPEN tras timeout, cierre tras éxito |

**Meta de cobertura: ≥ 70%**

Los tests usan SQLite in-memory (no requieren PostgreSQL ni Kafka).

---

## Deploy a Cloud Run

```bash
./deploy.sh
```

El script construye la imagen Docker con Cloud Build y la despliega en Cloud Run con:
- `--min-instances=1`: siempre hay al menos una instancia consumiendo de Kafka
- `--no-cpu-throttling`: el consumer loop no se pausa cuando no hay requests HTTP
- VPC connector para acceder a Kafka y PostgreSQL en la red privada

---

## Colección Postman

Importar los siguientes archivos en Postman:

1. **Colección:** `pms-sync-worker.postman_collection.json`
2. **Entorno:** `pms-sync-worker.postman_environment.json`

### Estructura de la colección

| Carpeta | Descripción |
|---|---|
| `Health & Status` | `GET /health` — verificar que el servicio está activo |
| `Kafka Messages — availability_update` | Ejemplos de mensajes para actualización de disponibilidad |
| `Kafka Messages — rate_update` | Ejemplos de mensajes para actualización de tarifas |
| `Kafka Messages — property_sync` | Ejemplos de sincronización completa de propiedad |
| `Errores y casos borde` | Mensajes que generan errores controlados |

> **Nota:** Las carpetas "Kafka Messages" documentan el contrato de los mensajes que circulan por Kafka. El campo `/_simulate/kafka` en la URL es un placeholder de documentación — el worker no expone ese endpoint. Para probar la lógica de negocio directamente, usar los tests unitarios.

### Variables de entorno Postman

| Variable | Valor por defecto | Cambiar para... |
|---|---|---|
| `base_url` | `http://localhost:8000` | Dev local |
| `hotel_id` | UUID de prueba | ID de hotel real en BD de test |
| `room_id` | UUID de prueba | ID de room real en BD de test |
| `event_id` | UUID de prueba | Cualquier UUID v4 |

---

## Tablas en PostgreSQL

Este worker comparte la BD `travelhub` con `pms-integration-services`.

| Tabla | Operaciones | Descripción |
|---|---|---|
| `availability` | UPSERT | Disponibilidad por habitación y fecha |
| `tariffs` | UPSERT | Tarifas por habitación y rango de fechas |
| `sync_events` | UPDATE status | Trazabilidad de comandos procesados |
| `pms_properties` | UPDATE last_sync_at | Timestamp de última sincronización exitosa |
| `hotels` | UPDATE (property_sync) | Info del hotel |
| `rooms` | INSERT/UPDATE (property_sync) | Habitaciones del hotel |
