# pms-sync-worker

Worker de sincronización PMS para **TravelHub** — MISW4501 Grupo 9 — Uniandes

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
   │     ├── AvailabilityUpdateStrategy → UPSERT disponibilidad (canonical)
   │     ├── RateUpdateStrategy         → UPSERT tarifa (canonical)
   │     └── PropertySyncStrategy       → Sync hotel canonical (rooms upsert disabled, pendiente contrato con search-service)
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

Todos los mensajes en el topic `pms-sync-queue` siguen este esquema base (publicados por `pms-integration-services` cuando recibe un webhook):

```json
{
  "command_id": "uuid-v4-generado-por-integration",
  "event_id": "evt-2026-001",
  "event_type": "availability_update | rate_update | property_sync",
  "hotel_id": "uuid-v4",
  "pms_provider": "hotelbeds | sabre | opera | ...",
  "pms_property_id": "string",
  "timestamp": "2026-04-25T10:00:00Z",
  "retry_count": 0,
  "data": { ... },
  "correlation_id": null,
  "created_at": "2026-04-25T10:00:00.123Z"
}
```

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `command_id` | UUID | No | Generado por `SyncCommand.create()` en pms-integration. |
| `event_id` | string | Sí | Identificador único del evento (string libre, **no UUID**). Clave de idempotencia. |
| `event_type` | string | Sí | Tipo: `availability_update`, `rate_update`, `property_sync`. Otros → `NonRetryableError`. |
| `hotel_id` | UUID | Sí | ID del hotel en TravelHub. |
| `pms_provider` | string | Sí | Nombre del proveedor PMS (`hotelbeds`, `sabre`, ...). |
| `pms_property_id` | string | No | ID de la propiedad en el PMS externo. |
| `timestamp` | datetime | No | Timestamp original del evento PMS. |
| `retry_count` | int | No (default 0) | Lo incrementa el consumer al republicar tras un fallo retryable. |
| `data` | object | Sí | Payload específico por `event_type` (ver secciones abajo). |
| `correlation_id` | string | No | Trazabilidad opcional. |
| `created_at` | datetime | No | Cuándo se construyó el comando. |

> **Producer key** = `hotel_id` → garantiza orden por hotel dentro de la misma partición.

---

### `availability_update`

Actualiza la disponibilidad de habitaciones para fechas específicas en la tabla canonical `disponibilidad`.

**Payload `data`:**
```json
{
  "habitacion_id": "b1000000-0000-0000-0000-000000000002",
  "dates": [
    {
      "date": "2025-07-01",
      "available_units": 5,
      "unidades_reservadas": 0
    }
  ]
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `habitacion_id` | varchar | ID canonical de la habitación. **Debe existir en `habitacion.id`**. Acepta legacy `room_id` por compat transicional. |
| `dates[].date` (o `fecha`) | string (`YYYY-MM-DD`) | Fecha de disponibilidad. |
| `dates[].available_units` (o `unidadesDisponibles`/`unidades_disponibles`) | int | Unidades disponibles reportadas por el PMS. |
| `dates[].unidades_reservadas` (o `unidadesReservadas`) | int | Reservas actuales (default 0). |

> El strategy acepta claves en inglés (`date`, `available_units`), snake_case español (`fecha`, `unidades_disponibles`) o camelCase canonical (`unidadesDisponibles`). Prioridad: inglesa → snake_case → camelCase.

**SQL ejecutado (UPSERT en `disponibilidad` por `(habitacionId, fecha)`):**
```sql
-- Si existe: actualiza unidadesDisponibles + ultimaActualizacion
-- Si no:    inserta con unidadesReservadas=0, fuenteActualizacion='pms_webhook'
```

Después del upsert el strategy ejecuta `get_conflicts(habitacion_id, fechas)`: si para alguna fecha
`unidadesDisponibles < unidadesReservadas`, el `ConflictResolver` clasifica el conflicto
(`critical_zero_availability` u `overbooking`) y notifica via `NotificationClient`.

**Ejemplo completo (mensaje en `pms-sync-queue`):**
```json
{
  "command_id": "9f1e2d3c-4b5a-6789-abcd-ef0123456789",
  "event_id": "evt-2026-001",
  "event_type": "availability_update",
  "hotel_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "pms_provider": "hotelbeds",
  "pms_property_id": "HB-MDE-001",
  "timestamp": "2026-04-25T10:00:00Z",
  "retry_count": 0,
  "data": {
    "habitacion_id": "b1000000-0000-0000-0000-000000000002",
    "dates": [
      {"date": "2026-07-01", "available_units": 5},
      {"date": "2026-07-02", "available_units": 4},
      {"date": "2026-07-03", "available_units": 3}
    ]
  }
}
```

---

### `rate_update`

Actualiza tarifas (precio por noche) para rangos de fechas en la tabla canonical `tarifa`.

**Payload `data`:**
```json
{
  "room_mappings": {
    "PMS-ROOM-001": "b1000000-0000-0000-0000-000000000001"
  },
  "rates": [
    {
      "pms_room_id": "PMS-ROOM-001",
      "fechaInicio": "2025-07-01T00:00:00+00:00",
      "fechaFin": "2025-07-31T23:59:59+00:00",
      "precioBase": 120.00,
      "moneda": "USD",
      "descuento": 0.0
    }
  ]
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `room_mappings` | dict | Mapping `pms_room_id → habitacion.id` (varchar canonical). **Obligatorio** — la canonical `habitacion` no tiene `pms_room_id`. Sin mapping para un rate, se omite con warning. |
| `rates[].pms_room_id` | string | ID de la habitación en el PMS — se resuelve a `habitacionId` via `room_mappings`. |
| `rates[].fechaInicio` (o `fecha_inicio`) | string ISO 8601 | Inicio del período. timestamptz. |
| `rates[].fechaFin` (o `fecha_fin`) | string ISO 8601 | Fin del período. timestamptz. |
| `rates[].precioBase` (o `precio_base`/`precio_por_noche`) | float | Precio base por noche. |
| `rates[].moneda` | string | Código ISO 4217 (`USD`, `COP`, `EUR`). |
| `rates[].descuento` | float | Rango [0, 1]. Default 0.0. |

**Comportamiento UPSERT en `tarifa`:**
- Si existe `(habitacionId, fechaInicio, fechaFin)` → actualiza `precioBase`, `moneda`, `descuento`.
- Si no existe → inserta nuevo registro.

**Ejemplo completo:**
```json
{
  "event_id": "b2c3d4e5-0000-0000-0000-000000000001",
  "event_type": "rate_update",
  "hotel_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "pms_provider": "sabre",
  "pms_property_id": "SABRE-001",
  "retry_count": 0,
  "data": {
    "room_mappings": {"PMS-ROOM-001": "b1000000-0000-0000-0000-000000000002"},
    "rates": [
      {
        "pms_room_id": "PMS-ROOM-001",
        "fechaInicio": "2025-12-20T00:00:00+00:00",
        "fechaFin": "2026-01-05T23:59:59+00:00",
        "precioBase": 280.00,
        "moneda": "USD",
        "descuento": 0.0
      }
    ]
  }
}
```

---

### `property_sync`

Sincronización completa de una propiedad. Solo `property_info` se persiste actualmente — `rooms`, `availability`, `tariffs` están **deshabilitados** en este sprint.

**Payload `data` (solo `property_info` se procesa hoy):**
```json
{
  "property_info": {
    "nombre": "Hotel Boutique Candelaria",
    "direccion": "Calle 12 #4-72",
    "ciudad": "Bogotá",
    "pais": "Colombia"
  }
}
```

**Flujo de ejecución (actual):**
1. `property_info` → actualiza cols `nombre`, `direccion`, `ciudad`, `pais` en `hotel` canonical si llegan en el payload.
2. `rooms`, `availability`, `tariffs` si llegan → se loguea warning y se omiten:
   - La canonical `habitacion` tiene 11 cols NOT NULL (`tipo`, `categoria`, `capacidadMaxima`, `descripcion`, `imagenes JSON`, `tipo_habitacion`, `tipo_cama JSON`, `tamano_habitacion`, `amenidades JSON`, etc.) que el webhook PMS no provee.
   - Pendiente: definir contrato con `search-service` (owner de habitacion) o tabla auxiliar de mapping.
   - Para sync de tarifas/disponibilidad usar los event_types `rate_update` y `availability_update` (con `room_mappings`).
3. `COMMIT` — si falla → `ROLLBACK`.

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
