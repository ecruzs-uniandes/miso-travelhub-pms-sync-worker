# CLAUDE CODE — pms-sync-worker (Kafka Consumer / Worker)
> Instrucciones para Claude Code CLI. Ejecutar en orden. No omitir pasos.
> Proyecto: TravelHub — Grupo 9 | Curso: MISW4501 — Uniandes

---

## 1. Resumen del Servicio

**pms-sync-worker** es el worker que consume comandos de sincronización desde la cola Kafka `pms-sync-queue` y los procesa. Su responsabilidad es:

1. Consumir mensajes del topic Kafka `pms-sync-queue`
2. Deserializar el `SyncCommand`
3. Según el `event_type`, ejecutar la lógica de sincronización correspondiente
4. Actualizar la tabla `availability` y/o `tariffs` en PostgreSQL
5. Actualizar el `sync_events.status` a "completed" o "failed"
6. Actualizar `pms_properties.last_sync_at`
7. En caso de conflictos (habitación vendida simultáneamente en dos canales), aplicar lógica de resolución
8. Notificar a `notification-services` si hay conflictos o errores críticos (HTTP call)

**Patrones aplicados:**
- **Command** (GoF): Cada mensaje Kafka es un comando ejecutable con retry y trazabilidad
- **Strategy** (GoF): Diferentes estrategias de procesamiento según `event_type`
- **Circuit Breaker**: Protección ante fallos de BD o servicios downstream
- **Observer**: Notificar cambios de disponibilidad para que otros servicios se enteren

**ASR:** AH004 — Sincronizar 1,200 propiedades en ≤ 2 minutos con reintentos e idempotencia.

---

## 2. Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Framework | Python 3.11 + FastAPI (solo para /health) |
| ORM | SQLAlchemy 2.0 |
| BD | PostgreSQL 15 (Cloud SQL) — misma BD que pms-integration-services |
| Cola | Apache Kafka (confluent-kafka-python) |
| HTTP Client | httpx (para llamar a notification-services) |
| Tests | pytest + pytest-asyncio |
| Container | Docker → Cloud Run |
| Puerto | 8000 (solo para health check) |

---

## 3. Estructura de Archivos

```
pms-sync-worker/
├── app/
│   ├── __init__.py
│   ├── main.py                          # FastAPI app (solo /health) + worker startup
│   ├── config.py                        # Settings con Pydantic BaseSettings
│   ├── database.py                      # Engine + SessionLocal + Base (compartido)
│   │
│   ├── models/                          # SQLAlchemy models (MISMOS que pms-integration-services)
│   │   ├── __init__.py
│   │   ├── hotel.py
│   │   ├── room.py
│   │   ├── availability.py
│   │   ├── tariff.py
│   │   ├── pms_property.py
│   │   └── sync_event.py
│   │
│   ├── schemas/
│   │   ├── __init__.py
│   │   └── sync_command.py              # SyncCommand (misma definición que en el API)
│   │
│   ├── worker/                          # Core del worker
│   │   ├── __init__.py
│   │   ├── kafka_consumer.py            # Consumer loop: poll → process → commit
│   │   ├── command_handler.py           # Router: según event_type → strategy
│   │   └── worker_runner.py             # Background task que corre el consumer loop
│   │
│   ├── strategies/                      # Strategy pattern: procesadores por event_type
│   │   ├── __init__.py
│   │   ├── base_strategy.py             # ABC con método execute()
│   │   ├── availability_update.py       # Procesa actualizaciones de disponibilidad
│   │   ├── rate_update.py               # Procesa actualizaciones de tarifas
│   │   └── property_sync.py             # Sincronización completa de propiedad
│   │
│   ├── services/
│   │   ├── __init__.py
│   │   ├── availability_service.py      # UPSERT disponibilidad (batch)
│   │   ├── tariff_service.py            # UPSERT tarifas
│   │   ├── conflict_resolver.py         # Lógica de resolución de conflictos
│   │   ├── notification_client.py       # HTTP client para notification-services
│   │   └── sync_event_service.py        # Actualizar status en sync_events
│   │
│   └── resilience/
│       ├── __init__.py
│       ├── circuit_breaker.py           # Implementación simple de Circuit Breaker
│       └── retry_handler.py             # Retry con exponential backoff
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                      # Fixtures: test db, mock kafka consumer
│   ├── test_availability_strategy.py    # Tests procesamiento de disponibilidad
│   ├── test_rate_strategy.py            # Tests procesamiento de tarifas
│   ├── test_conflict_resolver.py        # Tests resolución de conflictos
│   ├── test_command_handler.py          # Tests routing de comandos
│   ├── test_retry_handler.py            # Tests de reintentos
│   └── test_circuit_breaker.py          # Tests circuit breaker
│
├── Dockerfile
├── requirements.txt
├── .env.example
├── deploy.sh
└── README.md
```

---

## 4. Variables de Entorno

```bash
# .env.example
DATABASE_HOST=10.100.0.3
DATABASE_PORT=5432
DATABASE_NAME=travelhub
DATABASE_USER=travelhub_app
DATABASE_PASSWORD=lALk8rAOj1TSltRQzGavZdBCrSu67ZJg

KAFKA_BOOTSTRAP_SERVERS=10.100.0.5:9092
KAFKA_TOPIC_PMS_SYNC=pms-sync-queue
KAFKA_CONSUMER_GROUP=pms-sync-worker-group

NOTIFICATION_SERVICE_URL=https://notification-services-PLACEHOLDER.us-central1.run.app

SERVICE_NAME=pms-sync-worker
SERVICE_PORT=8000

# Retry config
MAX_RETRIES=3
RETRY_BACKOFF_BASE=2           # seconds, exponential: 2, 4, 8

# Circuit Breaker config
CB_FAILURE_THRESHOLD=5          # Open after 5 consecutive failures
CB_RECOVERY_TIMEOUT=30          # Try again after 30 seconds
```

---

## 5. Modelos SQLAlchemy

**IMPORTANTE:** Los modelos son exactamente los MISMOS que en `pms-integration-services`. Ambos servicios comparten la misma base de datos `travelhub` y las mismas tablas. Copiar los modelos tal cual del otro servicio.

> En un monorepo real se usaría un paquete compartido. Para este MVP, duplicar los modelos en ambos servicios es aceptable. Mantener sincronizados.

---

## 6. Arquitectura del Worker

### 6.1 main.py — Dual mode: Health API + Worker loop

```python
# El worker necesita:
# 1. Un endpoint /health para que Cloud Run pueda hacer health checks
# 2. Un loop de fondo que consume de Kafka
#
# Implementación con FastAPI lifespan:

from contextlib import asynccontextmanager
import asyncio
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: crear tablas + iniciar worker en background
    create_tables()
    worker_task = asyncio.create_task(run_worker())
    yield
    # Shutdown: cancelar worker
    worker_task.cancel()

app = FastAPI(title="PMS Sync Worker", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "pms-sync-worker"}
```

### 6.2 Kafka Consumer Loop (kafka_consumer.py)

```python
# Flujo del consumer loop:
#
# while True:
#   msg = consumer.poll(timeout=1.0)
#   if msg is None:
#       continue
#   if msg.error():
#       handle_error(msg)
#       continue
#
#   command = deserialize(msg.value())
#
#   try:
#       handler.process(command)
#       consumer.commit(msg)        # Commit DESPUÉS de procesar exitosamente
#   except RetryableError:
#       # Re-publicar con retry_count + 1 si < MAX_RETRIES
#       # Si >= MAX_RETRIES → marcar como "failed"
#       republish_or_fail(command)
#       consumer.commit(msg)        # Commit para no reprocesar
#   except NonRetryableError:
#       mark_as_failed(command)
#       consumer.commit(msg)

# Consumer config:
#   group.id = "pms-sync-worker-group"
#   auto.offset.reset = "earliest"
#   enable.auto.commit = False       # Manual commit después de procesar
#   max.poll.interval.ms = 300000    # 5 min max processing time
```

### 6.3 Command Handler (command_handler.py) — Strategy Router

```python
# Mapea event_type → Strategy
#
# STRATEGY_MAP = {
#     "availability_update": AvailabilityUpdateStrategy(),
#     "rate_update": RateUpdateStrategy(),
#     "property_sync": PropertySyncStrategy(),
# }
#
# def process(command: SyncCommand):
#     strategy = STRATEGY_MAP.get(command.event_type)
#     if not strategy:
#         raise NonRetryableError(f"Unknown event_type: {command.event_type}")
#
#     # Actualizar sync_events.status = "processing"
#     update_sync_event(command.event_id, status="processing")
#
#     try:
#         strategy.execute(command)
#         update_sync_event(command.event_id, status="completed", processed_at=now())
#         update_pms_property_last_sync(command.hotel_id, command.pms_provider)
#     except Exception as e:
#         update_sync_event(command.event_id, status="failed")
#         raise
```

---

## 7. Strategies (Procesamiento por event_type)

### 7.1 AvailabilityUpdateStrategy

```python
# Recibe: SyncCommand con data = { room_mappings, dates[] }
#
# Flujo:
# 1. Mapear room_id del PMS → room_id de TravelHub
#    - Buscar en rooms WHERE hotel_id = command.hotel_id
#    - Si no se encuentra el mapping, crear room automáticamente o loggear warning
#
# 2. Para cada date entry:
#    - UPSERT en availability (INSERT ON CONFLICT DO UPDATE)
#    - SET unidades_disponibles = new_value
#    - SET ultima_actualizacion = now()
#    - SET fuente_actualizacion = "pms_webhook"
#
# 3. Detectar conflictos:
#    - Si unidades_disponibles < unidades_reservadas → CONFLICTO
#    - Llamar conflict_resolver.resolve()
#
# 4. Batch processing: usar executemany / bulk operations para performance
#
# SQL para UPSERT:
# INSERT INTO availability (id, room_id, fecha, unidades_disponibles, unidades_reservadas, ultima_actualizacion, fuente_actualizacion)
# VALUES (gen_random_uuid(), :room_id, :fecha, :disponibles, 0, now(), 'pms_webhook')
# ON CONFLICT (room_id, fecha)
# DO UPDATE SET
#   unidades_disponibles = EXCLUDED.unidades_disponibles,
#   ultima_actualizacion = now(),
#   fuente_actualizacion = 'pms_webhook'
```

### 7.2 RateUpdateStrategy

```python
# Recibe: SyncCommand con data = { rates[] }
#
# Flujo:
# 1. Mapear room_id del PMS → room_id TravelHub
# 2. Para cada rate entry:
#    - UPSERT en tariffs
#    - Manejar overlapping date ranges
# 3. Loggear cambios de tarifa para auditoría
```

### 7.3 PropertySyncStrategy

```python
# Recibe: SyncCommand con data = { property_info, rooms[], availability[] }
#
# Flujo COMPLETO de sincronización de propiedad:
# 1. Actualizar info del hotel si cambió (nombre, dirección, etc.)
# 2. Sincronizar rooms: crear nuevas, desactivar eliminadas
# 3. Sincronizar availability para todas las rooms
# 4. Sincronizar tariffs
# 5. Todo en una transacción
```

---

## 8. Conflict Resolver

```python
# Lógica de resolución de conflictos cuando la disponibilidad
# reportada por el PMS contradice las reservas existentes en TravelHub.
#
# Escenarios:
# 1. PMS dice disponibles=0 pero TravelHub tiene reservas futuras
#    → CONFLICTO CRÍTICO: notificar al hotel_admin + platform_admin
#    → Marcar disponibilidad como "conflicto"
#    → NO cancelar reservas automáticamente
#
# 2. PMS dice disponibles=2 pero TravelHub tiene reservadas=3
#    → OVERBOOKING detectado
#    → Notificar inmediatamente
#    → Estrategia: ofrecer habitación similar (futuro) / escalar a soporte
#
# 3. PMS dice disponibles=5 pero TravelHub dice disponibles=3
#    → Actualización normal (PMS tiene más info, es la fuente de verdad)
#    → Actualizar sin conflicto
#
# Implementar como Strategy pattern si se quiere variar por hotel/proveedor
```

---

## 9. Resilience

### 9.1 Circuit Breaker

```python
# Estados: CLOSED → OPEN → HALF_OPEN
#
# CLOSED: todo normal, cuenta failures consecutivos
# OPEN: después de FAILURE_THRESHOLD failures → rechaza llamadas por RECOVERY_TIMEOUT
# HALF_OPEN: después del timeout, permite 1 llamada. Si éxito → CLOSED. Si falla → OPEN.
#
# Aplicar en:
# - Conexiones a PostgreSQL
# - Llamadas HTTP a notification-services
#
# Implementación simple con clase CircuitBreaker:

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_failure_time = None

    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpenError()

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
```

### 9.2 Retry Handler

```python
# Exponential backoff con jitter:
#
# retry_delay = RETRY_BACKOFF_BASE ** retry_count + random(0, 1)
#
# Si retry_count >= MAX_RETRIES → marcar como "failed" definitivamente
# Si retry_count < MAX_RETRIES → re-publicar en Kafka con retry_count + 1
#
# Usar el mismo topic (pms-sync-queue) para reintentos.
# Alternativa: dead letter topic `pms-sync-dlq` para mensajes que exceden MAX_RETRIES.
```

---

## 10. Notification Client

```python
# HTTP client para notificar a notification-services cuando hay:
# 1. Conflictos de disponibilidad
# 2. Errores críticos de sincronización
# 3. Sincronización completa exitosa (opcional)
#
# Endpoint (futuro): POST /api/v1/notifications/internal
# Payload:
# {
#   "type": "pms_sync_conflict" | "pms_sync_error" | "pms_sync_complete",
#   "hotel_id": "uuid",
#   "details": { ... },
#   "recipients": ["hotel_admin"]
# }
#
# Si notification-services no está disponible → loggear y continuar
# (Circuit Breaker protege esta llamada)
#
# NOTA: Si notification-services aún no está implementado, implementar como
# logger.warning() que simula la notificación. Esto se conectará después.
```

---

## 11. Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

EXPOSE 8000

# El worker corre como uvicorn (health endpoint) + background task (consumer loop)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## 12. requirements.txt

```
fastapi==0.111.0
uvicorn[standard]==0.30.1
sqlalchemy==2.0.30
psycopg2-binary==2.9.9
pydantic==2.7.1
pydantic-settings==2.3.1
confluent-kafka==2.4.0
httpx==0.27.0
```

---

## 13. Deploy a Cloud Run

```bash
#!/bin/bash
# deploy.sh
set -e

SERVICE_NAME="pms-sync-worker"
REGION="us-central1"
PROJECT="gen-lang-client-0930444414"

echo ">>> Building Docker image..."
gcloud builds submit --tag gcr.io/$PROJECT/$SERVICE_NAME --project $PROJECT

echo ">>> Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image gcr.io/$PROJECT/$SERVICE_NAME \
  --vpc-connector=travelhub-connector \
  --set-env-vars "DATABASE_HOST=10.100.0.3,DATABASE_PORT=5432,DATABASE_NAME=travelhub,DATABASE_USER=travelhub_app,DATABASE_PASSWORD=lALk8rAOj1TSltRQzGavZdBCrSu67ZJg,KAFKA_BOOTSTRAP_SERVERS=10.100.0.5:9092,KAFKA_TOPIC_PMS_SYNC=pms-sync-queue,KAFKA_CONSUMER_GROUP=pms-sync-worker-group,NOTIFICATION_SERVICE_URL=https://notification-services-PLACEHOLDER.us-central1.run.app,MAX_RETRIES=3,RETRY_BACKOFF_BASE=2,CB_FAILURE_THRESHOLD=5,CB_RECOVERY_TIMEOUT=30" \
  --allow-unauthenticated \
  --port 8000 \
  --region $REGION \
  --project $PROJECT \
  --min-instances=1 \
  --max-instances=3 \
  --no-cpu-throttling

# NOTA: --min-instances=1 para que siempre haya un consumer corriendo
# NOTA: --no-cpu-throttling para que el consumer loop no se pause

echo ">>> Deployed."
```

---

## 14. Tests Requeridos (pytest)

### 14.1 test_availability_strategy.py
- `test_availability_update_creates_records` → Inserta disponibilidad nueva
- `test_availability_update_upserts_existing` → Actualiza disponibilidad existente (mismo room_id + fecha)
- `test_availability_update_batch_performance` → 100 dates en < 2 segundos
- `test_availability_conflict_detected` → Detecta disponibles < reservadas

### 14.2 test_rate_strategy.py
- `test_rate_update_creates_tariff` → Inserta tarifa nueva
- `test_rate_update_overwrites_existing` → Actualiza tarifa existente

### 14.3 test_conflict_resolver.py
- `test_overbooking_detected_and_flagged` → disponibles=0 con reservas=2 → conflicto
- `test_normal_update_no_conflict` → disponibles > reservadas → sin conflicto
- `test_conflict_notification_sent` → Mock notification client, verificar llamada

### 14.4 test_command_handler.py
- `test_routes_to_correct_strategy` → availability_update → AvailabilityUpdateStrategy
- `test_unknown_event_type_raises_error` → "unknown_type" → NonRetryableError
- `test_updates_sync_event_status` → status cambia de received → processing → completed

### 14.5 test_retry_handler.py
- `test_retry_increments_count` → retry_count 0 → 1
- `test_max_retries_exceeded_marks_failed` → retry_count=3 → status "failed"
- `test_exponential_backoff_delay` → retry 0=2s, 1=4s, 2=8s

### 14.6 test_circuit_breaker.py
- `test_closed_allows_calls` → estado normal
- `test_opens_after_threshold_failures` → 5 failures → OPEN
- `test_open_rejects_calls` → CircuitBreakerOpenError
- `test_half_open_after_timeout` → Después de recovery_timeout → permite 1 llamada
- `test_closes_on_success_after_half_open` → éxito en HALF_OPEN → CLOSED

### 14.7 conftest.py
- SQLite in-memory para tests
- Mock Kafka consumer con mensajes predefinidos
- Mock httpx para notification client
- Fixtures: hotel, room, pms_property, availability preexistente

**Meta de cobertura: ≥ 70%**

---

## 15. Notas Importantes

1. **Este worker DEBE correr siempre** (min-instances=1 en Cloud Run). Si no hay instancias activas, los mensajes Kafka se acumulan.
2. **--no-cpu-throttling** es necesario en Cloud Run para que el consumer loop no se detenga cuando no hay requests HTTP.
3. **Manual commit** en Kafka: solo commitear DESPUÉS de procesar exitosamente. Si el worker muere, el mensaje se re-entrega.
4. **Los modelos SQLAlchemy son compartidos con pms-integration-services.** Mantenerlos sincronizados.
5. **La resolución de conflictos no cancela reservas automáticamente.** Solo notifica. La cancelación la decide un humano o booking-services.
6. **Si Kafka no está disponible en dev:** implementar un modo "poll from DB" como fallback. Buscar sync_events con status="queued" y procesarlos directamente. Activar con flag `KAFKA_ENABLED=true/false`.
7. **Dead Letter Queue (DLQ):** Los mensajes que exceden MAX_RETRIES deberían ir a un topic `pms-sync-dlq` para investigación. En MVP, basta con marcarlos como "failed" en sync_events y loggear.
