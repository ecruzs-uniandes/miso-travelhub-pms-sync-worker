# CLAUDE.md — pms-sync-worker

Worker que consume mensajes Kafka del topic `pms-sync-queue`, ejecuta strategies por
`event_type` (availability_update, rate_update, property_sync) y persiste cambios en la DB
de PMS con reintentos + circuit breaker + DLQ.

## Stack

Python 3.11 · FastAPI 0.111 (solo para `/health`) · SQLAlchemy 2.0.30 (sync + psycopg2) · confluent-kafka · PostgreSQL

## Comandos

```bash
# Local
cd ../travelhub-local && docker compose up -d
pytest -v
black --check app/ tests/ && isort --check-only app/ tests/ && ruff check app/ tests/
docker build -t pms-sync-worker:dev .
./deploy/deploy.sh dev    # o prod
```

## Estructura

```
app/
├── main.py                       # FastAPI lifespan: create_tables + run_worker (asyncio task)
├── config.py                     # pydantic-settings (sslmode=disable en URL)
├── database.py                   # sync engine, SessionLocal, Base
├── worker/
│   ├── kafka_consumer.py         # consume loop + retry + DLQ + commit on success
│   ├── command_handler.py        # router por event_type → strategy
│   └── worker_runner.py          # orquesta consumer task / fallback DB poll
├── strategies/                   # availability_update, rate_update, property_sync
├── services/                     # availability_service (writes disponibilidad), tarifa_service (writes tarifa), sync_event, conflict_resolver, notification_client
├── resilience/                   # circuit_breaker, retry_handler
├── schemas/sync_command.py       # event_id: str, hotel_id: str (con field_validator que coerce UUID→str, ver bug 2026-05-16)
└── models/                       # SQLAlchemy alineados con schema PG real
.github/workflows/ci.yml          # WIF + Cloud Run direct VPC
clouddeploy.yaml                  # canary 10→50→100
skaffold.yaml + k8s/service-prod.yaml
deploy/deploy.sh                  # script manual dev|prod
pms-sync-worker.postman_collection.json    # /health + ejemplos de mensajes Kafka
pms-sync-worker.postman_environment.json   # variables Postman
```

## Superficie HTTP / Kafka

Este servicio NO expone API de negocio: la entrada es Kafka, no HTTP.

| Tipo | Endpoint / Topic | Detalle |
|---|---|---|
| HTTP | `GET /health` | Liveness para Cloud Run. Devuelve `{status: "healthy", service: "pms-sync-worker"}` (200). |
| Kafka in | `pms-sync-queue` | Consume `SyncCommand` (`event_type ∈ {availability_update, rate_update, property_sync}`). `enable.auto.commit=false`. |
| Kafka out (retry) | `pms-sync-queue` | Re-publica con `retry_count++` cuando la strategy lanza excepción retryable. |
| Kafka out (DLQ) | `pms-sync-dlq` | Mensajes con `retry_count > MAX_RETRIES` o `NonRetryableError`. |
| HTTP out | `NOTIFICATION_SERVICE_URL` | POST a notification-services cuando el conflict resolver detecta `critical_zero_availability` u `overbooking`. |

> El esquema completo de `SyncCommand` y los `data` por `event_type` están documentados en `README.md` y en `pms-sync-worker.postman_collection.json` (carpetas `Kafka Messages — *`).
> **Strategy `availability_update`** acepta `data.habitacion_id` (canonical varchar) — o `data.room_id` por compat transicional. Sin mapeo PMS→TravelHub para este event_type.
> **Strategy `rate_update`** requiere `data.room_mappings` (dict `pms_room_id → habitacion.id` varchar) porque la canonical `habitacion` no tiene `pms_room_id`. Si falta el mapping para un rate, se loguea warning y se omite.
> **Strategy `property_sync`** en este sprint hace solo `_sync_hotel_info` (actualiza nombre/direccion/ciudad/pais en `hotel` canonical). El upsert de rooms está deshabilitado con warning porque la canonical `habitacion` tiene 11 cols NOT NULL que el webhook PMS no provee — pendiente contrato con search-service.
> **Guía de testing end-to-end** (preparar BD, obtener JWT, enviar webhook, ver el worker procesar): ver `../PMS_TESTING_GUIDE.md` en la raíz del monorepo.

## Despliegue actual

| Ambiente | Project | URL | Estado |
|---|---|---|---|
| **DEV** | `gen-lang-client-0930444414` | https://pms-sync-worker-ridyy4wz4q-uc.a.run.app | ✅ Auto-deploy via push a `feature/*` o `develop` |
| **PROD** | `travelhub-prod-492116` | https://pms-sync-worker-qhweqfkejq-uc.a.run.app | ✅ Desplegado 2026-05-08 (Cloud Deploy canary). Conecta a Kafka VM PROD `10.20.3.3:9092` |

### Branch de trabajo

`main` — CI/CD pipeline activo (deploy-prod habilitado en commit `e3400df` de 2026-05-08; antes estaba en `if: false # TODO Fase 2`).

## Bugs resueltos

### ✅ 2026-05-16 — Type mismatch UUID vs varchar en queries de `sync_events` y `pms_properties`

Síntoma: cualquier mensaje en `pms-sync-queue` fallaba con `operator does not exist: character varying = uuid` en la query `SELECT ... FROM sync_events WHERE hotel_id == :hotel_id`. Worker entraba en loop de retry → DLQ → en algunos casos el Kafka consumer crasheaba con `InFailedSqlTransaction`.

Causa: el refactor canonical (2026-05-14) pasó `hotel.id` a varchar en la BD, pero `SyncCommand.hotel_id` se quedó como `UUID` en Pydantic. SQLAlchemy enviaba el UUID al WHERE y PG no podía comparar `varchar = uuid`. Adicionalmente las FKs hacia `hotel.id` quedaron en estado inconsistente entre DEV (drift residual UUID en columnas) y PROD (ya migrado a varchar) — runbook completo en `../COMANDOS_UTILES.md` §6.Z.

Fixes (worker):
- **PR #2 (`2cf9b5a`)**: `app/schemas/sync_command.py` — `hotel_id: UUID` → `hotel_id: str`. `app/services/sync_event_service.py` — `update_pms_property_last_sync(hotel_id: UUID)` → `(hotel_id: str)`.
- **PR #3 (`864572f`) hotfix**: Pydantic v2 con `hotel_id: str` rechaza objetos `UUID` (a diferencia de v1). Bug colateral: `worker_runner.py` DB poll lee `pms_property.hotel_id` que viene como `UUID` desde SQLAlchemy → falla validación → loop infinito → test `test_db_poll_processes_queued_event` colgaba el CI hasta timeout. Fix: `@field_validator("hotel_id", mode="before")` que normaliza con `str(v)`.

Validación smoke E2E DEV (`pms-sync-worker-00014-scv`):
```
Processing command: event_id=smoke-final2-..., event_type=availability_update, hotel=d2e3f4a5-...
Command smoke-final2-... completed successfully
sync_event.status = "completed", retry_count = 0
disponibilidad.unidadesDisponibles 5 → 2 → 4 → 3 (consistente)
```

Canary PROD (`prod-3cb97d51-20260517021832`) — SUCCEEDED 2026-05-17.

## Tests (2026-05-14)

`pytest` corre 40 tests pasando contra sqlite in-memory.

**Tests eliminados en el refactor canonical** (estaban en `pytest.mark.skip` desde hace un sprint):
- `test_availability_strategy.py`, `test_rate_strategy.py`, `test_property_sync.py`, `test_conflict_resolver.py`

Tenían fixtures contra modelos legacy (`Room`, `Availability` con cols snake_case, `Tariff.precio_por_noche`). Borrados por desgaste. Pendiente reescribir contra los modelos canonical (`Habitacion`, `Disponibilidad` camelCase, `Tarifa.precioBase`).

## Network setup (gotchas críticos)

1. **Direct VPC egress** + subnet-services. Sin VPC connector.
2. **`?sslmode=disable`** en `DATABASE_URL` (psycopg2 + Cloud SQL private IP)
3. **Tag `data-layer`** requerido en VMs custom (Kafka VM) para que aplique `fw-allow-services-to-data`

## Secrets en GCP (DEV)

| Env var | Secret |
|---|---|
| `DATABASE_*` | `PMS_DATABASE_*` (5 secrets, compartidos con pms-integration) |
| `KAFKA_BOOTSTRAP_SERVERS` | `KAFKA_BOOTSTRAP_SERVERS` |
| `NOTIFICATION_SERVICE_URL` | `NOTIFICATION_SERVICE_URL` (= URL de user-services) |

## CI/CD

| Trigger | Acción |
|---|---|
| Push `feature/*`, `develop` | Tests + Lint + Build + Deploy DEV |
| PR a `main`/`develop` | Tests + Lint + Docker Build |
| Push `main` | Build + Cloud Deploy canary PROD |

WIF resources hardcoded en `ci.yml`:

```yaml
DEV_SA: github-deploy-pms-sync-worker@gen-lang-client-0930444414.iam.gserviceaccount.com
PROD_SA: github-deploy-pms-sync-worker@travelhub-prod-492116.iam.gserviceaccount.com
```

## Patterns clave

### Strategy pattern (por `event_type`)

```python
STRATEGIES = {
  "availability_update": AvailabilityUpdateStrategy(),
  "rate_update": RateUpdateStrategy(),
  "property_sync": PropertySyncStrategy(),
}
```

### Retry + Circuit Breaker

- **Retry**: hasta `MAX_RETRIES=3` veces con backoff exponencial (`RETRY_BACKOFF_BASE=2`).
  Mensaje fallido se re-publica al mismo topic con `retry_count` incrementado.
- **Circuit Breaker**: tras `CB_FAILURE_THRESHOLD=5` errores consecutivos, abre circuito
  por `CB_RECOVERY_TIMEOUT=30s` (half-open después).
- **DLQ**: si `retry_count > MAX_RETRIES`, mensaje va a `pms-sync-dlq` con error.

### Idempotencia

El consumer NO commitea offset hasta que el mensaje se procesó exitosamente
(`enable.auto.commit: false`). Si crashea antes del commit, Kafka redelivery automático.

### Schema mapping crítico

El modelo SQLAlchemy fue alineado con el DDL real de `init-db.sql`:
- `SyncEvent`: `event_id: str` (no UUID), columnas reales (`pms_provider`, `hotel_id`, `payload_hash`, `status`, `retry_count: Integer`)
- NO existen: `pms_property_id`, `payload`, `error_message`, `updated_at` en SyncEvent (tests viejos los usaban)

## NO HACER

- Cambiar `event_id` a UUID (los webhooks PMS lo envían como string libre, ej. `"evt-123-demo"`)
- Bloquear el consumer loop con sync IO largo (usar `asyncio.to_thread` si es necesario)
- Comitear sin haber procesado exitosamente (causa pérdida de mensajes)
- Re-encolar en `pms-sync-queue` desde una strategy (eso lo hace el `kafka_consumer` con retry_handler)
