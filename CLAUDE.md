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
├── services/                     # availability, tariff, sync_event, conflict_resolver, notification_client
├── resilience/                   # circuit_breaker, retry_handler
├── schemas/sync_command.py       # event_id: str, hotel_id: UUID
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
> **Strategy `availability_update` consume `data.room_id` directamente** (UUID de la tabla `rooms` de TravelHub) — no hay mapeo PMS→TravelHub para este event_type. Las strategies `rate_update` y `property_sync` sí usan `pms_room_id` con `room_mappings` o lookup por `Room.pms_room_id`.
> **Guía de testing end-to-end** (preparar BD, obtener JWT, enviar webhook, ver el worker procesar): ver `../PMS_TESTING_GUIDE.md` en la raíz del monorepo.

## Despliegue actual

| Ambiente | Project | URL | Estado |
|---|---|---|---|
| **DEV** | `gen-lang-client-0930444414` | https://pms-sync-worker-ridyy4wz4q-uc.a.run.app | ✅ Auto-deploy via push a `feature/*` o `develop` |
| **PROD** | `travelhub-prod-492116` | — | ⏸ Pipeline Cloud Deploy creado; deploy con push a `main` (requiere Kafka prod) |

### Branch de trabajo

`feature/ci-cd-setup` — config CI/CD WIF + Cloud Deploy.

## Tests skipped (TODO arreglar)

5 archivos de test tienen `pytestmark = pytest.mark.skip` con razón: el refactor de modelos
SQLAlchemy para alinear con el schema PG real (commit anterior) rompió fixtures viejos.

| Archivo | Razón |
|---|---|
| `test_command_handler.py` | usa `SyncEvent(pms_property_id=...)` que ya no existe |
| `test_sync_event_service.py` | mismo problema |
| `test_property_sync.py` | usa `event_id=uuid.uuid4()` (ahora `str`), también pms_property_id |
| `test_rate_strategy.py` | mismo |
| `test_availability_strategy.py` | mismo |

**TODO**: reescribir con la nueva schema (`event_id: str`, `hotel_id: UUID`, `payload_hash` en vez de `payload`, etc.).

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
