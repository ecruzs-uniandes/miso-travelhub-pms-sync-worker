#!/usr/bin/env bash
set -euo pipefail

# --------------------------------------------------
# Deploy pms-sync-worker a GCP Cloud Run (dev o prod)
# Uso:
#   ./deploy/deploy.sh dev
#   ./deploy/deploy.sh prod
# --------------------------------------------------

ENVIRONMENT="${1:-}"

if [[ "$ENVIRONMENT" != "dev" && "$ENVIRONMENT" != "prod" ]]; then
    echo "ERROR: primer argumento debe ser 'dev' o 'prod'" >&2
    exit 1
fi

if [ "$ENVIRONMENT" = "dev" ]; then
    PROJECT_ID="gen-lang-client-0930444414"
    VPC_CONNECTOR="travelhub-connector"
else
    PROJECT_ID="travelhub-prod-492116"
    VPC_CONNECTOR="prod-travelhub-connector"
fi

REGION="${GCP_REGION:-us-central1}"
SERVICE_NAME="pms-sync-worker"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${SERVICE_NAME}/${SERVICE_NAME}"
TAG="${DEPLOY_TAG:-latest}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()   { echo -e "${GREEN}[deploy-${ENVIRONMENT}]${NC} $1"; }
warn()  { echo -e "${YELLOW}[deploy-${ENVIRONMENT}]${NC} $1"; }
error() { echo -e "${RED}[deploy-${ENVIRONMENT}]${NC} $1" >&2; exit 1; }

command -v gcloud >/dev/null 2>&1 || error "gcloud no encontrado."
command -v docker >/dev/null 2>&1 || error "docker no encontrado."

log "Autenticando Docker con Artifact Registry ${REGION}..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

log "Build: ${IMAGE}:${TAG}"
docker build -t "${IMAGE}:${TAG}" .

log "Push a Artifact Registry..."
docker push "${IMAGE}:${TAG}"

log "Deploy a Cloud Run (${REGION}) proyecto ${PROJECT_ID}..."
gcloud run deploy "$SERVICE_NAME" \
    --project="$PROJECT_ID" \
    --image="${IMAGE}:${TAG}" \
    --region="$REGION" --platform=managed \
    --port=8000 --allow-unauthenticated \
    --vpc-connector="$VPC_CONNECTOR" \
    --memory=512Mi --cpu=1 \
    --min-instances=1 --max-instances=5 \
    --set-env-vars="SERVICE_NAME=${SERVICE_NAME},SERVICE_PORT=8000,KAFKA_ENABLED=true,KAFKA_TOPIC_PMS_SYNC=pms-sync-queue,KAFKA_CONSUMER_GROUP=pms-sync-worker-group,MAX_RETRIES=3,RETRY_BACKOFF_BASE=2,CB_FAILURE_THRESHOLD=5,CB_RECOVERY_TIMEOUT=30" \
    --set-secrets="DATABASE_HOST=PMS_DATABASE_HOST:latest,DATABASE_PORT=PMS_DATABASE_PORT:latest,DATABASE_NAME=PMS_DATABASE_NAME:latest,DATABASE_USER=PMS_DATABASE_USER:latest,DATABASE_PASSWORD=PMS_DATABASE_PASSWORD:latest,KAFKA_BOOTSTRAP_SERVERS=KAFKA_BOOTSTRAP_SERVERS:latest,NOTIFICATION_SERVICE_URL=NOTIFICATION_SERVICE_URL:latest" \
    --quiet

SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" --project="$PROJECT_ID" --region="$REGION" --format="value(status.url)")
log "Deploy exitoso: ${SERVICE_URL}"
