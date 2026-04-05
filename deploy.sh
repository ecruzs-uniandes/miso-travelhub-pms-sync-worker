#!/bin/bash
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
  --set-env-vars "DATABASE_HOST=10.100.0.3,DATABASE_PORT=5432,DATABASE_NAME=travelhub,DATABASE_USER=travelhub_app,DATABASE_PASSWORD=lALk8rAOj1TSltRQzGavZdBCrSu67ZJg,KAFKA_BOOTSTRAP_SERVERS=10.100.0.5:9092,KAFKA_TOPIC_PMS_SYNC=pms-sync-queue,KAFKA_CONSUMER_GROUP=pms-sync-worker-group,KAFKA_ENABLED=true,NOTIFICATION_SERVICE_URL=https://notification-services-PLACEHOLDER.us-central1.run.app,MAX_RETRIES=3,RETRY_BACKOFF_BASE=2,CB_FAILURE_THRESHOLD=5,CB_RECOVERY_TIMEOUT=30" \
  --allow-unauthenticated \
  --port 8000 \
  --region $REGION \
  --project $PROJECT \
  --min-instances=1 \
  --max-instances=3 \
  --no-cpu-throttling

echo ">>> Deployed successfully."
