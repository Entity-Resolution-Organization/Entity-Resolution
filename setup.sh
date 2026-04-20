#!/bin/bash
set -e

echo "=== Entity Resolution Setup ==="

# Pull latest .env files from Secret Manager
echo "Pulling secrets..."
gcloud secrets versions access latest --secret=er-env-model > Model-Pipeline/.env
gcloud secrets versions access latest --secret=er-env-monitoring > Monitoring-Pipeline/.env
gcloud secrets versions access latest --secret=er-env-data > Data-Pipeline/.env

# Configure Docker auth for Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

# Get VM external IP and inject MLflow URI
EXTERNAL_IP=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/externalIp" -H "Metadata-Flavor: Google")
MLFLOW_URI="http://$EXTERNAL_IP:5000"
echo "MLflow URI: $MLFLOW_URI"

echo "MLFLOW_TRACKING_URI=$MLFLOW_URI" >> Model-Pipeline/.env
echo "MLFLOW_TRACKING_URI=$MLFLOW_URI" >> Monitoring-Pipeline/.env

# Write MLflow URI to GCS for Cloud Run
echo "$MLFLOW_URI" | gcloud storage cp - gs://entity-resolution-bucket-1/config/mlflow_uri.txt

# Start Model Pipeline services (MLflow + trainer)
echo "Starting Model Pipeline..."
cd Model-Pipeline
sudo docker compose up -d mlflow
cd ..

# Start Inference Pipeline services
echo "Starting Inference Pipeline..."
cd Inference-Pipeline
sudo docker compose up -d
cd ..

echo "=== Setup Complete ==="
echo "MLflow:   $MLFLOW_URI"
echo "Airflow:  http://$EXTERNAL_IP:8080"
echo "UI:       http://$EXTERNAL_IP:8501"
