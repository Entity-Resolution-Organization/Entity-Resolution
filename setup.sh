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

# Use hardcoded external IP (update this if VM IP changes)
EXTERNAL_IP="34.72.173.207"
MLFLOW_URI="http://$EXTERNAL_IP:5000"
echo "MLflow URI: $MLFLOW_URI"

echo "MLFLOW_TRACKING_URI=$MLFLOW_URI" >> Model-Pipeline/.env
echo "MLFLOW_TRACKING_URI=$MLFLOW_URI" >> Monitoring-Pipeline/.env

# Create Inference Pipeline .env
cat > Inference-Pipeline/.env << ENVEOF
GCP_PROJECT_ID=entity-resolution-487121
GCP_BUCKET_NAME=entity-resolution-bucket-1
GCP_REGION=us-central1
MLFLOW_TRACKING_URI=$MLFLOW_URI
USE_MOCK_CLIENT=true
GOOGLE_APPLICATION_CREDENTIALS=
ENVEOF

# Write MLflow URI to GCS
echo "$MLFLOW_URI" | gcloud storage cp - gs://entity-resolution-bucket-1/config/mlflow_uri.txt

# Start Model Pipeline services (MLflow)
echo "Starting Model Pipeline..."
cd Model-Pipeline
sudo docker-compose up -d mlflow
cd ..

# Start Inference Pipeline services
echo "Starting Inference Pipeline..."
cd Inference-Pipeline
sudo docker-compose up -d inference-api
cd ..

echo "=== Setup Complete ==="
echo "MLflow: $MLFLOW_URI"
echo "UI:     http://$EXTERNAL_IP:8501"
