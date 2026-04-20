# Entity Resolution — Deployment Guide

This document covers the full deployment process for the Dynamic Entity Resolution system on Google Cloud Platform (GCP), including infrastructure setup via Terraform, service deployment, monitoring, and CI/CD.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Environment Setup](#environment-setup)
4. [Secrets Configuration](#secrets-configuration)
5. [Infrastructure Provisioning (Terraform)](#infrastructure-provisioning-terraform)
6. [Running the Deployment Script](#running-the-deployment-script)
7. [Deploying the Inference API (Cloud Run)](#deploying-the-inference-api-cloud-run)
8. [Running the ML Pipelines](#running-the-ml-pipelines)
9. [Model Monitoring with Grafana](#model-monitoring-with-grafana)
10. [CI/CD Pipeline (GitHub Actions)](#cicd-pipeline-github-actions)
11. [Verifying the Deployment](#verifying-the-deployment)
12. [Re-running After VM Restart](#re-running-after-vm-restart)
13. [Teardown](#teardown)
14. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

The deployment runs entirely on **Google Cloud Platform (GCP)**:

| Component | GCP Service | Purpose |
|---|---|---|
| MLflow Tracking | Compute Engine VM (`airflow-vm`) | Experiment tracking and model registry |
| Inference API + UI | Compute Engine VM (`airflow-vm`) | FastAPI backend + frontend on port 8000 |
| Grafana Monitoring | Compute Engine VM (`airflow-vm`) | Model performance and drift dashboards |
| Model Training | Vertex AI Pipelines | DeBERTa + GNN training |
| Data Storage | GCS (`entity-resolution-bucket-1`) | Artifacts, configs, raw/processed data |
| Prediction Logs | BigQuery (`entity_resolution` dataset) | Inference logging and monitoring |
| Container Registry | Artifact Registry (`ml-models`) | Docker image storage |
| Secrets | Secret Manager | Secure `.env` file storage |

---

## Prerequisites

### Required Tools

Install the following before starting:

- [gcloud CLI](https://cloud.google.com/sdk/docs/install)
- [Terraform](https://developer.hashicorp.com/terraform/install) (>= v1.3)
- [Docker](https://docs.docker.com/get-docker/)
- `git`

### Authentication

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project entity-resolution-487121

gcloud services enable \
  compute.googleapis.com \
  aiplatform.googleapis.com \
  bigquery.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  run.googleapis.com \
  --project=entity-resolution-487121
```

---

## Environment Setup

### Step 1 — Clone the Repository

```bash
git clone https://github.com/Entity-Resolution-Organization/Entity-Resolution.git
cd Entity-Resolution
git checkout deployment
```

### Step 2 — Create `.env` Files Locally

**`Model-Pipeline/.env`**
```
GCS_ARTIFACT_ROOT=gs://entity-resolution-bucket-1/mlflow-artifacts
GCP_PROJECT_ID=entity-resolution-487121
GCP_REGION=us-central1
MODEL_VERSION=v1
GCS_BUCKET=entity-resolution-bucket-1
```

**`Monitoring-Pipeline/.env`**
```
GCP_PROJECT_ID=entity-resolution-487121
GCP_REGION=us-central1
BQ_DATASET=entity_resolution
BQ_TABLE=prediction_logs
ENABLE_PREDICTION_LOG=true
```

**`Data-Pipeline/.env`**
```
GCP_PROJECT_ID=entity-resolution-487121
GCP_REGION=us-central1
GCS_BUCKET=entity-resolution-bucket-1
```

> ⚠️ Never commit `.env` files — they are listed in `.gitignore`.

---

## Secrets Configuration

### Step 3 — Upload `.env` Files to Secret Manager

```bash
gcloud secrets create er-env-model \
  --data-file=Model-Pipeline/.env \
  --project=entity-resolution-487121

gcloud secrets create er-env-monitoring \
  --data-file=Monitoring-Pipeline/.env \
  --project=entity-resolution-487121

gcloud secrets create er-env-data \
  --data-file=Data-Pipeline/.env \
  --project=entity-resolution-487121
```

To update an existing secret:
```bash
gcloud secrets versions add er-env-model --data-file=Model-Pipeline/.env
```

---

## Infrastructure Provisioning (Terraform)

### Step 4 — Run Terraform

```bash
cd terraform/
terraform init
terraform plan
terraform apply    # type 'yes' when prompted
```

This creates:
- `airflow-vm` (Compute Engine) — runs MLflow, Inference API, Grafana
- `entity-resolution-bucket-1` + staging bucket (GCS)
- `entity_resolution` dataset (BigQuery)
- `ml-models` repository (Artifact Registry)
- `airflow-sa` and `vertex-trainer` service accounts with IAM roles
- Secret Manager secrets
- Firewall rules for ports 22, 5000, 8000, 8080, 3000, 8501

Note the outputs:
```bash
terraform output vm_external_ip   # e.g. 34.72.173.207
```

---

## Running the Deployment Script

### Step 5 — SSH into the VM and Run setup.sh

```bash
gcloud compute ssh airflow-vm \
  --zone=us-central1-a \
  --project=entity-resolution-487121
```

Once inside:
```bash
cd /home/ubuntu/Entity-Resolution
git pull origin deployment
bash setup.sh
```

`setup.sh` does the following automatically:
1. Pulls `.env` files from Secret Manager
2. Configures Docker auth for Artifact Registry
3. Injects `MLFLOW_TRACKING_URI` using the VM's external IP
4. Writes the MLflow URI to GCS for Cloud Run
5. Starts MLflow (port 5000) via Docker Compose
6. Starts the Inference API (port 8000) via Docker

Verify containers are running:
```bash
sudo docker ps
```

---

## Deploying the Inference API (Cloud Run)

To deploy a new version of the Inference API to Cloud Run:

```bash
# Build and push image
docker build \
  -t us-central1-docker.pkg.dev/entity-resolution-487121/ml-models/er-inference-api:latest \
  ./Inference-Pipeline

docker push \
  us-central1-docker.pkg.dev/entity-resolution-487121/ml-models/er-inference-api:latest

# Deploy
gcloud run deploy er-inference-api \
  --image=us-central1-docker.pkg.dev/entity-resolution-487121/ml-models/er-inference-api:latest \
  --region=us-central1 \
  --project=entity-resolution-487121 \
  --set-env-vars="GCP_PROJECT_ID=entity-resolution-487121,GCP_REGION=us-central1" \
  --service-account=vertex-trainer@entity-resolution-487121.iam.gserviceaccount.com \
  --allow-unauthenticated
```

---

## Running the ML Pipelines

### DeBERTa Training Pipeline (Stage 1)

```bash
cd Model-Pipeline
sudo docker-compose run --rm trainer python pipeline.py --run
```

On completion, endpoint info is written to:
```
gs://entity-resolution-bucket-1/pipeline-results/endpoint_info.json
```

### GNN Pipeline (Stage 2)

```bash
sudo docker-compose run --rm trainer python scripts/gnn/build_graph.py

sudo docker-compose run --rm trainer bash -c "
  JOB_SUFFIX=train ENTITY_TYPE=person python scripts/gnn/write_clusters.py"

sudo docker-compose run --rm trainer bash -c "
  JOB_SUFFIX=train ENTITY_TYPE=person python scripts/gnn/score_network.py"
```

---

## Model Monitoring with Grafana

Grafana runs on the VM at port 3000 and is connected to BigQuery for real-time model monitoring.

### Starting Grafana

```bash
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
```

### Accessing the Dashboard

Open `http://<VM_EXTERNAL_IP>:3000` in your browser.
- Default login: `admin` / `admin123`

The dashboard includes:
- **Data Drift Over Time** — drift scores per feature
- **Model Performance Over Time** — F1, precision, recall trends
- **Prediction Confidence Over Time** — probability and latency
- **Current Model Metrics** — latest F1, precision, recall values

### Setting Up BigQuery Data Source (First Time Only)

1. Go to **Connections → Data sources → Add new data source**
2. Search for **BigQuery** and select it
3. Under Authentication select **Google JWT File**
4. Upload the service account key JSON file
5. Set **Processing Location** to `US`
6. Click **Save & Test**

### Importing the Dashboard (First Time Only)

1. Go to **Dashboards → New → Import**
2. Upload `Monitoring-Pipeline/grafana/er-monitoring-dashboard.json`
3. Select the BigQuery data source when prompted

### Running the Monitoring Pipeline

```bash
cd Monitoring-Pipeline
python monitoring_pipeline.py --run
```

This evaluates the model, detects data drift, logs metrics to BigQuery, and sends Slack alerts if retraining is needed.

---

## CI/CD Pipeline (GitHub Actions)

The `.github/workflows/deploy.yml` workflow automatically triggers on every push to `main` or `dev`. It:

1. Authenticates to GCP using the `GCP_SA_KEY` secret
2. Builds and pushes the Inference API Docker image to Artifact Registry
3. Deploys the updated image to Cloud Run
4. SSHs into the VM and reruns `setup.sh` to pull latest code and restart services

Required GitHub secrets (already configured in the repo):
- `GCP_SA_KEY` — service account key JSON
- `GCP_PROJECT_ID` — `entity-resolution-487121`
- `GCP_REGION` — `us-central1`
- `GCP_BUCKET_NAME` — `entity-resolution-bucket-1`
- `GCP_SA_EMAIL` — service account email
- `MLFLOW_TRACKING_URI` — MLflow tracking URL

---

## Verifying the Deployment

Once everything is running, verify each service:

| Service | URL |
|---|---|
| MLflow | `http://<VM_EXTERNAL_IP>:5000` |
| Inference API + UI | `http://<VM_EXTERNAL_IP>:8000` |
| Grafana | `http://<VM_EXTERNAL_IP>:3000` |
| Vertex AI Pipelines | https://console.cloud.google.com/vertex-ai/pipelines?project=entity-resolution-487121 |
| BigQuery | https://console.cloud.google.com/bigquery?project=entity-resolution-487121 |

Test the inference endpoint:
```bash
curl -X POST http://<VM_EXTERNAL_IP>:8000/api/match \
  -H "Content-Type: application/json" \
  -d '{"name1": "John Doe", "name2": "J. Doe"}'
```

---

## Re-running After VM Restart

> VM restarts change the external IP — MLflow URI becomes stale.

```bash
gcloud compute ssh airflow-vm --zone=us-central1-a --project=entity-resolution-487121
cd /home/ubuntu/Entity-Resolution
bash setup.sh
```

---

## Teardown

> ⚠️ This permanently deletes all GCS data and BigQuery tables. Back up first.

```bash
# Stop services on VM
sudo docker stop $(sudo docker ps -q)

# Destroy all GCP resources
cd terraform/
terraform destroy
```

---

## Troubleshooting

**Containers not starting:**
```bash
sudo docker ps
sudo docker logs <container_name> --tail=30
```

**MLflow not reachable:**
```bash
bash setup.sh   # refreshes URI and restarts services
```

**Grafana can't connect to BigQuery:**
```bash
# Re-upload the service account key in Grafana data source settings
```

**Inference API unhealthy:**
```bash
sudo docker logs inference-api --tail=30
```

---

## Quick Reference

```bash
# SSH into VM
gcloud compute ssh airflow-vm --zone=us-central1-a --project=entity-resolution-487121

# Restart all services
bash setup.sh

# View running containers
sudo docker ps

# View logs
sudo docker logs inference-api --tail=30
sudo docker logs model-pipeline-mlflow-1 --tail=30

# Update a secret
gcloud secrets versions add er-env-model --data-file=Model-Pipeline/.env

# Run monitoring pipeline manually
cd Monitoring-Pipeline && python monitoring_pipeline.py --run
```