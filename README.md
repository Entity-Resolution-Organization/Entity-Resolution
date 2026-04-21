# Resolv — Entity Resolution

ML system for PERSON entity resolution on Google Cloud Platform. Identifies and links records referring to the same real-world person across multiple data sources using DeBERTa-v3 + LoRA semantic matching, combined with a deterministic field-rule engine and graph-based contextual risk scoring.

**Live demo:** [entity-resolution-756491711716.us-central1.run.app](https://entity-resolution-756491711716.us-central1.run.app)

## Repository Structure

```
Entity-Resolution/
├── Data-Pipeline/              # Airflow data processing pipeline
│   ├── dags/                   # DAG definitions
│   ├── scripts/                # Processing modules
│   ├── tests/                  # Unit tests
│   ├── config/                 # Dataset configuration
│   └── data/                   # Pipeline outputs (DVC tracked)
├── Model-Pipeline/             # DeBERTa-v3 + LoRA training on Vertex AI
│   ├── config/                 # Training + inference config (YAML)
│   ├── scripts/                # Training, evaluation, deployment scripts
│   └── notebooks/              # Exploration notebooks
├── Inference-Pipeline/         # FastAPI backend + React frontend
│   ├── scripts/                # Backend (app.py, build_graph, write_clusters, score_network)
│   ├── frontend/               # React 19 + Vite + Tailwind v4
│   ├── config/                 # Inference configuration
│   ├── Dockerfile              # Multi-stage build for Cloud Run
│   └── requirements.txt
├── Monitoring-Pipeline/        # Vertex AI monitoring pipeline + Grafana
│   ├── scripts/                # ModelMonitor class (drift, metrics, retraining)
│   ├── grafana/                # Dashboard JSON + Grafana provisioning
│   └── monitoring_pipeline.py  # KFP v2 pipeline definition
├── terraform/                  # GCP infrastructure (Terraform)
└── README.md
```

## Pipeline Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────────┐
│  Data Pipeline   │     │  Model Pipeline  │     │     Inference Pipeline       │
│                  │     │                  │     │                             │
│ 3 Datasets       │────▶│ DeBERTa-v3+LoRA │────▶│ Blocking → Scoring → Rules  │
│ 450K accounts    │     │ Vertex AI Train  │     │ → Clustering → Risk Scoring │
│ 240K pairs       │     │ MLflow Tracking  │     │ → Customer 360, KYC, Fraud  │
│ DVC + Airflow    │     │ Quality Gates    │     │ FastAPI + React on Cloud Run│
└─────────────────┘     └─────────────────┘     └─────────────────────────────┘
                                                            │
                                                            ▼
                                              ┌─────────────────────────┐
                                              │  Monitoring Pipeline     │
                                              │                         │
                                              │ Daily drift detection   │
                                              │ Auto-retraining         │
                                              │ Grafana + Slack alerts  │
                                              └─────────────────────────┘
```

## How It Works

### 1. Data Pipeline (Airflow + DVC)
- Ingests 3 PERSON datasets: Pseudopeople (synthetic), NC Voter Registry (real), OFAC SDN List (sanctions)
- 450K accounts → 240K balanced training pairs (50/50 positive/negative)
- Validates with Great Expectations, detects bias, versions with DVC, uploads to GCS
- Quality gate: schema validation, bias detection, split validation

### 2. Model Pipeline (Vertex AI + MLflow)
- **Base model**: DeBERTa-v3-base (220M params, disentangled attention)
- **Fine-tuning**: LoRA adapters (rank=8, alpha=16) — trains only ~0.3% of parameters
- **Training**: 3 epochs, batch size 32, learning rate 2e-5, AdamW
- **Performance**: 98.6% accuracy, 98.4% precision, 98.8% recall, 99.8% AUC on 27K test samples
- **Quality gate**: F1 >= 0.75, precision >= 0.70, recall >= 0.70, AUC >= 0.80

### 3. Inference Pipeline (FastAPI + React)

**Graph construction:**
1. **Blocking** — 7 keys per record (name prefix, surname, DOB year, email user, phone suffix, street number, postal code) reduce N² to ~O(N log N)
2. **DeBERTa scoring** — Vertex AI online endpoint scores candidate pairs
3. **Field rules** — DOB mismatch vetoes (cap at 0.10), email/phone/DOB match boosts (+0.10 to +0.20)
4. **Transitive closure** — connected components clustering at 0.45 threshold
5. **Contextual risk scoring** — cluster_risk (30%), bad_neighbour (40%), shared_field (20%), centrality (10%)

**Product features:**

| Feature | Description |
|---------|-------------|
| Pairwise Match | Compare two records with DeBERTa + field rules, see field-level attribution |
| Batch Unify | Upload CSV → pipeline → download unified CSV with cluster IDs |
| Customer 360 | Search by name → unified golden record from all source systems |
| KYC Screening | OFAC alerts with 1-hop and 2-hop risk paths, investigation graph |
| Fraud Detection | Cross-cluster shared email/phone/company anomalies |
| Cluster Explorer | Visualize transitive closure, direct vs transitive edges |
| Analytics | Pipeline metrics, model performance, bias reports |

### 4. Monitoring Pipeline (Vertex AI + Grafana)
- Runs daily at 2am UTC via Cloud Scheduler
- Evaluates model on holdout data, detects data drift using Kolmogorov-Smirnov test
- **Decision logic**: `RETRAIN_DATA` (drift > 30%) → triggers Airflow + Model Pipeline; `RETRAIN_MODEL` (F1 drops below threshold) → triggers Model Pipeline only; `HEALTHY` → Slack notification only
- Results logged to BigQuery; visualized in Grafana dashboard

## Tech Stack

| Component | Technology |
|-----------|-----------|
| ML Model | DeBERTa-v3-base + LoRA (HuggingFace Transformers + PEFT) |
| Training | Vertex AI Pipelines, T4 GPU |
| Experiment Tracking | MLflow |
| Data Versioning | DVC 3.x + GCS remote |
| Orchestration | Apache Airflow 2.8.1 |
| Model Serving | Vertex AI Online Endpoint (1-3 replicas) |
| Backend | FastAPI, Python 3.10 |
| Frontend | React 19, Vite 8, Tailwind CSS v4, Framer Motion |
| Storage | Google Cloud Storage, BigQuery |
| Deployment | Cloud Run (single container: API + static frontend) |
| Monitoring | Vertex AI Pipelines, Evidently AI, Grafana, BigQuery |
| Notifications | Slack Incoming Webhooks |
| Infrastructure | Terraform, GCP |
| CI/CD | GitHub Actions |

---

## Deployment

This project uses **Cloud Deployment on Google Cloud Platform**. The Inference API runs on Cloud Run, the model is served via Vertex AI Online Endpoint, and all supporting services (Airflow, MLflow, Grafana) run on a GCE VM provisioned by Terraform.

### Prerequisites

- GCP project with the following APIs enabled: Vertex AI, BigQuery, Cloud Storage, Cloud Run, Secret Manager, Artifact Registry, Compute Engine
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.0
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) authenticated (`gcloud auth login`)
- Docker
- Git

### Step 1 — Provision Infrastructure (Terraform)

All GCP resources are defined in `terraform/`:

```bash
cd terraform/
terraform init
terraform apply
```

This provisions:
- `production-vm` (e2-standard-4, Ubuntu 22.04) — runs Airflow, MLflow, Grafana
- GCS buckets (main, staging, data, DVC)
- BigQuery dataset (`entity_resolution`)
- Artifact Registry (`ml-models`)
- Secret Manager secrets (`er-env-model`, `er-env-monitoring`, `er-env-data`)
- Firewall rules (ports 22, 5000, 8000, 8080, 3000, 8501)
- Service accounts (`airflow-sa`, `vertex-trainer`) with IAM roles

### Step 2 — Populate Secrets

Create `.env` files from the templates and upload to Secret Manager:

**`Data-Pipeline/.env`**
```bash
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=changeme
GCP_PROJECT_ID=entity-resolution-487121
GCS_BUCKET=entity-resolution-bucket-1
DVC_GCS_BUCKET=entity-resolution-dvc-bucket
ENVIRONMENT=production
```

**`Model-Pipeline/.env`**
```bash
GCP_PROJECT_ID=entity-resolution-487121
GCS_ARTIFACT_ROOT=gs://entity-resolution-bucket-1/mlflow-artifacts
```

**`Monitoring-Pipeline/.env`**
```bash
GCP_PROJECT_ID=entity-resolution-487121
GCP_REGION=us-central1
GCS_BUCKET=entity-resolution-bucket-1
STAGING_BUCKET=gs://entity-resolution-staging-bucket
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow
```

Upload to Secret Manager:
```bash
gcloud secrets versions add er-env-model      --data-file=Model-Pipeline/.env
gcloud secrets versions add er-env-monitoring --data-file=Monitoring-Pipeline/.env
gcloud secrets versions add er-env-data       --data-file=Data-Pipeline/.env
```

### Step 3 — Start All Services

The VM startup script automatically clones the repo and runs `setup.sh` on first boot. If secrets were not ready in time, SSH in and run manually:

```bash
gcloud compute ssh production-vm --zone=us-central1-a
cd /opt/Entity-Resolution
bash setup.sh
```

`setup.sh` does the following automatically:
1. Pulls `.env` files from Secret Manager
2. Fetches the VM's external IP and injects `MLFLOW_TRACKING_URI` and `AIRFLOW_VM_IP`
3. Configures Docker auth for Artifact Registry
4. Starts Airflow (Data Pipeline), MLflow (Model Pipeline), Inference API + UI, and Grafana

### Step 4 — Verify Deployment

| Service | URL | Expected |
|---------|-----|----------|
| Airflow | `http://<VM_IP>:8080` | DAG list visible |
| MLflow | `http://<VM_IP>:5000` | Experiment tracking UI |
| Inference API | `http://<VM_IP>:8000/health` | `{"status": "ok"}` |
| Inference UI | `http://<VM_IP>:8501` | Streamlit UI |
| Grafana | `http://<VM_IP>:3000` | Dashboard (admin/admin) |
| Cloud Run | [live demo](https://entity-resolution-756491711716.us-central1.run.app) | React frontend |

Check running containers on VM:
```bash
sudo docker ps
```

### Step 5 — Run Data Pipeline

```bash
# Trigger the Airflow DAG (from VM or locally)
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline

# Or via Airflow UI at http://<VM_IP>:8080
```

### Step 6 — Run Model Training

```bash
cd /opt/Entity-Resolution/Model-Pipeline
docker compose run --rm trainer python pipeline.py --compile
docker compose run --rm trainer python pipeline.py --run --mlflow-uri http://<VM_IP>:5000
```

---

## CI/CD

Four workflows run automatically on push to `main` or `dev`:

| Workflow | Triggers on | What it does |
|----------|-------------|--------------|
| `test.yml` | `Data-Pipeline/**` | Lint, unit tests with coverage, validate `datasets.yaml` |
| `train-pipeline.yml` | `Model-Pipeline/**` | Lint, validate config, tests, build trainer Docker image, compile KFP pipeline → upload `pipeline.yaml` to GCS |
| `inference-pipeline.yml` | `Inference-Pipeline/**` | Lint, unit tests, build and push Inference API image to Artifact Registry |
| `deploy.yml` | `main`, `dev` (all pushes) | Build and push Inference API image, deploy to Cloud Run |

**GitHub Secrets required:**

| Secret | Value |
|--------|-------|
| `GCP_SA_KEY` | JSON key for `vertex-trainer@entity-resolution-487121.iam.gserviceaccount.com` |
| `GCP_PROJECT_ID` | `entity-resolution-487121` |

To create the service account key:
```bash
gcloud iam service-accounts keys create key.json \
  --iam-account=vertex-trainer@entity-resolution-487121.iam.gserviceaccount.com
```

Add the contents of `key.json` to GitHub → Settings → Secrets → Actions → `GCP_SA_KEY`. Delete `key.json` locally after.

---

## Model Monitoring & Automated Retraining

The Monitoring Pipeline runs daily at 2am UTC via Cloud Scheduler on Vertex AI Pipelines.

### What It Monitors
- **Performance metrics**: F1, precision, recall on holdout test set
- **Data drift**: Kolmogorov-Smirnov test per feature (via Evidently AI), comparing production distribution against training reference

### Retraining Thresholds
| Condition | Threshold | Action |
|-----------|-----------|--------|
| Data drift | > 30% of features drifted | `RETRAIN_DATA` — triggers Airflow DAG + Model Pipeline |
| F1 drop | Below `min_f1` in config | `RETRAIN_MODEL` — triggers Model Pipeline only |
| Healthy | All thresholds met | Slack notification only |

### Automated Retraining Flow
```
Cloud Scheduler (daily 2am UTC)
    → Vertex AI Monitoring Pipeline
        → Evaluate model on holdout data
        → Detect drift (KS test per feature)
        → Check thresholds
            → RETRAIN_DATA: trigger Airflow DAG → retrain model → redeploy
            → RETRAIN_MODEL: retrain model → redeploy
            → HEALTHY: Slack alert
```

### Notifications
Slack alerts are sent after every pipeline run with F1, precision, recall, drift ratio, and drifted feature count. Configure `SLACK_WEBHOOK_URL` in the Monitoring Pipeline `.env`.

### Grafana Dashboard
Available at `http://<VM_IP>:3000` (admin/admin) after training has populated BigQuery. Panels:
- Model Performance Over Time (F1, precision, recall)
- Data Drift Over Time (per-feature drift scores)
- Prediction Confidence Over Time
- Latest Drift Score by Feature
- Current Model Metrics

---

## Logs

| What | Where |
|------|-------|
| VM startup script | `sudo journalctl -u google-startup-scripts.service --no-pager` |
| Docker container logs | `sudo docker logs <container-name> -f` |
| Airflow task logs | Airflow UI → DAG → task → logs |
| MLflow runs | MLflow UI → Experiments |
| Monitoring pipeline | Vertex AI Console → Pipelines |
| Cloud Run logs | GCP Console → Cloud Run → `er-inference-api` → Logs |
| BigQuery monitoring data | `SELECT * FROM entity_resolution.monitoring_metrics ORDER BY timestamp DESC LIMIT 10` |

---

## Re-running After VM Restart

```bash
gcloud compute ssh production-vm --zone=us-central1-a
cd /opt/Entity-Resolution && bash setup.sh
```