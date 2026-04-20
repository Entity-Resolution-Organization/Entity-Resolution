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
│   ├── dags/                   # Inference Airflow DAG
│   ├── Dockerfile              # Multi-stage build for Cloud Run
│   └── requirements.txt
├── Initial_Setup/              # GCP infrastructure (Terraform)
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
| Infrastructure | Terraform, GCP |

## Getting Started

### Prerequisites
- GCP project with Vertex AI, BigQuery, Cloud Storage, Cloud Run enabled
- Python 3.10+, Node.js 20+
- Docker

### 1. Infrastructure
Provision GCP resources using Terraform. See [Initial_Setup/](Initial_Setup/)

### 2. Data Pipeline
```bash
cd Data-Pipeline
cp .env.example .env
docker compose up -d
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

### 3. Model Training
```bash
cd Model-Pipeline
pip install -r requirements.txt
python scripts/train.py --config config/training_config.yaml
```

### 4. Inference Pipeline (local)
```bash
cd Inference-Pipeline
pip install -r requirements.txt
cd frontend && npm ci && npm run build && cd ..
GCP_BUCKET_NAME=<bucket> CONFIG_PATH=../Model-Pipeline/config/training_config.yaml \
  USE_MOCK_CLIENT=false uvicorn scripts.app:app --host 0.0.0.0 --port 8000
```

### 5. Deploy to Cloud Run
```bash
cd Inference-Pipeline
docker build --platform linux/amd64 -t gcr.io/<project>/entity-resolution-inference .
docker push gcr.io/<project>/entity-resolution-inference
gcloud run deploy entity-resolution \
  --image gcr.io/<project>/entity-resolution-inference \
  --region us-central1 --allow-unauthenticated \
  --memory 2Gi --cpu 2 --timeout 600
```

## Team

Northeastern University — MLOps Spring 2026 — Group 13

---

## Deployment

For full deployment instructions including Terraform setup, environment configuration, running services, monitoring, and CI/CD — see the [Deployment Guide](terraform/README.md) or refer to the sections below.

### Quick Start (Fresh Environment)

```bash
# 1. Clone and switch to deployment branch
git clone https://github.com/Entity-Resolution-Organization/Entity-Resolution.git
cd Entity-Resolution
git checkout deployment

# 2. Authenticate with GCP
gcloud auth login
gcloud auth application-default login
gcloud config set project entity-resolution-487121

# 3. Create .env files (see .env.example in each pipeline folder)

# 4. Upload secrets
gcloud secrets create er-env-model --data-file=Model-Pipeline/.env --project=entity-resolution-487121
gcloud secrets create er-env-monitoring --data-file=Monitoring-Pipeline/.env --project=entity-resolution-487121
gcloud secrets create er-env-data --data-file=Data-Pipeline/.env --project=entity-resolution-487121

# 5. Provision infrastructure
cd terraform && terraform init && terraform apply

# 6. SSH into VM and start services
gcloud compute ssh airflow-vm --zone=us-central1-a --project=entity-resolution-487121
cd /home/ubuntu/Entity-Resolution && bash setup.sh
```

### Running Services

| Service | URL |
|---|---|
| MLflow | `http://<VM_IP>:5000` |
| Inference API + UI | `http://<VM_IP>:8000` |
| Grafana Monitoring | `http://<VM_IP>:3000` |

### CI/CD

Pushing to `main` or `dev` automatically triggers the GitHub Actions workflow (`.github/workflows/deploy.yml`) which builds and redeploys the Inference API to Cloud Run.
