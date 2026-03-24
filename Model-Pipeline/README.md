# Entity Resolution — Model Pipeline

End-to-end MLOps pipeline for training, evaluating, and deploying a DeBERTa-based entity resolution model. Built on GCP with Airflow orchestration, MLflow experiment tracking, and Docker-based execution.

---

## Architecture

```
GitHub push
    ↓
GitHub Actions CI
    lint → validate config → test → build Docker image → push to Artifact Registry
    ↓
Airflow DAG (er_model_pipeline)
    train → evaluate → bias_detection + sensitivity_analysis → rollback_check → quality_gate → push_to_registry
    ↓
MLflow (experiment tracking)    GCS (model weights + artifacts)    Artifact Registry (model image)
```

The pipeline is triggered manually or automatically by the data pipeline (`er_data_pipeline`) via `TriggerDagRunOperator`.

---

## Project Structure

```
Model-Pipeline/
├── config/
│   └── training_config.yaml       # All pipeline configuration
├── dags/
│   └── airflow_model_dag.py       # Airflow DAG definition
├── scripts/
│   ├── train.py                   # DeBERTa + LoRA fine-tuning
│   ├── evaluate.py                # Threshold-based evaluation, metrics + plots
│   ├── bias_detection.py          # Slice-based fairness analysis
│   ├── sensitivity_analysis.py    # Feature importance via field masking
│   └── push_to_registry.py        # Quality gate + push model to Artifact Registry
├── Dockerfile                     # Training container image
├── docker-compose.yml             # Local services: mlflow, trainer, airflow
└── requirements.txt               # Python dependencies
```

---

## Model

**Base model:** `microsoft/deberta-v3-base`
**Fine-tuning:** LoRA (r=8, alpha=16, dropout=0.1)
**Task:** Binary classification — match / no-match for person entity pairs
**Input features:** name1, name2, address1, address2

---

## Pipeline Stages

### 1. Train (`train.py`)
- Loads training data from GCS (`gs://entity-resolution-bucket-1/training/`)
- Fine-tunes DeBERTa-v3-base with LoRA adapters
- Logs parameters and validation metrics to MLflow
- Uploads model weights to GCS (`models/{entity_type}/final_model/`)

### 2. Evaluate (`evaluate.py`)
- Downloads model weights from GCS
- Runs inference on test set with calibrated threshold (0.45)
- Generates confusion matrix, ROC curve, PR curve
- Logs test metrics to MLflow

### 3. Bias Detection (`bias_detection.py`)
- Slices predictions by `source_dataset` and `entity_type`
- Computes F1 disparity across slices
- Flags bias if disparity exceeds 0.15 threshold

### 4. Sensitivity Analysis (`sensitivity_analysis.py`)
- Masks input fields one at a time (name1, name2, address1, address2)
- Measures probability drop to estimate feature importance
- Generates bar chart and summary plot

### 5. Rollback Check
- Compares current F1 against best previous GO run in MLflow
- Triggers rollback if F1 degradation > 2%
- Rollback re-tags previous model image as `:latest` in Artifact Registry

### 6. Quality Gate
- Checks test_f1 ≥ 0.95, precision ≥ 0.95, recall ≥ 0.95, auc ≥ 0.95
- Branches to `push_to_registry` (GO) or `alert` (NO-GO)

### 7. Push to Registry (`push_to_registry.py`)
- Packages model weights into a Docker serving image
- Auto-increments version (v1, v2, ...)
- Pushes to Artifact Registry
- Saves rollback metadata to `registry_log.json`

---

## Setup

### Prerequisites

- GCP project with billing enabled
- GCS bucket: `entity-resolution-bucket-1`
- Artifact Registry repository: `ml-models`
- Docker with NVIDIA GPU support (for training)
- `gcloud` CLI authenticated

### Environment Variables (`.env`)

```bash
GCS_ARTIFACT_ROOT=gs://entity-resolution-bucket-1/mlflow-artifacts
AIRFLOW_SECRET_KEY=your-secret-key
```

### Start services

```bash
cd Model-Pipeline
docker compose up -d
```

Services:
- MLflow UI: `http://localhost:5000`
- Airflow UI: `http://localhost:8081` (admin / admin)

### Run scripts manually

```bash
# Train
docker compose run --rm trainer python scripts/train.py

# Evaluate
docker compose run --rm trainer python scripts/evaluate.py

# Bias detection
docker compose run --rm trainer python scripts/bias_detection.py

# Sensitivity analysis
docker compose run --rm trainer python scripts/sensitivity_analysis.py

# Push to registry (run on VM, needs gcloud)
CONFIG_PATH=config/training_config.yaml python scripts/push_to_registry.py
```

---

## CI/CD (GitHub Actions)

Workflow: `.github/workflows/train-pipeline.yml`

Triggers on push/PR to `main` or `dev` branches when `Model-Pipeline/**` changes.

**Jobs:**

| Job | Description |
|-----|-------------|
| lint | black, isort, flake8 |
| validate-config | checks required sections in `training_config.yaml` |
| test | pytest (skips gracefully if no tests) |
| build | builds and pushes `er-trainer:latest` to Artifact Registry |

**Required GitHub Secrets:**

| Secret | Value |
|--------|-------|
| `GCP_SA_KEY` | Service account JSON with Artifact Registry write access |

---

## Configuration (`config/training_config.yaml`)

Key settings:

```yaml
data:
  gcs_bucket: "entity-resolution-bucket-1"
  entity_types: ["person"]

model:
  base_model: "microsoft/deberta-v3-base"

training:
  num_epochs: 3
  batch_size: 16
  learning_rate: 2e-05

validation:
  classification_threshold: 0.45
  min_f1: 0.95
  min_precision: 0.95
  min_recall: 0.95
  min_auc: 0.95

mlflow:
  tracking_uri: "http://mlflow:5000"
  experiment_name: "entity-resolution-deberta"
```

---
## Data Pipeline Integration

The data pipeline (`er_data_pipeline`) triggers this pipeline automatically on successful completion via `TriggerDagRunOperator`. To trigger manually:

```bash
# Airflow UI: DAGs → er_model_pipeline → Trigger DAG
# Or CLI:
airflow dags trigger er_model_pipeline
```
