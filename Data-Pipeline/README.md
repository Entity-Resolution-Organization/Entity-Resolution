# Entity Resolution Data Pipeline

A production-grade, multi-domain data pipeline for entity resolution using Apache Airflow, DVC, and Google Cloud Platform. This pipeline processes person, product, and publication entities across 6 datasets to generate training data for ML-based entity matching models.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [GCP Setup (Required for Cloud Upload)](#gcp-setup-required-for-cloud-upload)
6. [Dataset Information](#dataset-information)
7. [Pipeline Components](#pipeline-components)
8. [Configuration](#configuration)
9. [Running the Pipeline](#running-the-pipeline)
10. [Data Outputs](#data-outputs)
11. [Validation & Quality](#validation--quality)
12. [Testing](#testing)
13. [Data Versioning (DVC)](#data-versioning-dvc)
14. [Troubleshooting](#troubleshooting)
15. [Alerting & Monitoring](#alerting--monitoring)
16. [Pipeline Optimization](#pipeline-optimization)
17. [Development Guide](#development-guide)
18. [Team & Acknowledgments](#team--acknowledgments)

---

## Project Overview

### What This Pipeline Does

Entity resolution identifies records referring to the same real-world entity across different data sources. This pipeline:

- **Ingests data** from 6 diverse sources covering 3 entity types
- **Normalizes schemas** to a common format (id, name, address)
- **Generates training pairs** with positive (match) and negative (non-match) labels
- **Detects bias** in entity type distribution, language, and geography
- **Validates data quality** using schema expectations
- **Versions data** with DVC for reproducibility
- **Uploads to GCP** (Cloud Storage + BigQuery)

### Multi-Domain Capability

| Domain | Entity Type | Use Case |
|--------|-------------|----------|
| **Person** | PERSON | Customer deduplication, KYC, fraud detection |
| **Product** | PRODUCT | Catalog matching, price comparison |
| **Publication** | PUBLICATION | Author disambiguation, citation linking |

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Apache Airflow 2.8.1 | DAG scheduling, task dependencies |
| Data Versioning | DVC 3.x | Dataset tracking, reproducibility |
| Validation | Schema Validator | Data quality expectations |
| Bias Detection | Custom module | Fairness analysis |
| Cloud Storage | Google Cloud Storage | Production data storage |
| Data Warehouse | BigQuery | Analytics and ML training |
| Containerization | Docker Compose | Local development environment |

---

## Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ENTITY RESOLUTION DATA PIPELINE                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         DATA SOURCES (6 Datasets)                     │   │
│  ├──────────────┬──────────────┬──────────────┬─────────────────────────┤   │
│  │   PERSON     │   PERSON     │   PERSON     │                         │   │
│  │ Pseudopeople │  NC Voters   │  OFAC SDN    │                         │   │
│  │    (5K)      │    (5K)      │   (5K)*      │                         │   │
│  ├──────────────┼──────────────┼──────────────┤                         │   │
│  │   PRODUCT    │   PRODUCT    │ PUBLICATION  │                         │   │
│  │ WDC Products │ Amazon 2018  │  DBLP-ACM    │                         │   │
│  │    (5K)      │    (5K)      │    (5K)      │                         │   │
│  └──────────────┴──────────────┴──────────────┴─────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      AIRFLOW DAG PIPELINE                             │   │
│  │                                                                       │   │
│  │   ┌─────────┐   ┌────────────┐   ┌──────────────┐   ┌─────────────┐  │   │
│  │   │  Load   │──▶│  Validate  │──▶│  Transform   │──▶│  Organize   │  │   │
│  │   │  (6x)   │   │    Raw     │   │   (6x) +     │   │  Datasets   │  │   │
│  │   │         │   │            │   │   Pairs      │   │             │  │   │
│  │   └─────────┘   └────────────┘   └──────────────┘   └──────┬──────┘  │   │
│  │                                                             │         │   │
│  │                                  ┌──────────────────────────┼────────┐ │   │
│  │                                  ▼                          ▼        │ │   │
│  │                         ┌─────────────────┐      ┌─────────────────┐│ │   │
│  │                         │     Schema      │      │Training Splits  ││ │   │
│  │                         │   Validation    │      │   Validation    ││ │   │
│  │                         └─────────────────┘      └─────────────────┘│ │   │
│  │                                  │                          │        │ │   │
│  │                                  └──────────────┬───────────┘        │ │   │
│  │                                                 ▼                    │ │   │
│  │                                        ┌─────────────────┐           │ │   │
│  │                                        │  Bias Detection │           │ │   │
│  │                                        └────────┬────────┘           │ │   │
│  │                                                 ▼                    │ │   │
│  │                                        ┌─────────────────┐           │ │   │
│  │                                        │  Quality Gate   │           │ │   │
│  │                                        └────────┬────────┘           │ │   │
│  │                                                 ▼                    │ │   │
│  │                                        ┌─────────────────┐           │ │   │
│  │                                        │  DVC Version    │           │ │   │
│  │                                        │  (Track Data)   │           │ │   │
│  │                                        └────────┬────────┘           │ │   │
│  └─────────────────────────────────────────────────┼──────────────────────┘   │
│                                                     │                         │
│                                                     ▼                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                          CLOUD UPLOAD                                 │   │
│  │                                                                       │   │
│  │   ┌─────────────┐          ┌─────────────┐         ┌─────────────┐   │   │
│  │   │    GCS      │          │  BigQuery   │         │   Verify    │   │   │
│  │   │   Upload    │─────────▶│    Load     │────────▶│  Complete   │   │   │
│  │   │             │          │             │         │             │   │   │
│  │   └─────────────┘          └─────────────┘         └─────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

* OFAC SDN downloads from Treasury API; may fail (pipeline continues with 5 datasets)
```

### Airflow Task Flow (32 Tasks)

```
start
  │
  ├─▶ load_pseudopeople ────┐
  ├─▶ load_nc_voters ───────┤
  ├─▶ load_ofac_sdn ────────┤
  ├─▶ load_wdc_products ────┼─▶ validate_all_raw ───┐
  ├─▶ load_amazon_2018 ─────┤                       │
  └─▶ load_dblp_acm ────────┘                       │
                                                     │
                         ┌───────────────────────────┘
                         │
  ┌──────────────────────┼──────────────────────┐
  │                      │                      │
  ▼                      ▼                      ▼
transform_pseudopeople  transform_nc_voters  transform_ofac_sdn
transform_wdc_products  transform_amazon_2018 transform_dblp_acm
  │                      │                      │
  └──────────────────────┼──────────────────────┘
                         │
                         ▼
                  organize_datasets
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
  validate_training  schema_validation  bias_detection
     _splits              │                │
        │                 │                │
        └─────────────────┼────────────────┘
                          ▼
                    quality_gate
                          │
                          ▼
                  dvc_track_and_version
                          │
                          ▼
                    upload_to_gcs
                          │
                          ▼
                   load_to_bigquery
                          │
                          ▼
                   verify_and_complete
                          │
                          ▼
                   pipeline_complete
```

**Pipeline always runs full flow:** Load → Validate → Transform → Organize → Validations → Quality Gate → DVC → GCS → BigQuery → Complete

---

## Prerequisites

### Required Software

| Software | Version | Installation |
|----------|---------|--------------|
| Python | 3.10+ | [python.org](https://python.org) |
| Docker | 24.0+ | [docker.com](https://docker.com) |
| Docker Compose | 2.20+ | Included with Docker Desktop |
| Git | 2.40+ | [git-scm.com](https://git-scm.com) |

### System Requirements

- **Disk Space**: 10GB free (for Docker images and data)
- **RAM**: 8GB minimum, 16GB recommended
- **OS**: macOS, Linux, or Windows with WSL2

### For Cloud Upload (Required for Full Pipeline)

- Google Cloud Platform account with billing enabled
- GCS bucket
- BigQuery dataset
- Service account with Storage Admin and BigQuery permissions

---

## Quick Start

Follow these steps to get the pipeline running in under 10 minutes.

### TL;DR - Quick Commands

```bash
# 1. Start Airflow
cd Data-Pipeline
docker compose up -d

# 2. Wait for services (30 seconds)
sleep 30

# 3. Trigger pipeline (local mode)
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline

# 4. Trigger pipeline (cloud mode - requires GCP setup)
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline \
  --conf '{"execution_mode": "cloud"}'

# 5. Check status (replace RUN_ID with actual run ID from step 3/4)
docker exec data-pipeline-airflow-scheduler-1 airflow tasks states-for-dag-run \
  er_data_pipeline "RUN_ID"

# 6. View Airflow UI
open http://localhost:8080  # Login: admin/admin
```

### Step 1: Clone Repository

```bash
git clone https://github.com/Entity-Resolution-Organization/Entity-Resolution.git
cd Entity-Resolution/Data-Pipeline
```

### Step 2: Install Python Dependencies

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Step 3: Start Airflow

```bash
docker compose up -d
# Wait ~30 seconds for initialization
docker compose ps  # Verify all services running
```

### Step 4: Access Airflow UI

Open browser: **http://localhost:8080**
- **Username**: `admin`
- **Password**: `admin`

### Step 5: Trigger Pipeline

**Via UI**: Find `er_data_pipeline`, click play button (▶)

**Via CLI**:
```bash
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

### Step 6: View Results

```bash
ls -la data/processed/         # Local outputs
ls -la data/training/          # Training splits
cat data/metrics/quality_gate_results.json | python -m json.tool
```

---

## GCP Setup (Required for Cloud Upload)

Pipeline uploads data to GCS and BigQuery. External users must complete this setup to replicate the full pipeline.

### Step 1: Create GCP Project

```bash
# Edit docker-compose.yml
# Change: LOCAL_MODE: "true" → LOCAL_MODE: "false"

# Set GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Restart Airflow
docker compose restart
```

---

## Running the Pipeline

### Airflow Web UI

Access at **http://localhost:8080** (credentials: `admin`/`admin`)

**Key pages:**
- **DAGs**: View and trigger pipelines
- **Graph**: Visualize task dependencies
- **Grid**: Monitor task status over time
- **Logs**: Debug task failures

### Pipeline Trigger Commands

#### Local Mode (Default - No Cloud Uploads)

```bash
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

#### Cloud Mode (Uploads to GCS + BigQuery)

```bash
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline \
  --conf '{"execution_mode": "cloud"}'
```

> **Note:** Cloud mode requires GCP credentials configured in `.env` and `secrets/gcp-sa-key.json`

### Monitoring Pipeline Status

#### Check All Task States

```bash
# First, get the run ID from trigger output, then:
docker exec data-pipeline-airflow-scheduler-1 airflow tasks states-for-dag-run \
  er_data_pipeline "manual__2026-02-23T20:45:41+00:00"
```

#### List Recent DAG Runs

```bash
docker exec data-pipeline-airflow-scheduler-1 airflow dags list-runs \
  -d er_data_pipeline --limit 5
```

#### Check Specific Task State

```bash
docker exec data-pipeline-airflow-scheduler-1 airflow tasks state \
  er_data_pipeline quality_gate "manual__2026-02-23T20:45:41+00:00"
```

### Clean Run (Clear All Data)

Before running a fresh pipeline, clear existing data:

```bash
# Clear local data directories
rm -rf data/processed/* data/training/* data/analytics/* data/metrics/* data/raw/*

# Clear container data
docker exec data-pipeline-airflow-scheduler-1 rm -rf \
  /opt/airflow/data/processed/* \
  /opt/airflow/data/training/* \
  /opt/airflow/data/analytics/* \
  /opt/airflow/data/metrics/* \
  /opt/airflow/data/raw/*

# Trigger fresh run
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

### Container Management

```bash
# Restart containers (after code changes)
docker compose restart airflow-scheduler airflow-webserver

# View container logs
docker compose logs -f airflow-scheduler

# Check container health
docker compose ps

# Stop all containers
docker compose down

# Start all containers
docker compose up -d

# Rebuild containers (after Dockerfile changes)
docker compose up -d --build
```

### Checking Outputs

```bash
# View processed data (local mount)
ls -la data/processed/
ls -la data/training/
ls -la data/analytics/

# View metrics and reports
cat data/metrics/bias_report.json | python -m json.tool
cat data/metrics/quality_gate_results.json | python -m json.tool

# Count records in merged data
wc -l data/analytics/merged_all.csv

# Check from inside container
docker exec data-pipeline-airflow-scheduler-1 ls -la /opt/airflow/data/processed/
```

### Expected Pipeline Output

After a successful run, you should see:

| Directory | Contents |
|-----------|----------|
| `data/processed/` | 6 dataset folders with accounts.csv and pairs.csv |
| `data/training/` | person/, product/, publication/ with train/val/test splits |
| `data/analytics/` | merged_all.csv (~287K rows), merged_pairs.csv |
| `data/metrics/` | bias_report.json, quality_gate_results.json, schema_validation_results.json |

### Pipeline Duration

| Mode | Tasks | Duration |
|------|-------|----------|
| Local | 26 | ~3-4 minutes |
| Cloud | 26 | ~4-5 minutes |

---

## Cloud Integration (GCS + BigQuery)

This section covers how to run the pipeline with Google Cloud Platform integration, uploading data to Cloud Storage and BigQuery.

### Prerequisites for Cloud Mode

1. **Google Cloud Project** with billing enabled
2. **Service Account** with the following roles:
   - `Storage Admin` (for GCS bucket operations)
   - `BigQuery Data Editor` (for loading data)
   - `BigQuery Job User` (for running load jobs)
3. **GCS Bucket** created in your project
4. **BigQuery Dataset** created in your project

### Step 1: Create GCP Resources

```bash
gcloud storage buckets create gs://${PROJECT_ID}-entity-resolution \
  --project=${PROJECT_ID} \
  --location=us-central1
```

### Step 4: Create BigQuery Dataset

```bash
bq mk --dataset \
  --project_id=${PROJECT_ID} \
  --location=US \
  entity_resolution_bq

# Create tables
bq mk --table ${PROJECT_ID}:entity_resolution_bq.accounts \
  id:STRING,name:STRING,address:STRING,dob:STRING,entity_type:STRING,source_dataset:STRING,cluster_id:STRING
```

### Step 5: Create Service Account

```bash
# Create service account
gcloud iam service-accounts create airflow-sa \
  --display-name="Airflow Pipeline Service Account" \
  --project=${PROJECT_ID}

# Grant Storage Admin role
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:airflow-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.admin"

# Grant BigQuery Admin role
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:airflow-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"
```

### Step 6: Generate Service Account Key

```bash
# Create secrets directory
mkdir -p secrets

# Generate key file
gcloud iam service-accounts keys create secrets/gcp-sa-key.json \
  --iam-account=airflow-sa@${PROJECT_ID}.iam.gserviceaccount.com

# Verify key file
cat secrets/gcp-sa-key.json | jq '.client_email'
# Should show: airflow-sa@your-project-id.iam.gserviceaccount.com
```

**Security Note:** The `secrets/` directory is in `.gitignore` and won't be committed to git.

### Step 7: Configure Environment Variables

Create `.env` file:

```bash
cat > .env << EOF
# =============================================================================
# Airflow Environment Configuration
# =============================================================================

# Airflow UID
AIRFLOW_UID=50000

# Airflow Web UI Credentials
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# GCP Configuration
GCP_PROJECT_ID=${PROJECT_ID}
GCS_BUCKET=${PROJECT_ID}-entity-resolution
BQ_DATASET=entity_resolution_bq
EOF
```

### Step 8: Restart Docker Services

```bash
# Restart to load new credentials
docker compose down
docker compose up -d

# Verify credentials mounted
docker compose exec airflow-scheduler ls -la /opt/airflow/secrets/
# Should show: gcp-sa-key.json
```

### Step 9: Run Pipeline

```bash
# Trigger DAG
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

### Step 10: Verify Cloud Upload

**Check GCS:**
```bash
# List uploaded files
gsutil ls -r gs://${GCS_BUCKET}/

# Expected structure:
# gs://bucket/analytics/2026-02-24/merged_all.csv
# gs://bucket/training/2026-02-24/person/train.csv
# gs://bucket/metrics/2026-02-24/quality_gate.json
```

**Check BigQuery:**
```bash
# Query row count
bq query --project_id=${PROJECT_ID} \
  "SELECT COUNT(*) as total FROM entity_resolution_bq.accounts"

# Expected: ~287,000 rows
```

### Troubleshooting GCP Setup

**Error: "Permission denied" on GCS**
```bash
# Verify service account has correct role
gcloud projects get-iam-policy ${PROJECT_ID} \
  --flatten="bindings[].members" \
  --filter="bindings.members:airflow-sa@${PROJECT_ID}.iam.gserviceaccount.com"
```

**Error: "Invalid credentials"**
```bash
# Test credentials in container
docker compose exec airflow-scheduler python -c \
  "from google.cloud import storage; print(storage.Client().project)"
```

**Error: "Bucket does not exist"**
```bash
# List buckets
gsutil ls -p ${PROJECT_ID}
```

---

## Dataset Information

### Supported Datasets

| Dataset | Entity Type | Base Records | Target Records | Source | Status |
|---------|-------------|--------------|----------------|--------|--------|
| **Pseudopeople** | PERSON | 500 | 5,000 | Synthetic | Active |
| **NC Voters** | PERSON | 500 | 5,000 | Synthetic | Active |
| **OFAC SDN** | PERSON | 500 | 5,000 | Treasury API | May fail |
| **WDC Products** | PRODUCT | 500 | 5,000 | Synthetic | Active |
| **Amazon 2018** | PRODUCT | 500 | 5,000 | Synthetic | Active |
| **DBLP-ACM** | PUBLICATION | 500 | 5,000 | Synthetic | Active |

### Entity Type Distribution

Default configuration produces:

| Entity Type | Accounts | Training Pairs | Percentage |
|-------------|----------|----------------|------------|
| PERSON | 137,330 | 25,000 | 48% |
| PRODUCT | 100,000 | 20,000 | 35% |
| PUBLICATION | 50,000 | 10,000 | 17% |
| **Total** | **287,330** | **55,000** | 100% |

---

## Pipeline Components

### Core Modules

| Script | Purpose | Key Functions |
|--------|---------|---------------|
| `dataset_factory.py` | Dataset handlers with factory pattern | `get_dataset_handler()`, `download()`, `normalize_schema()` |
| `preprocessing.py` | Data transformation and pair generation | `preprocess_dataset()`, `generate_pairs()` |
| `data_validation.py` | Quality gates and validation | `DatasetValidator`, `QualityGate` |
| `schema_validation.py` | Schema expectations | `SchemaValidator.validate_all()` |
| `bias_detection.py` | Multi-domain bias analysis | `BiasDetector.generate_bias_report()` |

### Airflow DAG Structure

| Task ID | Description | Duration |
|---------|-------------|----------|
| `load_{dataset}` (6x) | Download/generate dataset | 5-10s each |
| `validate_all_raw` | Consolidated validation | 2-3s |
| `transform_{dataset}` (6x) | Normalize + generate pairs | 10-15s each |
| `organize_datasets` | Create training splits + analytics | 5-8s |
| `validate_training_splits` | Check for data leakage | 2-3s |
| `schema_validation` | Validate schema expectations | 2-3s |
| `bias_detection` | Analyze bias across dimensions | 3-5s |
| `quality_gate` | Go/no-go decision | 1s |
| `dvc_track_and_version` | Version data with DVC | 5-10s |
| `upload_to_gcs` | Upload to Cloud Storage | 5-10s |
| `load_to_bigquery` | Load BigQuery tables | 5-10s |

**Total Pipeline Time**: ~2-3 minutes (default config)

---

## Configuration

### Dataset Configuration (`config/datasets.yaml`)

```yaml
# All 6 datasets processed in parallel
active_datasets:
  - "pseudopeople"
  - "nc_voters"
  - "ofac_sdn"
  - "wdc_products"
  - "amazon_2018"
  - "dblp_acm"

datasets:
  pseudopeople:
    name: "Pseudopeople Synthetic Population"
    entity_type: "PERSON"
    base_records: 500        # Raw records
    target_records: 5000     # After expansion
    corruption:
      rate: 0.15             # 15% corruption
      name_variants: true
      address_typos: true
    pairs:
      total: 1000
      positive: 500          # Matching pairs
      negative: 500          # Non-matching pairs
```

### Scaling Configuration

| Use Case | base_records | target_records | Pipeline Time |
|----------|--------------|----------------|---------------|
| Quick test | 100 | 1,000 | ~30s |
| Development | 500 | 5,000 | ~2min |
| Integration | 1,000 | 50,000 | ~5min |
| Production | 100,000 | 6,000,000 | ~30min |

---

## Running the Pipeline

### Monitoring in Airflow UI

1. **Graph View**: Visualize task dependencies
2. **Grid View**: Track runs over time
3. **Logs**: Debug failures (click task → Logs)
4. **Gantt Chart**: Identify bottlenecks

### Command Line Operations

```bash
# Trigger pipeline
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline

# Check run status
docker exec data-pipeline-airflow-scheduler-1 airflow dags list-runs -d er_data_pipeline

# View task logs
docker exec data-pipeline-airflow-scheduler-1 airflow tasks logs \
  er_data_pipeline load_pseudopeople manual__2026-02-24T10:00:00+00:00
```

---

## Data Outputs

### Local Outputs

| Directory | Contents | Description |
|-----------|----------|-------------|
| `data/raw/` | Per-dataset raw CSV files | Source data after download |
| `data/processed/` | Per-dataset accounts + pairs | Transformed outputs |
| `data/training/` | Train/val/test splits by entity type | ML training data |
| `data/analytics/` | Merged data across all datasets | Bias analysis |
| `data/metrics/` | Validation JSON reports | Quality metrics |

### Cloud Outputs (GCS)

```
gs://{bucket}/
├── analytics/2026-02-24/
│   ├── merged_all.csv          (287K accounts)
│   └── merged_pairs.csv        (60K pairs)
├── training/2026-02-24/
│   ├── person/
│   │   ├── train.csv           (17,500 pairs)
│   │   ├── val.csv             (3,750 pairs)
│   │   └── test.csv            (3,750 pairs)
│   ├── product/...
│   └── publication/...
└── metrics/2026-02-24/
    ├── quality_gate.json
    └── bias_report.json
```

### BigQuery Tables

| Table | Schema | Row Count |
|-------|--------|-----------|
| `accounts` | id, name, address, entity_type, source_dataset | ~287K |

---

## Validation & Quality

### Schema Validation

**Accounts Expectations:**
- Required columns: id, name, address
- ID uniqueness per dataset
- Null rate thresholds: name <5%, address <10%
- Entity type in {PERSON, PRODUCT, PUBLICATION}

**Pairs Expectations:**
- Required columns: id1, id2, label
- Label values in {0, 1}
- Unique (id1, id2) pairs

### Training Split Validation

- 70/15/15 stratified splits
- Data leakage detection
- Label balance checks

### Bias Detection

| Bias Type | Metric | Risk Threshold |
|-----------|--------|----------------|
| Entity Type | Distribution imbalance | >3x |
| Language | Non-ASCII character rate | >5% |
| Geographic | US vs international | >80% single region |
| Data Source | Records per source | >3x |
| Label | Positive/negative ratio | >2x |

### Quality Gate

Pipeline halts if:
- Schema validation fails
- Data leakage detected
- Critical bias issues found (HIGH risk)

---

## Testing

### Run Unit Tests

```bash
# All tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=scripts --cov-report=html --cov-report=term-missing
open htmlcov/index.html
```

### Test Coverage

| Module | Coverage |
|--------|----------|
| preprocessing.py | 87% |
| schema_validation.py | 71% |
| bias_detection.py | 62% |
| dataset_factory.py | 49% |
| **Total** | **69%** |

**Test Suite**: 87 tests across 3 categories

---

## Data Versioning (DVC)

### DVC Automatic Versioning

Every pipeline run automatically:
1. Tracks outputs with DVC (`dvc add`)
2. Pushes to remote storage (`dvc push`)
3. Commits tracking files to git

### DVC Commands

```bash
# Check status
dvc status

# Pull versioned data
dvc pull

# Reproduce pipeline
dvc repro

# View pipeline DAG
dvc dag
```

### DVC Remote Storage

**Local Development**: `../dvc-storage`
**Production**: Google Cloud Storage bucket

---

## Troubleshooting

### Pipeline Failures

```bash
# Check specific task logs
docker exec data-pipeline-airflow-scheduler-1 airflow tasks logs \
  er_data_pipeline <task_id> <run_id>

# Check Airflow scheduler logs
docker logs data-pipeline-airflow-scheduler-1 --tail 100
```

### Common Issues

**DAG not appearing**:
```bash
docker exec data-pipeline-airflow-scheduler-1 python -c \
  "import sys; sys.path.insert(0, '/opt/airflow/dags'); import airflow_data_dag"
```

**Out of disk space**:
```bash
docker system prune -a
rm -rf data/raw/* data/processed/*
```

**GCP upload fails**:
```bash
# Verify credentials
docker exec data-pipeline-airflow-scheduler-1 \
  cat /opt/airflow/secrets/gcp-sa-key.json | jq .client_email
```

---

## Alerting & Monitoring

### Alert Channels

| Channel | Configuration | Use Case |
|---------|---------------|----------|
| Console | Always enabled | Development |
| Email | SMTP env vars | Team notifications |
| Slack | Webhook URL | Real-time alerts |

### Alert Types

- Data drift detection
- Schema violations
- Bias detection
- Pipeline failures

See `scripts/alerting.py` for implementation.

---

## Pipeline Optimization

**Bottlenecks identified:**
- Data loading: 35-40% of runtime
- Transformation: 25-30% of runtime

**Optimization opportunities:**
- Parallel dataset loading (40-60% faster)
- Per-entity transformation parallelism (50-70% faster)
- Incremental validation (50% faster)

See **[docs/pipeline_optimization.md](docs/pipeline_optimization.md)** for details.

---

## Development Guide

### Adding a New Dataset

1. Create handler in `dataset_factory.py`:
```python
class NewDatasetHandler(DatasetHandler):
    def download(self) -> pd.DataFrame:
        # Download or generate data
        pass

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[['id', 'name', 'address']]
```

2. Register in factory:
```python
def get_dataset_handler(dataset_name: str, config: Dict):
    handlers = {
        'new_dataset': NewDatasetHandler,
    }
```

3. Add to `datasets.yaml`:
```yaml
active_datasets:
  - "new_dataset"

datasets:
  new_dataset:
    entity_type: "PERSON"
    base_records: 500
```

---

## Team & Acknowledgments

**Northeastern University - MLOps Course (Spring 2026)**

### Dataset Sources

| Dataset | Source | License |
|---------|--------|---------|
| Pseudopeople | IHME | MIT |
| OFAC SDN | US Treasury | Public Domain |
| WDC Products | Web Data Commons | CC BY-SA |
| Amazon 2018 | UCSD/McAuley | Research |
| DBLP-ACM | Leipzig DB Group | Research |

### References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [DVC Documentation](https://dvc.org/doc)
- [Entity Resolution Survey](https://arxiv.org/abs/2101.06427)

---