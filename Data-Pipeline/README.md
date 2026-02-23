# Entity Resolution Data Pipeline

A production-grade, multi-domain data pipeline for entity resolution using Apache Airflow, DVC, and Google Cloud Platform. This pipeline processes person, product, and publication entities across 6 datasets to generate training data for ML-based entity matching models.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [Dataset Information](#dataset-information)
6. [Pipeline Components](#pipeline-components)
7. [Configuration](#configuration)
8. [Running the Pipeline](#running-the-pipeline)
9. [Cloud Integration (GCS + BigQuery)](#cloud-integration-gcs--bigquery)
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

Entity resolution (also known as record linkage or deduplication) identifies records that refer to the same real-world entity across different data sources. This pipeline:

- **Ingests data** from 6 diverse sources covering 3 entity types
- **Normalizes schemas** to a common format (id, name, address)
- **Generates training pairs** with positive (match) and negative (non-match) labels
- **Detects bias** in entity type distribution, language, and geography
- **Validates data quality** using schema expectations
- **Versions data** with DVC for reproducibility

### Multi-Domain Capability

The pipeline supports three entity domains for domain-specific model training (e.g., LoRA adapters):

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
│  │   │  Load   │──▶│  Validate  │──▶│  Transform   │──▶│   Schema    │  │   │
│  │   │  Data   │   │    Raw     │   │   (Corrupt   │   │  Validate   │  │   │
│  │   │         │   │            │   │  + Pairs)    │   │             │  │   │
│  │   └─────────┘   └────────────┘   └──────────────┘   └─────────────┘  │   │
│  │                                                            │          │   │
│  │                                                            ▼          │   │
│  │                                                     ┌─────────────┐   │   │
│  │                                                     │    Bias     │   │   │
│  │                                                     │  Detection  │   │   │
│  │                                                     └─────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                    ┌───────────────┼───────────────┐                        │
│                    ▼               ▼               ▼                        │
│  ┌─────────────────────┐ ┌─────────────────┐ ┌─────────────────────────┐   │
│  │    LOCAL MODE       │ │   GCS BUCKET    │ │      BIGQUERY           │   │
│  │  /data/processed/   │ │  gs://bucket/   │ │  accounts, er_pairs     │   │
│  │  accounts.csv       │ │  processed/     │ │       tables            │   │
│  │  er_pairs.csv       │ │                 │ │                         │   │
│  └─────────────────────┘ └─────────────────┘ └─────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         DVC TRACKING                                  │   │
│  │    dvc.yaml ──▶ data/raw/*.csv ──▶ data/processed/*.csv              │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

* OFAC SDN may fail due to external API; pipeline continues with available datasets
```

### Airflow Task Flow

```
load_data_task ──▶ data_validation_task ──▶ data_transformation_task
                                                       │
                                                       ▼
                                            schema_validation_task
                                                       │
                                                       ▼
                                             bias_detection_task
                                                       │
                              ┌────────────────────────┼────────────────────────┐
                              ▼                        ▼                        ▼
                     [LOCAL_MODE=true]         upload_to_gcs            pipeline_complete
                      pipeline_complete              │
                                                     ▼
                                              load_to_bigquery
```

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

### Optional (for Production Mode)

- Google Cloud Platform account
- GCS bucket created
- BigQuery dataset created
- Service account with Storage and BigQuery permissions

---

## Quick Start

Follow these steps to get the pipeline running in under 10 minutes.

### Step 1: Clone Repository

```bash
git clone https://github.com/Entity-Resolution-Organization/Entity-Resolution.git
cd Entity-Resolution/Data-Pipeline
```

### Step 2: Install Python Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Start Airflow

```bash
# Start all services (Airflow, PostgreSQL)
docker compose up -d

# Wait for services to initialize (~30 seconds)
sleep 30

# Verify services are running
docker compose ps
```

Expected output:
```
NAME                                 STATUS
data-pipeline-airflow-scheduler-1   running
data-pipeline-airflow-webserver-1   running
data-pipeline-postgres-1            running
```

### Step 4: Access Airflow UI

Open your browser to: **http://localhost:8080**

- **Username**: `admin`
- **Password**: `admin`

### Step 5: Trigger the Pipeline

**Option A: Via Airflow UI**
1. Find `er_data_pipeline` in the DAG list
2. Click the play button (▶) to trigger
3. Click on the DAG to view task progress

**Option B: Via Command Line**
```bash
docker exec data-pipeline-airflow-scheduler-1 \
  airflow dags trigger er_data_pipeline
```

### Step 6: View Results

After pipeline completes (~2-3 minutes):

```bash
# Check output files
ls -la data/processed/

# View account records
head data/processed/accounts.csv

# View training pairs
head data/processed/er_pairs.csv

# View validation results
cat data/metrics/schema_validation_results.json | python -m json.tool

# View bias report
cat data/metrics/bias_report.json | python -m json.tool
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

### Why These Datasets?

1. **Diversity**: Cover 3 distinct entity domains (person, product, publication)
2. **Scalability**: Configurable from 1K to 6M records per dataset
3. **Real-world patterns**: Each includes domain-specific corruption patterns
4. **Benchmark compatibility**: DBLP-ACM is a standard ER benchmark

### Entity Type Distribution

With default configuration, the pipeline produces:

| Entity Type | Accounts | Pairs | Percentage |
|-------------|----------|-------|------------|
| PERSON | 10,000 | 2,000 | 40% |
| PRODUCT | 10,000 | 2,000 | 40% |
| PUBLICATION | 5,000 | 1,000 | 20% |
| **Total** | **25,000** | **5,000** | 100% |

---

## Pipeline Components

### Airflow Tasks

| Task ID | Description | Input | Output |
|---------|-------------|-------|--------|
| `load_data_task` | Downloads/generates data from all active datasets | datasets.yaml | data/raw/data.csv |
| `data_validation_task` | Validates raw data schema and quality | raw data | validation.json |
| `data_transformation_task` | Normalizes, corrupts, generates pairs | raw data | accounts.csv, er_pairs.csv |
| `schema_validation_task` | Validates processed data against expectations | processed data | schema_validation_results.json |
| `bias_detection_task` | Analyzes bias across entity types and sources | processed data | bias_report.json |
| `pipeline_complete` | Marks successful pipeline completion | - | - |

### Script Modules

| Script | Purpose |
|--------|---------|
| `dataset_factory.py` | Factory pattern handlers for each dataset |
| `preprocessing.py` | Normalization, corruption, pair generation |
| `schema_validation.py` | Data quality validation with expectations |
| `bias_detection.py` | Multi-domain bias analysis |

---

## Configuration

### Dataset Configuration (`config/datasets.yaml`)

```yaml
# Enable multiple datasets for multi-domain processing
active_datasets:
  - "pseudopeople"   # PERSON
  - "nc_voters"      # PERSON
  - "wdc_products"   # PRODUCT
  - "amazon_2018"    # PRODUCT
  - "dblp_acm"       # PUBLICATION

datasets:
  pseudopeople:
    name: "Pseudopeople Synthetic Population"
    entity_type: "PERSON"           # Domain classification
    base_records: 500               # Raw records to generate
    target_records: 5000            # After expansion
    corruption:
      rate: 0.15                    # 15% corruption rate
      name_variants: true
      address_typos: true
    pairs:
      total: 1000                   # Training pairs
      positive: 500                 # Matching pairs
      negative: 500                 # Non-matching pairs
```

### Adjusting Record Counts

| Use Case | base_records | target_records | Pipeline Time |
|----------|--------------|----------------|---------------|
| Quick test | 100 | 1,000 | ~30 seconds |
| Development | 500 | 5,000 | ~2 minutes |
| Integration test | 1,000 | 50,000 | ~5 minutes |
| Production | 100,000 | 6,000,000 | ~30 minutes |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOCAL_MODE` | `true` | Skip GCS/BigQuery in local mode |
| `GCP_PROJECT_ID` | `your-gcp-project-id` | GCP project |
| `GCS_BUCKET` | `your-gcs-bucket` | Storage bucket |
| `BQ_DATASET` | `your-bq-dataset` | BigQuery dataset |

### Switching to Production Mode

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

Access at **http://localhost:8080** (credentials: airflow/airflow)

**Key pages:**
- **DAGs**: View and trigger pipelines
- **Graph**: Visualize task dependencies
- **Grid**: Monitor task status over time
- **Logs**: Debug task failures

### Command Line Operations

```bash
# List all DAGs
docker exec data-pipeline-airflow-scheduler-1 airflow dags list

# Trigger DAG
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline

# Check task status
docker exec data-pipeline-airflow-scheduler-1 airflow tasks states-for-dag-run \
  er_data_pipeline "manual__2026-02-20T22:10:47+00:00"

# View task logs
docker exec data-pipeline-airflow-scheduler-1 airflow tasks logs \
  er_data_pipeline load_data_task "manual__2026-02-20T22:10:47+00:00"
```

### Checking Outputs

```bash
# In Docker container
docker exec data-pipeline-airflow-scheduler-1 ls -la /opt/airflow/data/processed/

# Locally (mounted volume)
ls -la data/processed/
```

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
# Set your project ID
export PROJECT_ID="your-project-id"

# Create GCS bucket
gcloud storage buckets create gs://${PROJECT_ID}-entity-resolution \
  --project=${PROJECT_ID} \
  --location=us-central1

# Create BigQuery dataset
bq mk --dataset \
  --project_id=${PROJECT_ID} \
  --location=US \
  ${PROJECT_ID}:your-bq-dataset
```

### Step 2: Create Service Account and Download Key

```bash
# Create service account
gcloud iam service-accounts create entity-resolution-sa \
  --display-name="Entity Resolution Pipeline" \
  --project=${PROJECT_ID}

# Grant permissions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:entity-resolution-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:entity-resolution-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:entity-resolution-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Download key file
gcloud iam service-accounts keys create secrets/gcp-sa-key.json \
  --iam-account=entity-resolution-sa@${PROJECT_ID}.iam.gserviceaccount.com
```

### Step 3: Configure Environment Variables

Update the `.env` file with your GCP settings:

```bash
# .env file
# =============================================================================
# Airflow Docker Environment Variables
# =============================================================================

# Airflow UID (run: echo $(id -u) to get your user ID)
AIRFLOW_UID=50000

# Airflow Web UI Credentials
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# GCP Configuration
GCP_PROJECT_ID=your-project-id
GCS_BUCKET=your-project-id-entity-resolution
BQ_DATASET=your-bq-dataset

# Execution Mode: "local" (default) or "cloud"
EXECUTION_MODE=local
```

### Step 4: Place Service Account Key

Copy your service account key to the secrets directory:

```bash
cp /path/to/your/service-account-key.json secrets/gcp-sa-key.json
```

> **Security Note:** The `secrets/` directory is in `.gitignore` and will NOT be committed to git.

### Step 5: Restart Docker Containers

```bash
# Restart to pick up new environment variables and credentials
docker compose down
docker compose up -d

# Verify credentials are mounted
docker compose exec airflow-scheduler ls -la /opt/airflow/secrets/
# Should show: gcp-sa-key.json
```

### Step 6: Run Pipeline in Cloud Mode

**Option A: One-time cloud run (recommended for testing)**

```bash
# Trigger with cloud mode via DAG config
docker compose exec airflow-webserver airflow dags trigger er_data_pipeline \
  --conf '{"execution_mode": "cloud"}'
```

**Option B: Set cloud as default mode**

Edit `.env` file:
```bash
EXECUTION_MODE=cloud
```

Then restart and trigger normally:
```bash
docker compose restart airflow-webserver airflow-scheduler
docker compose exec airflow-webserver airflow dags trigger er_data_pipeline
```

### What Gets Uploaded

When running in cloud mode, the pipeline uploads:

| Destination | Files | Description |
|-------------|-------|-------------|
| **GCS** | `analytics/{date}/merged_all.csv` | 287K merged entity records |
| **GCS** | `analytics/{date}/merged_pairs.csv` | 60K training pairs |
| **GCS** | `training/{date}/{entity_type}/*.csv` | Train/val/test splits |
| **GCS** | `metrics/{date}/*.json` | Validation reports |
| **BigQuery** | `{dataset}.accounts` | Full accounts table |

### Verify Cloud Upload

**Check GCS:**
```bash
# List uploaded files
gsutil ls -r gs://${GCS_BUCKET}/

# Check file sizes
gsutil du -s gs://${GCS_BUCKET}/analytics/
gsutil du -s gs://${GCS_BUCKET}/training/
```

**Check BigQuery:**
```bash
# Query row count
bq query --project_id=${PROJECT_ID} \
  "SELECT COUNT(*) as total FROM ${BQ_DATASET}.accounts"

# Query by entity type
bq query --project_id=${PROJECT_ID} \
  "SELECT entity_type, COUNT(*) as count
   FROM ${BQ_DATASET}.accounts
   GROUP BY entity_type"
```

**Expected Results:**
```
+--------+
| total  |
+--------+
| 287330 |
+--------+

+-------------+--------+
| entity_type | count  |
+-------------+--------+
| PERSON      | 137330 |
| PRODUCT     | 100000 |
| PUBLICATION |  50000 |
+-------------+--------+
```

### Monitoring Cloud Tasks

In Airflow UI (http://localhost:8080), monitor these cloud-specific tasks:

| Task | Description | Duration |
|------|-------------|----------|
| `decide_execution_path` | Selects cloud or local path | ~1s |
| `upload_to_gcs` | Uploads 16 files to GCS | ~5-10s |
| `load_to_bigquery` | Loads accounts to BigQuery | ~5-10s |
| `verify_and_complete` | Verifies uploads succeeded | ~1s |

### Troubleshooting Cloud Integration

**Error: "The specified bucket does not exist"**
```bash
# Create the bucket
gcloud storage buckets create gs://${GCS_BUCKET} --project=${PROJECT_ID}
```

**Error: "Permission denied" on GCS**
```bash
# Verify service account has Storage Admin role
gcloud projects get-iam-policy ${PROJECT_ID} \
  --filter="bindings.members:entity-resolution-sa"
```

**Error: "Dataset not found" in BigQuery**
```bash
# Create the dataset
bq mk --dataset ${PROJECT_ID}:${BQ_DATASET}
```

**Error: "Invalid credentials"**
```bash
# Verify key file exists and is valid JSON
cat secrets/gcp-sa-key.json | jq '.client_email'

# Test authentication
docker compose exec airflow-scheduler \
  python -c "from google.cloud import storage; print(storage.Client().project)"
```

### Cost Considerations

| Resource | Estimated Cost | Notes |
|----------|---------------|-------|
| GCS Storage | ~$0.02/GB/month | ~100MB per run |
| BigQuery Storage | ~$0.02/GB/month | ~50MB per run |
| BigQuery Queries | $5/TB scanned | First 1TB free |
| **Total per run** | **< $0.01** | Minimal for dev/test |

> **Tip:** Use `EXECUTION_MODE=local` for development to avoid cloud costs.

---

## Data Outputs

### Output Files

| File | Location | Description |
|------|----------|-------------|
| `accounts.csv` | data/processed/ | Combined entity records |
| `er_pairs.csv` | data/processed/ | Training pairs with labels |
| `validation.json` | data/metrics/ | Raw data validation results |
| `schema_validation_results.json` | data/metrics/ | Schema validation report |
| `bias_report.json` | data/metrics/ | Bias detection analysis |

### Accounts Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | Unique entity identifier |
| `name` | string | Entity name (normalized) |
| `address` | string | Entity address/description |
| `dob` | string | Date of birth (persons) or null |
| `entity_type` | string | PERSON, PRODUCT, or PUBLICATION |
| `source_dataset` | string | Origin dataset name |
| `cluster_id` | string | Ground truth cluster for pairs |

### Pairs Schema

| Column | Type | Description |
|--------|------|-------------|
| `id1` | string | First entity ID |
| `id2` | string | Second entity ID |
| `name1` | string | First entity name |
| `name2` | string | Second entity name |
| `address1` | string | First entity address |
| `address2` | string | Second entity address |
| `label` | int | 1=match, 0=non-match |
| `entity_type` | string | Entity domain |
| `source_dataset` | string | Origin dataset |

### Using Data for Model Training

```python
import pandas as pd

# Load training pairs
pairs = pd.read_csv('data/processed/er_pairs.csv')

# Filter by entity type for domain-specific training
person_pairs = pairs[pairs['entity_type'] == 'PERSON']
product_pairs = pairs[pairs['entity_type'] == 'PRODUCT']

# Check label distribution
print(pairs['label'].value_counts())
```

---

## Validation & Quality

### Schema Validation

The pipeline validates data against these expectations:

**Accounts Validation:**
- Required columns exist (id, name, address)
- ID values are not null
- ID values are unique (within dataset)
- Name null rate < 5%
- Address null rate < 10%
- Name length between 1-500 characters
- Entity type in {PERSON, PRODUCT, PUBLICATION}

**Pairs Validation:**
- Required columns exist (id1, id2, label)
- No null values in ID columns
- Label values in {0, 1}
- Unique (id1, id2) pairs per dataset

### Viewing Validation Results

```bash
cat data/metrics/schema_validation_results.json | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f'Overall Success: {data[\"overall_success\"]}')
print(f'Success Rate: {data[\"summary\"][\"overall_success_rate\"]}%')
print(f'Entity Types: {data[\"multi_domain\"][\"entity_types\"]}')
"
```

### Bias Detection

The pipeline analyzes bias across multiple dimensions:

| Bias Type | What It Checks | Risk Threshold |
|-----------|----------------|----------------|
| Entity Type | Distribution across PERSON/PRODUCT/PUBLICATION | >3x imbalance |
| Language | Non-ASCII character distribution | >5% |
| Geographic | US vs international addresses | >80% single region |
| Data Source | Records per source dataset | >3x imbalance |
| Label | Positive vs negative pairs | >2x imbalance |

### Example Bias Report

```json
{
  "multi_domain_analysis": {
    "entity_type_distribution": {
      "PERSON": 10000,
      "PRODUCT": 10000,
      "PUBLICATION": 5000
    },
    "source_datasets_present": 5,
    "entity_type_imbalance_ratio": 2.0
  },
  "summary": {
    "overall_bias_risk": "MEDIUM",
    "total_issues": 2,
    "bias_issues": ["language_bias", "geographic_bias"]
  }
}
```

---

## Testing

### Running Unit Tests

```bash
cd Data-Pipeline

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_schema_validation.py -v

# Run with coverage report
pytest tests/ -v --cov=scripts --cov-report=html
open htmlcov/index.html
```

### Test Coverage

The project maintains comprehensive test coverage with automated reporting.

```bash
# Run tests with coverage
pytest tests/ --cov=scripts --cov-report=html --cov-report=term-missing --cov-config=setup.cfg

# View HTML report
open htmlcov/index.html
```

**Coverage Report (Latest):**

| Module | Statements | Missing | Branch | Coverage |
|--------|------------|---------|--------|----------|
| preprocessing.py | 288 | 34 | 110 | 87% |
| schema_validation.py | 211 | 59 | 58 | 71% |
| bias_detection.py | 218 | 76 | 90 | 62% |
| dataset_factory.py | 241 | 121 | 22 | 49% |
| **TOTAL** | **958** | **290** | **280** | **69%** |

**Coverage Configuration:** See `setup.cfg` and `pytest.ini` for coverage settings.

### Test Suite Summary

```
======================== test session starts ========================
collected 87 items

tests/test_bias_detection.py        16 passed
tests/test_preprocessing.py         24 passed
tests/test_schema_validation.py     47 passed
======================== 87 passed in 11.56s ========================
```

### Test Categories

| Category | Tests | Description |
|----------|-------|-------------|
| Schema Validation | 47 | Data quality expectations |
| Preprocessing | 24 | Data cleaning, normalization |
| Bias Detection | 16 | Entity type, language, geographic bias |

---

## Data Versioning (DVC)

### DVC Setup

DVC tracks large data files that shouldn't be stored in Git.

```bash
# Check DVC status
dvc status

# Pull tracked data from remote
dvc pull

# Push local data to remote
dvc push
```

### DVC Pipeline

```yaml
# dvc.yaml
stages:
  generate_data:
    cmd: python scripts/generate_test_data.py --dataset pseudopeople
    outs:
      - data/raw/pseudopeople.csv

  preprocess_data:
    cmd: python scripts/generate_test_data.py --dataset pseudopeople
    deps:
      - data/raw/pseudopeople.csv
    outs:
      - data/processed/pseudopeople_accounts.csv
      - data/processed/pseudopeople_pairs.csv

  validate_data:
    cmd: python scripts/schema_validation.py --accounts ... --pairs ...
    deps:
      - data/processed/pseudopeople_accounts.csv
    metrics:
      - data/metrics/schema_validation_results.json

  detect_bias:
    cmd: python scripts/bias_detection.py --accounts ... --pairs ...
    metrics:
      - data/metrics/bias_report.json
```

### Reproducing Pipeline

```bash
# Reproduce full pipeline
dvc repro

# Reproduce specific stage
dvc repro validate_data

# View pipeline DAG
dvc dag
```

---

## Troubleshooting

### Common Issues

#### Airflow containers won't start

```bash
# Check logs
docker compose logs airflow-scheduler

# Reset everything
docker compose down -v
docker compose up -d
```

#### DAG not appearing in Airflow UI

```bash
# Check for import errors
docker exec data-pipeline-airflow-scheduler-1 python -c "
import sys
sys.path.insert(0, '/opt/airflow/dags')
import airflow_data_dag
print('DAG loaded successfully')
"

# Force DAG refresh
docker exec data-pipeline-airflow-scheduler-1 airflow dags reserialize
```

#### Pipeline task failing

```bash
# Check task logs
docker exec data-pipeline-airflow-scheduler-1 airflow tasks logs \
  er_data_pipeline <task_id> <execution_date>

# Check container logs
docker logs data-pipeline-airflow-scheduler-1 2>&1 | tail -50
```

#### Out of disk space

```bash
# Clean Docker resources
docker system prune -a

# Remove old Airflow logs
rm -rf logs/*
```

### Resetting the Pipeline

```bash
# Stop all services
docker compose down

# Remove all data (start fresh)
rm -rf data/raw/* data/processed/* data/metrics/*

# Restart services
docker compose up -d
```

---

## Alerting & Monitoring

### Anomaly Detection Alerting

The pipeline includes an alerting module (`scripts/alerting.py`) that provides notifications for data anomalies and pipeline issues.

#### Supported Alert Channels

| Channel | Configuration | Use Case |
|---------|---------------|----------|
| **Console** | Always enabled | Development, debugging |
| **Email** | SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD | Team notifications |
| **Slack** | SLACK_WEBHOOK_URL | Real-time team alerts |

#### Alert Types

```python
from scripts.alerting import AlertManager

alert_manager = AlertManager()

# Data drift alert
alert_manager.alert_data_drift(
    metric_name="entity_type_distribution",
    drift_percentage=15.2,
    expected_value={"PERSON": 0.5, "PRODUCT": 0.3},
    actual_value={"PERSON": 0.35, "PRODUCT": 0.45},
    dataset="combined"
)

# Schema violation alert
alert_manager.alert_schema_violation(
    expectation="expect_column_values_to_not_be_null",
    column="account_id",
    failure_details={"null_count": 150},
    dataset="pseudopeople"
)

# Bias detection alert
alert_manager.alert_bias_detected(
    bias_type="geographic_bias",
    affected_groups=["NC", "CA"],
    disparity_ratio=0.45
)

# Pipeline failure alert
alert_manager.alert_pipeline_failure(
    task_name="data_transformation",
    error_message="Memory exceeded",
    pipeline_run_id="run_123"
)
```

#### Environment Variables

```bash
# Email configuration
export SMTP_HOST="smtp.gmail.com"
export SMTP_PORT="587"
export SMTP_USERNAME="your-email@gmail.com"
export SMTP_PASSWORD="your-app-password"
export ALERT_FROM_EMAIL="alerts@entity-resolution.io"
export ALERT_TO_EMAILS="team@example.com,lead@example.com"

# Slack configuration
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXX/YYY/ZZZ"
```

---

## Pipeline Optimization

For detailed performance analysis and optimization recommendations, see:

**[docs/pipeline_optimization.md](docs/pipeline_optimization.md)**

### Key Findings

| Bottleneck | Current Time | Optimization | Expected Improvement |
|------------|--------------|--------------|---------------------|
| Data Loading | 45-60s (35-40%) | Parallel loading | 40-60% reduction |
| Transformation | 30-45s (25-30%) | Per-entity parallelism | 50-70% reduction |
| Validation | 10-15s (8-10%) | Incremental validation | 50% reduction |

### Quick Optimization Wins

1. **Enable caching** for external datasets
2. **Parallel dataset loading** with ThreadPoolExecutor
3. **Reduce logging verbosity** in tight loops

---

## Development Guide

### Adding a New Dataset

1. **Create handler in `dataset_factory.py`:**

```python
class NewDatasetHandler(DatasetHandler):
    def download(self) -> pd.DataFrame:
        # Download or generate data
        pass

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        # Map to standard schema: id, name, address
        return df[['id', 'name', 'address']]
```

2. **Register in factory:**

```python
def get_dataset_handler(dataset_name: str, config: Dict):
    handlers = {
        ...
        'new_dataset': NewDatasetHandler,
    }
```

3. **Add to `config/datasets.yaml`:**

```yaml
active_datasets:
  - "new_dataset"

datasets:
  new_dataset:
    name: "New Dataset"
    entity_type: "PERSON"
    base_records: 500
    target_records: 5000
```

### Modifying Preprocessing

Edit `scripts/preprocessing.py`:

```python
def preprocess_dataset(df: pd.DataFrame, config: Dict):
    # Add custom normalization
    df['name'] = df['name'].apply(custom_normalize)

    # Add custom corruption
    df = apply_custom_corruption(df, config)

    return accounts_df, pairs_df
```

### Extending Validation

Edit `scripts/schema_validation.py`:

```python
def validate_accounts(self, df: pd.DataFrame) -> Dict:
    # Add custom expectation
    exp_result = {
        "expectation": "expect_custom_rule",
        "success": custom_check(df),
        "details": {...}
    }
    results["expectations"].append(exp_result)
```

---

## Team & Acknowledgments

### Team

**Northeastern University - MLOps Course (Spring 2026)**

- Data Pipeline Development
- Multi-domain Entity Resolution
- Cloud-native ML Systems

### Dataset Sources

| Dataset | Source | License |
|---------|--------|---------|
| Pseudopeople | IHME | MIT |
| NC Voters | Leipzig DB Group | Research |
| OFAC SDN | US Treasury | Public Domain |
| WDC Products | Web Data Commons | CC BY-SA |
| Amazon 2018 | UCSD/McAuley | Research |
| DBLP-ACM | Leipzig DB Group | Research |

### References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [DVC Documentation](https://dvc.org/doc)
- [Entity Resolution Survey](https://arxiv.org/abs/2101.06427)
- [Leipzig ER Benchmarks](https://dbs.uni-leipzig.de/research/projects/object_matching/)

---

## License

This project is developed for educational purposes as part of the MLOps course at Northeastern University.

---

**Questions?** Open an issue on GitHub or contact the team.
