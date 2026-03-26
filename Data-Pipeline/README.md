# Entity Resolution Data Pipeline

Data pipeline for PERSON entity resolution using Apache Airflow, DVC, and GCP. Processes 3 datasets (Pseudopeople, NC Voters, OFAC SDN) to generate training data for a deBERTa + LoRA entity matching model.

## Pipeline Flow

```
Load (3 parallel) → Validate Raw → Transform (3 parallel) → Organize
→ [Schema Validation | Bias Detection | Split Validation] (parallel)
→ Quality Gate → DVC Versioning → GCS Upload
```

**Outputs:**
- `training/` — train.csv, val.csv, test.csv (70/15/15 stratified splits)
- `analytics/` — merged data for bias analysis
- `metrics/` — bias report, schema validation, quality gate results

## Prerequisites

- Docker & Docker Compose
- Git
- GCP account (for cloud upload)

## Setup

```bash
git clone https://github.com/Entity-Resolution-Organization/Entity-Resolution.git
cd Entity-Resolution/Data-Pipeline
```

Create `.env` from the example:

```bash
cp .env.example .env
```

Edit `.env` with your values:

```dotenv
AIRFLOW_UID=50000          # Run: echo $(id -u)
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=changeme
GCP_PROJECT_ID=your-project-id
GCS_BUCKET=your-bucket
DVC_GCS_BUCKET=your-dvc-bucket
ENVIRONMENT=local          # "local" or "production"
```

Start Airflow:

```bash
docker compose up -d
```

Wait ~30 seconds, then verify:

```bash
docker compose ps
```

## Running the Pipeline

**Airflow UI:** http://localhost:8080 — find `er_data_pipeline`, click ▶

**CLI:**

```bash
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

**Check status:**

```bash
docker exec data-pipeline-airflow-scheduler-1 airflow dags list-runs -d er_data_pipeline --limit 5
```

**View task logs:**

```bash
docker exec data-pipeline-airflow-scheduler-1 airflow tasks logs er_data_pipeline <task_id> <run_id>
```

## Outputs

### Local (data/)

| Directory | Contents |
|-----------|----------|
| `raw/` | Per-dataset downloaded/generated CSVs |
| `processed/` | Per-dataset accounts + pairs after transform |
| `analytics/` | All datasets merged (bias analysis input) |
| `training/` | train.csv, val.csv, test.csv + metadata.json |
| `metrics/` | Validation and bias report JSONs |

### GCS (after successful run)

```
gs://{bucket}/
├── analytics/
│   ├── merged_all.csv
│   └── merged_pairs.csv
├── training/
│   ├── train.csv
│   ├── val.csv
│   ├── test.csv
│   └── metadata.json
└── metrics/
    ├── bias_report.json
    ├── quality_gate.json
    ├── schema_validation.json
    └── training_split_validation.json
```

Each run overwrites GCS. DVC handles versioning.

## Configuration

### Datasets (config/datasets.yaml)

```yaml
active_datasets:
  - "pseudopeople"
  - "nc_voters"
  - "ofac_sdn"

datasets:
  pseudopeople:
    name: "Pseudopeople Synthetic Population"
    entity_type: "PERSON"
    base_records: 15000       # Raw records generated
    target_records: 150000    # After expansion with corruption
    corruption:
      rate: 0.25              # 25% of fields corrupted per variant
    pairs:
      positive: 40000         # Matching pairs
      negative: 40000         # Non-matching pairs
```

### Scaling

| Use Case | base_records | target_records | pairs (pos+neg) |
|----------|-------------|----------------|-----------------|
| Quick test | 100 | 1,000 | 200 |
| Development | 1,000 | 10,000 | 2,000 |
| Production | 15,000 | 150,000 | 80,000 |

## Quality Gates

The pipeline blocks upload if any critical check fails:

**Schema Validation** — required columns, null thresholds (id: 0%, name: <5%, address: <10%), ID uniqueness, binary labels

**Training Split Validation** — 70/15/15 ratio tolerance (±5%), no data leakage between splits, label balance (≥40% minority class)

**Bias Detection** — language bias (<5% non-ASCII = HIGH), geographic bias (>80% US = HIGH), label imbalance (>15% deviation = flagged), synthetic data ratio (>70% = HIGH)

**Quality Gate Decision:**
- **GO** — all checks pass
- **GO-WITH-WARNINGS** — non-critical issues (e.g. high bias risk)
- **NO-GO** — critical failures block DVC and GCS upload

## Testing

```bash
# Run all tests
cd Data-Pipeline
pytest tests/ -v

# With coverage
pytest tests/ --cov=scripts --cov-report=term-missing --cov-config=setup.cfg
```

CI runs automatically on push to `main` or `dev` via GitHub Actions.

## DVC

Data is versioned automatically on each pipeline run. DVC pointer files (`.dvc`) are committed to Git; actual data is pushed to GCS.

```bash
# Check what's changed
dvc status

# Pull data from remote
dvc pull

# Rollback to previous version
git checkout <commit> -- data/*.dvc
dvc checkout
```

## GCP VM Deployment

On a GCP VM, auth happens via the VM's attached service account — no key files needed.

```bash
# Set environment to production (handlers download real data)
# In .env:
ENVIRONMENT=production

# Start
docker compose up -d

# Trigger
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

## Common Issues

**DAG not visible in Airflow UI:**

```bash
docker exec data-pipeline-airflow-scheduler-1 python -c \
  "import sys; sys.path.insert(0, '/opt/airflow/dags'); import airflow_data_dag"
```

**GCS upload fails:**

```bash
# Verify GCP auth inside container
docker exec data-pipeline-airflow-scheduler-1 python -c \
  "from google.cloud import storage; print(storage.Client().project)"
```

**Clear data for fresh run:**

```bash
rm -rf data/raw/* data/processed/* data/training/* data/analytics/* data/metrics/*
```

**Rebuild after Dockerfile changes:**

```bash
docker compose up -d --build
```

## Project Structure

```
Data-Pipeline/
├── config/
│   └── datasets.yaml          # Dataset configuration
├── dags/
│   └── airflow_data_dag.py    # Airflow DAG definition
├── scripts/
│   ├── bias_detection.py      # Bias analysis
│   ├── data_validation.py     # Raw + split validation, quality gate
│   ├── dataset_factory.py     # Dataset download/generation handlers
│   ├── preprocessing.py       # Normalization, corruption, pair generation
│   └── schema_validation.py   # Schema checks for accounts + pairs
├── tests/
│   ├── test_bias_detection.py
│   ├── test_dataset_factory.py
│   ├── test_preprocessing.py
│   └── test_schema_validation.py
├── data/                       # Pipeline outputs (gitignored, DVC tracked)
├── logs/                       # Airflow logs (gitignored)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── setup.cfg
├── pytest.ini
├── .env.example
└── README.md
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow 2.8.1 |
| Data Versioning | DVC 3.x with GCS remote |
| Cloud Storage | Google Cloud Storage |
| Containerization | Docker Compose |
| CI/CD | GitHub Actions |
| Python | 3.10 |