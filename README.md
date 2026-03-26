# Entity Resolution

ML system for PERSON entity resolution on Google Cloud Platform. Identifies and links records referring to the same real-world person across multiple data sources using a deBERTa + LoRA model.

## Repository Structure

```
Entity-Resolution/
├── Initial-Setup/              # GCP infrastructure (Terraform)
├── Data-Pipeline/              # Airflow data processing pipeline
│   ├── dags/                   # DAG definition
│   ├── scripts/                # Processing modules
│   ├── tests/                  # Unit tests
│   ├── config/                 # Dataset configuration
│   ├── data/                   # Pipeline outputs (DVC tracked)
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── README.md
├── Model-Pipeline/             # deBERTa + LoRA training (future)
├── Monitoring-Pipeline/        # Drift detection, alerting (future)
├── requirements-dev.txt        # Shared dev dependencies
├── run_data_pipeline.sh        # Pipeline trigger + DVC commit script
└── README.md
```

## Getting Started

### 1. Infrastructure

Provision GCP resources (VM, buckets, service account) using Terraform.

See [Initial-Setup/README.md](Initial-Setup/README.md)

### 2. Data Pipeline

Process 3 PERSON datasets → generate training pairs → validate → upload to GCS.

See [Data-Pipeline/README.md](Data-Pipeline/README.md)

```bash
cd Data-Pipeline
cp .env.example .env        # Edit with your GCP values
docker compose up -d
# Trigger via UI at http://localhost:8080 or CLI:
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

Or from the repo root:

```bash
./run_data_pipeline.sh      # Triggers DAG, waits, commits DVC files
```

## Pipeline Overview

```
3 PERSON Datasets (Pseudopeople, NC Voters, OFAC SDN)
    │
    ▼
Load (parallel) → Validate Raw → Transform (parallel) → Organize
    │
    ▼
Schema Validation ─┐
Bias Detection ────┼─→ Quality Gate → DVC Version → GCS Upload
Split Validation ──┘
```

**Outputs:**

| Path | Contents |
|------|----------|
| `training/` | train.csv, val.csv, test.csv (70/15/15 splits) |
| `analytics/` | Merged data for bias analysis |
| `metrics/` | Bias report, schema validation, quality gate results |

**Scale:** 450K accounts, 240K training pairs across 3 datasets.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Infrastructure | Terraform, GCP |
| Orchestration | Apache Airflow 2.8.1 |
| Data Versioning | DVC 3.x + GCS remote |
| Cloud Storage | Google Cloud Storage |
| Containerization | Docker Compose |
| CI/CD | GitHub Actions |
| Python | 3.10 |

## Documentation

| Document | Description |
|----------|-------------|
| [Initial_Setup/README.md](Initial_Setup/README.md) | GCP infrastructure provisioning |
| [Data-Pipeline/README.md](Data-Pipeline/README.md) | Pipeline setup, configuration, and usage |
| [Data-Pipeline/config/datasets.yaml](Data-Pipeline/config/datasets.yaml) | Dataset configuration |

## Team

Northeastern University — MLOps Course (Spring 2026)