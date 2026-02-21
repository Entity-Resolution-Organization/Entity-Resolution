# Entity Resolution

A cloud-native ML system for entity resolution operating on Google Cloud Platform (BigQuery and Vertex AI). The system identifies and links records referring to the same real-world entities across multiple data sources, supporting person, product, and publication domains.

---

## Overview

Enterprise data warehouses accumulate millions of identity records with inconsistent identifiers - customer names with typos, suppliers with changing addresses, and individuals with evolving affiliations. Traditional fuzzy matching systems require constant manual tuning and fail when encountering novel variations.

This project presents a production-grade entity resolution system that:

- **Processes multi-domain data** across person, product, and publication entities
- **Handles temporal evolution** through incremental learning from user corrections
- **Detects data drift** and triggers automated retraining when quality degrades
- **Scales to millions of records** using BigQuery and Vertex AI infrastructure


---

## Repository Structure

```
Entity-Resolution/
├── Data-Pipeline/              # Data processing pipeline
│   ├── dags/                   # Airflow DAG definitions (32 tasks)
│   ├── scripts/                # Processing modules
│   │   ├── dataset_factory.py  # Multi-domain dataset handlers
│   │   ├── preprocessing.py    # Data transformation
│   │   ├── data_validation.py  # Validation gates
│   │   ├── schema_validation.py # Schema checks
│   │   ├── bias_detection.py   # Bias analysis
│   │   └── alerting.py         # Alert system
│   ├── config/                 # Dataset configurations
│   ├── tests/                  # Unit tests (107 tests)
│   ├── schema_validation/      # Great Expectations config
│   ├── data/                   # Data directory (DVC tracked)
│   │   ├── raw/                # Source datasets
│   │   ├── processed/          # Transformed data
│   │   ├── training/           # ML training splits
│   │   └── metrics/            # Validation reports
│   ├── secrets/                # GCP credentials (gitignored)
│   ├── logs/                   # Airflow logs
│   ├── docker-compose.yml      # Airflow stack
│   ├── dvc.yaml                # DVC pipeline definition
│   └── README.md               # Detailed pipeline documentation
│
├── Model-Training/             # ML model training (future)
│   └── ...
│
├── Inference-Service/          # Production inference (future)
│   └── ...
│
└── README.md                   # This file
```

---

## Data Pipeline

The data pipeline processes 6 datasets across 3 entity types to generate training data for ML models.

### Quick Start

```bash
cd Data-Pipeline

# Start Airflow
docker compose up -d

# Access UI at http://localhost:8080 (airflow/airflow)

# Trigger pipeline
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline
```

### Features

| Feature | Description |
|---------|-------------|
| **Multi-Domain Processing** | Person, Product, Publication entities |
| **6 Dataset Handlers** | Pseudopeople, NC Voters, OFAC SDN, WDC Products, Amazon, DBLP-ACM |
| **Per-Dataset Validation** | Quality gates after each dataset load |
| **Schema Validation** | Data quality expectations with detailed reporting |
| **Training Split Validation** | 70/15/15 splits with leakage detection |
| **Quality Gate** | Go/no-go decision based on all validations |
| **Bias Detection** | Entity type, language, geographic, data source bias |
| **DVC Integration** | Data versioning and reproducibility |
| **Cloud-Ready** | Conditional GCS upload and BigQuery loading |

### Output

| Metric | Value |
|--------|-------|
| Total Accounts | ~287,000 |
| Training Pairs | 55,000 |
| Entity Types | 3 (PERSON, PRODUCT, PUBLICATION) |
| Source Datasets | 6 |
| DAG Tasks | 32 |
| Unit Tests | 107 |

**[See Full Documentation: Data-Pipeline/README.md](Data-Pipeline/README.md)**

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| **Orchestration** | Apache Airflow 2.8.1 |
| **Data Versioning** | DVC 3.x |
| **Validation** | Schema Validator (custom) |
| **Cloud Storage** | Google Cloud Storage |
| **Data Warehouse** | BigQuery |
| **ML Platform** | Vertex AI (planned) |
| **Containerization** | Docker Compose |

---

## Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                      ENTITY RESOLUTION SYSTEM                         │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐             │
│   │   Person    │    │   Product   │    │ Publication │             │
│   │  Datasets   │    │  Datasets   │    │  Datasets   │             │
│   │  (3 srcs)   │    │  (2 srcs)   │    │  (1 src)    │             │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘             │
│          │                  │                  │                     │
│          └──────────────────┼──────────────────┘                     │
│                             ▼                                        │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │                 DATA PIPELINE (32 Airflow Tasks)              │  │
│   │                                                               │  │
│   │  ┌────────┐   ┌──────────┐   ┌───────────┐   ┌─────────┐    │  │
│   │  │  Load  │ → │ Validate │ → │ Transform │ → │  Merge  │    │  │
│   │  │(6 tasks)│   │ (6 tasks)│   │ (6 tasks) │   │         │    │  │
│   │  └────────┘   └──────────┘   └───────────┘   └────┬────┘    │  │
│   │                                                    │         │  │
│   │                    ┌───────────────────────────────┘         │  │
│   │                    ▼                                         │  │
│   │  ┌─────────────────────────────────────────────────────┐    │  │
│   │  │              VALIDATION PHASE                        │    │  │
│   │  │  Schema Check │ Training Splits │ Bias Detection    │    │  │
│   │  └─────────────────────────┬───────────────────────────┘    │  │
│   │                            ▼                                 │  │
│   │  ┌─────────────────────────────────────────────────────┐    │  │
│   │  │              QUALITY GATE (Go/No-Go)                 │    │  │
│   │  └─────────────────────────┬───────────────────────────┘    │  │
│   │                            │                                 │  │
│   └────────────────────────────┼─────────────────────────────────┘  │
│                                │                                     │
│              ┌─────────────────┼─────────────────┐                  │
│              ▼                 ▼                 ▼                  │
│   ┌─────────────────┐  ┌─────────────┐  ┌─────────────┐            │
│   │  Local Files    │  │    GCS      │  │  BigQuery   │            │
│   │  (training/)    │  │   Bucket    │  │   Tables    │            │
│   └─────────────────┘  └─────────────┘  └─────────────┘            │
│                                │                                     │
│                                ▼                                     │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │            MODEL TRAINING (Vertex AI) - Future                │  │
│   │          LoRA Adapters per Entity Domain                      │  │
│   └──────────────────────────────────────────────────────────────┘  │
│                                │                                     │
│                                ▼                                     │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │           INFERENCE SERVICE (Cloud Run) - Future              │  │
│   └──────────────────────────────────────────────────────────────┘  │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### Training Data Output

| Entity Type | Train | Val | Test | Total |
|-------------|-------|-----|------|-------|
| **PERSON** | 17,500 | 3,750 | 3,750 | 25,000 |
| **PRODUCT** | 14,000 | 3,000 | 3,000 | 20,000 |
| **PUBLICATION** | 7,000 | 1,500 | 1,500 | 10,000 |

---

## Getting Started

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Git
- (Optional) GCP account for cloud mode

### Installation

```bash
# Clone repository
git clone https://github.com/Entity-Resolution-Organization/Entity-Resolution.git
cd Entity-Resolution

# Set up data pipeline
cd Data-Pipeline
pip install -r requirements.txt

# Start services
docker compose up -d
```

### Running the Pipeline

1. Access Airflow UI: http://localhost:8080
2. Login: airflow / airflow
3. Find `er_data_pipeline` and trigger it
4. View results in `data/processed/`

---

## Documentation

| Document | Description |
|----------|-------------|
| [Data-Pipeline/README.md](Data-Pipeline/README.md) | Complete pipeline documentation |
| [Data-Pipeline/config/datasets.yaml](Data-Pipeline/config/datasets.yaml) | Dataset configuration |
| [Data-Pipeline/dvc.yaml](Data-Pipeline/dvc.yaml) | DVC pipeline definition |

---

## Team

**Northeastern University - MLOps Course (Spring 2026)**

---

## License

This project is developed for educational purposes as part of the MLOps course at Northeastern University.
