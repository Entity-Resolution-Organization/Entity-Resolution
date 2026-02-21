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

### Use Cases

| Domain | Application |
|--------|-------------|
| **Financial Crime** | Fraud ring detection through resolved entity graphs |
| **Customer 360** | Unified customer view across touchpoints |
| **KYC/AML** | Know Your Customer compliance and sanctions screening |
| **Supplier Consolidation** | Vendor deduplication for procurement |
| **Catalog Matching** | Product matching across retailers |
| **Author Disambiguation** | Research publication linking |

---

## Repository Structure

```
Entity-Resolution/
├── Data-Pipeline/              # Data processing pipeline
│   ├── dags/                   # Airflow DAG definitions
│   ├── scripts/                # Processing scripts
│   ├── config/                 # Dataset configurations
│   ├── tests/                  # Unit tests
│   ├── schema_validation/      # Validation expectations
│   ├── data/                   # Data directory (DVC tracked)
│   ├── docker-compose.yml      # Airflow stack
│   ├── dvc.yaml               # DVC pipeline definition
│   └── README.md              # Detailed pipeline documentation
│
├── Model-Training/            # ML model training (future)
│   └── ...
│
├── Inference-Service/         # Production inference (future)
│   └── ...
│
└── README.md                  # This file
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
| **6 Dataset Handlers** | Pseudopeople, NC Voters, OFAC, WDC, Amazon, DBLP-ACM |
| **Schema Validation** | Data quality expectations with detailed reporting |
| **Bias Detection** | Entity type, language, geographic bias analysis |
| **DVC Integration** | Data versioning and reproducibility |
| **Cloud-Ready** | GCS and BigQuery integration |

### Output

| Metric | Value |
|--------|-------|
| Total Accounts | ~25,000 |
| Total Pairs | ~5,000 |
| Entity Types | 3 (PERSON, PRODUCT, PUBLICATION) |
| Source Datasets | 5-6 |

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
┌─────────────────────────────────────────────────────────────────┐
│                    ENTITY RESOLUTION SYSTEM                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│   │   Person    │    │   Product   │    │ Publication │        │
│   │  Datasets   │    │  Datasets   │    │  Datasets   │        │
│   │  (3 srcs)   │    │  (2 srcs)   │    │  (1 src)    │        │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘        │
│          │                  │                  │                │
│          └──────────────────┼──────────────────┘                │
│                             ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │              DATA PIPELINE (Airflow)                     │  │
│   │  Load → Validate → Transform → Schema Check → Bias Check │  │
│   └─────────────────────────────────────────────────────────┘  │
│                             │                                   │
│          ┌──────────────────┼──────────────────┐               │
│          ▼                  ▼                  ▼               │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐       │
│   │   Local     │    │    GCS      │    │  BigQuery   │       │
│   │   Files     │    │   Bucket    │    │   Tables    │       │
│   └─────────────┘    └─────────────┘    └─────────────┘       │
│                             │                                   │
│                             ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │           MODEL TRAINING (Vertex AI) - Future            │  │
│   │         LoRA Adapters per Entity Domain                  │  │
│   └─────────────────────────────────────────────────────────┘  │
│                             │                                   │
│                             ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │          INFERENCE SERVICE (Cloud Run) - Future          │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

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
