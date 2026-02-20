# Entity Resolution Data Pipeline

Data pipeline for entity resolution using Apache Airflow, DVC, and Google Cloud Platform.

## Overview

This pipeline processes entity data (persons, products, publications) for entity resolution/deduplication tasks. It generates training pairs (matching/non-matching) for ML model training.

## Project Structure

```
Data-Pipeline/
├── config/
│   ├── airflow.cfg          # Airflow configuration
│   └── datasets.yaml        # Dataset configurations (6 datasets)
├── dags/
│   └── airflow_data_dag.py  # Main Airflow DAG
├── scripts/
│   ├── dataset_factory.py   # Dataset handlers (factory pattern)
│   ├── preprocessing.py     # Normalization, corruption, pair generation
│   ├── generate_test_data.py # Generate test data for DVC tracking
│   └── utils.py
├── tests/
│   ├── test_preprocessing.py
│   └── test_dataset_factory.py
├── data/
│   ├── raw/                 # Raw downloaded data (DVC tracked)
│   ├── processed/           # Processed accounts + pairs (DVC tracked)
│   └── metrics/             # Pipeline metrics
├── logs/                    # Airflow logs
├── dvc.yaml                 # DVC pipeline definition
├── docker-compose.yaml      # Airflow stack (CeleryExecutor)
├── Dockerfile
└── requirements.txt
```

## Supported Datasets

| Dataset | Type | Description |
|---------|------|-------------|
| Pseudopeople | Person | Synthetic population data |
| NC Voters | Person | North Carolina voter registry |
| OFAC SDN | Person | US Treasury sanctions list |
| WDC Products | Product | Web Data Commons product corpus |
| Amazon 2018 | Product | Amazon co-purchase network |
| DBLP-ACM | Publication | Author disambiguation benchmark |

## Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- DVC

### Installation

```bash
cd Data-Pipeline

# Install dependencies
pip install -r requirements.txt

# Initialize DVC (already done)
dvc pull  # Pull tracked data
```

### Generate Test Data

```bash
# Generate single dataset
python scripts/generate_test_data.py --dataset pseudopeople

# Generate all datasets
python scripts/generate_test_data.py --all
```

### Run Airflow (Docker)

```bash
# Start Airflow stack
docker-compose up -d

# Access UI
open http://localhost:8080
# Login: airflow / airflow
```

## DVC Pipeline

### Pipeline Stages

```
generate_data → preprocess_data → validate_data → detect_bias
```

### Commands

```bash
dvc status       # Check pipeline status
dvc repro        # Reproduce full pipeline
dvc push         # Push data to remote
dvc pull         # Pull data from remote
dvc dag          # Visualize pipeline DAG
```

### Tracked Data

| File | Records | Description |
|------|---------|-------------|
| `data/raw/pseudopeople.csv` | 1000 | Raw synthetic person data |
| `data/processed/pseudopeople_accounts.csv` | 500 | Expanded + corrupted entities |
| `data/processed/pseudopeople_pairs.csv` | 100 | Training pairs (match/non-match) |

## Data Processing Pipeline

### 1. Data Acquisition (`dataset_factory.py`)

- Factory pattern with 6 dataset handlers
- Production mode: Downloads from external sources
- Local mode: Generates synthetic test data
- Retry logic with exponential backoff

### 2. Preprocessing (`preprocessing.py`)

**Normalization:**
- Name normalization (title case, whitespace)
- Address standardization (abbreviation expansion)
- Date format standardization

**Corruption (for training data augmentation):**
- Name variants: nicknames, swaps, middle initials, typos
- Address variants: abbreviations, typos, reordering

**Pair Generation:**
- Positive pairs: Same cluster_id (matching entities)
- Negative pairs: Different cluster_id (non-matching)
- Balanced sampling

### 3. Output Schema

**accounts.csv:**
```
id, name, address, dob, cluster_id
```

**pairs.csv:**
```
id1, id2, name1, name2, address1, address2, label
```
- `label=1`: Match (same entity)
- `label=0`: Non-match (different entities)

## Airflow DAG

**DAG ID:** `laundrograph_data_pipeline`

**Tasks:**
1. `load_data_task` - Download and normalize raw data
2. `data_validation_task` - Schema and quality validation
3. `data_transformation_task` - Preprocessing pipeline
4. `load_accounts_bigquery` - Load to BigQuery (parallel)
5. `load_pairs_bigquery` - Load to BigQuery (parallel)

## Testing

```bash
cd Data-Pipeline

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=scripts --cov-report=html
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `local` | `local` or `production` |
| `GCP_PROJECT_ID` | `project-id` | GCP project ID |
| `GCS_BUCKET` | `laundrograph-data` | GCS bucket name |
| `BQ_DATASET` | `laundrograph` | BigQuery dataset |

### Dataset Configuration

Edit `config/datasets.yaml` to configure:
- Target record counts
- Corruption rates
- Pair generation settings
- BigQuery table mappings

## Team

Northeastern University - MLOps Course (Spring 2026)
