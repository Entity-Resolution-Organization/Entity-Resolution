# Inference Pipeline

Entity Resolution inference service with a FastAPI backend and React frontend, powered by a DeBERTa-v3 + LoRA model on Google Cloud Vertex AI.

## Architecture

```
Inference-Pipeline/
├── scripts/
│   ├── app.py                # FastAPI backend (port 8000)
│   ├── model_client.py       # VertexAIClient + MockClient
│   ├── preprocess.py         # Name/address normalization
│   ├── build_graph.py        # Stage 2: blocking + scoring + enrichment + veto
│   ├── write_clusters.py     # Stage 2: connected components + BigQuery write
│   ├── score_network.py      # Stage 2: contextual risk scoring (Quantexa-style)
│   ├── pipeline_runner.py    # Orchestrates build_graph → write_clusters → score_network
│   ├── demo_data.py          # Synthetic demo dataset generator (25 records, 5 persons)
│   ├── demo_app.py           # Streamlit fallback UI (port 8501)
│   └── batch_inference.py    # GCS batch processor
├── frontend/                 # React SPA
│   ├── src/
│   │   ├── pages/            # Home, Resolve, Batch, Clusters, Customer360, VetoDemo, KYC, Fraud, Analytics, Pipeline
│   │   ├── components/       # Layout, DecisionBadge, ProbGauge, SimBars, EntityCard, etc.
│   │   └── api/client.js     # Axios wrapper for FastAPI endpoints
│   ├── vite.config.js        # Vite + Tailwind v4, proxy to port 8000
│   └── package.json
├── config/
│   ├── inference_config.yaml # Thresholds, GCS paths, mock toggle
│   └── training_config.yaml  # Graph, BigQuery, field_rules, network scoring config
├── dags/
│   └── airflow_inference_dag.py
├── tests/
├── Dockerfile
├── Dockerfile.graph          # Lightweight image for graph pipeline (no PyTorch)
├── docker-compose.yml
└── requirements.txt
```

## Quick Start

### Prerequisites

- Python 3.10+
- Node.js 18+
- GCP credentials with Vertex AI access (or use MockClient for local dev)

### 1. Backend (FastAPI)

```bash
cd Inference-Pipeline

# With real Vertex AI model:
export USE_MOCK_CLIENT=false
export GCP_PROJECT_ID=entity-resolution-487121
export GCP_BUCKET_NAME=entity-resolution-bucket-1
export GOOGLE_APPLICATION_CREDENTIALS=../Data-Pipeline/secrets/gcp-sa-key.json

python3 -m uvicorn scripts.app:app --host 0.0.0.0 --port 8000

# Or with mock client (no GCP needed):
USE_MOCK_CLIENT=true python3 -m uvicorn scripts.app:app --port 8000
```

### 2. Frontend (React)

```bash
cd Inference-Pipeline/frontend
npm install
npm run dev
# Opens at http://localhost:5173 (proxies API calls to port 8000)
```

### 3. Run the Graph Pipeline (Stage 2)

```bash
# Generate demo data and upload to GCS
python scripts/demo_data.py --upload

# Run build_graph (uses online endpoint for <1000 pairs, batch for larger)
MODEL_RESOURCE_NAME=projects/756491711716/locations/us-central1/models/3368374762412703744 \
CONFIG_PATH=../Model-Pipeline/config/training_config.yaml \
RECORDS_GCS_PATH=gs://entity-resolution-bucket-1/to-process/demo_records.csv \
USE_MOCK_CLIENT=true \
PYTHONPATH=. \
python scripts/build_graph.py

# Run write_clusters
CONFIG_PATH=../Model-Pipeline/config/training_config.yaml \
JOB_SUFFIX=demo_records \
MLFLOW_TRACKING_URI=file:///tmp/mlflow_local \
python scripts/write_clusters.py

# Run score_network
CONFIG_PATH=../Model-Pipeline/config/training_config.yaml \
JOB_SUFFIX=demo_records \
MLFLOW_TRACKING_URI=file:///tmp/mlflow_local \
python scripts/score_network.py
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/resolve` | Single pair resolution: `{name1, address1, name2, address2}` |
| POST | `/resolve/batch` | Batch resolution: `{pairs: [...]}` |
| POST | `/search` | Entity search against analytics dataset |
| POST | `/unify/upload` | Upload records CSV for full graph pipeline |
| GET | `/unify/status/{job_id}` | Poll pipeline job status |
| GET | `/unify/download/{job_id}` | Download unified CSV with cluster assignments |
| GET | `/unify/jobs` | List all pipeline jobs |
| GET | `/health` | API + endpoint connectivity status |
| GET | `/metrics/pipeline` | Quality gate, bias report, model metrics |
| GET | `/metrics/inference` | Runtime request count, latency, decision distribution |

### Example — Pairwise Resolution

```bash
curl -X POST http://localhost:8000/resolve \
  -H "Content-Type: application/json" \
  -d '{"name1":"Robert Smith","address1":"123 Main St","name2":"Bob Smith","address2":"123 Main Street"}'
```

## Stage 2: Graph and Contextual Engine

Pipeline: `build_graph.py` → `write_clusters.py` → `score_network.py`

### build_graph.py

Takes a records CSV and produces a scored edge graph:

- **Blocking** — name prefix, surname, street number, email username, phone suffix, DOB year
- **Scoring** — online endpoint for small jobs (<1000 pairs), Vertex AI Batch Prediction for large jobs
- **Enrichment** — field rules from config: DOB (exact, veto on mismatch), email (domain-aware), phone (digit suffix), company (Jaro-Winkler)
- **Veto logic** — hard rules override ML score (e.g., DOB present on both records and mismatched)

Output: `enriched_edges.csv` + `node_features.csv` in GCS.

### write_clusters.py

- Excludes vetoed edges, applies threshold (0.45), runs connected components (NetworkX)
- Stable cluster IDs (SHA256 of sorted member IDs)
- Writes to GCS + BigQuery (`entity_clusters`, `cluster_edges` tables)
- Produces unified CSV: original records + cluster_id + confidence

### score_network.py

Contextual risk scoring (Quantexa-style). Four scores, all configurable:

| Score | Weight | Signal |
|-------|--------|--------|
| Cluster risk | 0.30 | Large cluster + low confidence = suspicious |
| Bad neighbour | 0.40 | 2-hop connection to flagged entity |
| Shared field | 0.20 | Same email/phone across different names in cluster |
| Centrality | 0.10 | Hub entity across clusters |

Output: `network_scores` table in BigQuery.

## Frontend Pages

| Page | Route | Description |
|------|-------|-------------|
| Home | `/` | Landing page with architecture diagram and stats |
| Resolve | `/resolve` | Pairwise matching with field attribution |
| Batch | `/batch` | Pairwise CSV batch + Unify records (upload → pipeline → download) |
| Clusters | `/clusters` | Transitive cluster viewer with force-directed graph |
| Customer 360 | `/customer360` | Unified entity view with golden record + source breakdown |
| Veto Demo | `/veto` | Toggle DOB veto on/off, see edge split in real-time |
| KYC | `/kyc` | 2-hop bad neighbour analysis with risk score breakdown |
| Fraud | `/fraud` | Fraud ring detection — shared fields across different identities |
| Analytics | `/analytics` | Cluster dashboard: KPIs, size distribution, risk review queue |
| Pipeline | `/pipeline` | F1/Precision/Recall/AUC metrics from GCS |

## Demo Data

`demo_data.py` generates 25 synthetic records (5 real people + 6 singletons):

| Person | Pattern | Demo |
|--------|---------|------|
| Michael Greene | 4 variants, consistent DOB/email | Clean transitive merge |
| Sarah Chen | 4 variants, one has wrong DOB | Veto rule splits the edge |
| James Wilson | 4 variants, shares email with Person 4 | Fraud ring signal |
| Maria Garcia | 4 variants, shares company with Person 5 | 2-hop to flagged entity |
| Viktor Petrov | 3 variants, OFAC SDN flagged | Bad neighbour propagation |

## BigQuery Tables

| Table | Contents |
|-------|----------|
| `entity_clusters` | One row per record — cluster_id, size, confidence, requires_review |
| `cluster_edges` | One row per edge — deberta_score, rule scores, vetoed flag, veto_reason |
| `network_scores` | One row per record — all four context scores + composite |

## Configuration

Key settings in `config/training_config.yaml`:

| Section | Key Settings |
|---------|-------------|
| `graph` | `deberta_keep_threshold: 0.30`, `cluster_edge_threshold: 0.45` |
| `field_rules` | DOB (veto), email, phone, company — scorer, weight, veto_on_mismatch |
| `network_scoring.weights` | cluster_risk: 0.30, bad_neighbour: 0.40, shared_field: 0.20, centrality: 0.10 |
| `bigquery` | dataset, table names, location |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI + Uvicorn |
| Model | DeBERTa-v3-base + LoRA (Vertex AI) |
| Frontend | React 19 + Vite 8 + Tailwind CSS v4 |
| Charts | Plotly.js (react-plotly.js) |
| Graph Viz | react-force-graph-2d |
| Animations | Framer Motion |
| Icons | Lucide React |
| Clustering | NetworkX (connected components) |
| Data Store | GCS + BigQuery |
| Orchestration | Airflow DAG |
| CI/CD | GitHub Actions |

## Team

Group 13 — Northeastern University, MLOps Spring 2026
