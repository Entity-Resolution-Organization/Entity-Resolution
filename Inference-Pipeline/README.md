# Inference Pipeline

Entity Resolution inference service with a FastAPI backend and React frontend, powered by a DeBERTa-v3 + LoRA model on Google Cloud Vertex AI.

## Architecture

```
Inference-Pipeline/
├── scripts/
│   ├── app.py                # FastAPI backend (port 8000)
│   ├── model_client.py       # VertexAIClient + MockClient
│   ├── preprocess.py         # Name/address normalization
│   ├── demo_app.py           # Streamlit fallback UI (port 8501)
│   └── batch_inference.py    # GCS batch processor
├── frontend/                 # React SPA
│   ├── src/
│   │   ├── pages/            # Home, Resolve, Scenarios, Network, Batch, Pipeline, Monitor, Fraud
│   │   ├── components/       # Layout, DecisionBadge, ProbGauge, SimBars, EntityCard, etc.
│   │   └── api/client.js     # Axios wrapper for FastAPI endpoints
│   ├── vite.config.js        # Vite + Tailwind v4, proxy to port 8000
│   └── package.json
├── config/
│   └── inference_config.yaml # Thresholds, GCS paths, mock toggle
├── dags/
│   └── airflow_inference_dag.py
├── tests/
├── Dockerfile
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

### 3. Streamlit Fallback

```bash
cd Inference-Pipeline
USE_MOCK_CLIENT=true streamlit run scripts/demo_app.py
# Opens at http://localhost:8501
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/resolve` | Single pair resolution: `{name1, address1, name2, address2}` |
| POST | `/resolve/batch` | Batch resolution: `{pairs: [...]}` |
| POST | `/search` | Entity search against analytics dataset |
| GET | `/health` | API + endpoint connectivity status |
| GET | `/metrics/pipeline` | Quality gate, bias report, model metrics (F1, AUC, etc.) |
| GET | `/metrics/inference` | Runtime request count, latency, decision distribution |

### Example

```bash
curl -X POST http://localhost:8000/resolve \
  -H "Content-Type: application/json" \
  -d '{"name1":"Robert Smith","address1":"123 Main St","name2":"Bob Smith","address2":"123 Main Street"}'
```

Response:
```json
{
  "probability": 0.9989,
  "decision": "MATCH",
  "confidence_level": "HIGH",
  "field_similarities": {...},
  "latency_ms": 1775.2
}
```

## Frontend Pages

| Page | Route | Description |
|------|-------|-------------|
| Home | `/` | Landing page with architecture diagram, stats, feature cards |
| Resolve | `/resolve` | Pairwise matching with field attribution + rule-based scoring |
| Scenarios | `/scenarios` | 12 preset test cases with "Run All" |
| Network | `/network` | Force-directed entity graph (react-force-graph-2d) |
| Batch | `/batch` | CSV upload with column mapping, donut chart, histogram |
| Pipeline | `/pipeline` | F1/Precision/Recall/AUC metrics from GCS |
| Monitor | `/monitor` | Live inference stats, decision distribution |
| Fraud | `/fraud` | GraphSAGE GNN placeholder (in progress) |

## Resolve Page Features

The Resolve page provides two layers of explainability:

1. **Semantic Attribution (DeBERTa)**: Runs N+1 forward passes — baseline + one per masked field — to show how much each field contributed to the match score.

2. **Structured Rules (Deterministic)**: Email exact match, DOB confirmation (overrides to 100% if DOB matches and score > 0.5), phone match/partial/missing.

## Configuration

Key settings in `config/inference_config.yaml`:

| Setting | Default | Description |
|---------|---------|-------------|
| `thresholds.match_threshold` | 0.45 | Score >= this = MATCH |
| `thresholds.no_match_ceiling` | 0.20 | Score <= this = NO-MATCH |
| `mock.use_mock` | true | Use Jaro-Winkler fallback (override with `USE_MOCK_CLIENT` env var) |
| `vertex_ai.max_batch_size` | 256 | Max pairs per Vertex AI request |

## Docker

```bash
docker compose up
# API at :8000, Streamlit at :8501
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI + Uvicorn |
| Model | DeBERTa-v3-base + LoRA (Vertex AI) |
| Frontend | React 18 + Vite + Tailwind CSS v4 |
| Charts | Plotly.js (react-plotly.js) |
| Network Graph | react-force-graph-2d |
| Animations | Framer Motion |
| Icons | Lucide React |
| Batch | GCS read/write + MLflow logging |
| Orchestration | Airflow DAG |
| CI/CD | GitHub Actions |

## Team

Group 13 — Northeastern University, MLOps Spring 2026
