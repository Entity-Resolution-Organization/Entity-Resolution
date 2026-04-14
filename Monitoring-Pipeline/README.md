# Model Monitoring Pipeline

## Overview

This monitoring system continuously evaluates the deployed Entity Resolution (DeBERTa + LoRA) model in production. It detects performance degradation and data drift, triggers appropriate retraining workflows, sends Slack notifications with detailed metrics, and visualizes everything through a Grafana dashboard.

## Architecture

```
Cloud Scheduler (daily, 2am UTC)
        │
        ▼
Monitoring Pipeline (Vertex AI)
        │
        ├── Evaluate model on holdout data
        ├── Detect data drift (Kolmogorov-Smirnov test)
        ├── Check thresholds
        ├── Log metrics to BigQuery
        │
        ▼
   ┌─────────────────────────────────────┐
   │           Decision Logic            │
   ├─────────────────────────────────────┤
   │                                     │
   │  RETRAIN_DATA (data drift)          │
   │    → Trigger Data Pipeline (Airflow)│
   │    → Trigger ML Pipeline            │
   │    → Slack alert with drift details │
   │                                     │
   │  RETRAIN_MODEL (metrics drop)       │
   │    → Trigger ML Pipeline only       │
   │    → Slack alert with metrics       │
   │                                     │
   │  HEALTHY                            │
   │    → Slack notification             │
   │                                     │
   └─────────────────────────────────────┘
```

## Setup

```bash
git clone https://github.com/Entity-Resolution-Organization/Entity-Resolution.git
cd Entity-Resolution/Monitoring-Pipeline
```

Create `.env` from the example and fill in your values:

```bash
cp .env.example .env
nano .env
```

Install dependencies:

```bash
pip install python-dotenv google-cloud-aiplatform kfp
```

## Running the Pipeline

```bash
# Compile only
python monitoring_pipeline.py --compile

# Compile and upload to GCS
python monitoring_pipeline.py --compile --upload

# Compile, upload, and run
python monitoring_pipeline.py --run

# Trigger scheduled run manually
gcloud scheduler jobs run er-monitoring-daily --location=<GCP_REGION>
```

## Viewing Results

### Grafana Dashboard

Import `grafana/er-monitoring-dashboard.json` into your Grafana instance and select the BigQuery data source when prompted.

| Panel | What It Shows |
|-------|---------------|
| Model Performance Over Time | F1, precision, recall trends over days/weeks |
| Data Drift Over Time | Drift scores per feature over time |
| Prediction Confidence Over Time | Prediction probability and latency |
| Latest Drift Score by Feature | Bar chart of current drift per feature |
| Current Model Metrics | Latest F1, precision, recall values |

### Slack Notifications

Alerts arrive automatically after every pipeline run. Ask a team member for an invite to the Slack alerts channel.

| Alert | Details |
|-------|---------|
| Data Drift Alert | F1, precision, recall, drift ratio, drifted feature count |
| Model Deployed Alert | Model version, metrics, endpoint URL |
| Healthy Check Alert | Current F1 per entity type |

### BigQuery

Query monitoring data:

```sql
-- Latest model metrics
SELECT * FROM `entity_resolution.monitoring_metrics` ORDER BY timestamp DESC LIMIT 10

-- Recent drift reports
SELECT * FROM `entity_resolution.drift_reports` ORDER BY timestamp DESC LIMIT 20

-- Recent predictions
SELECT * FROM `entity_resolution.prediction_logs` ORDER BY timestamp DESC LIMIT 10
```

### Vertex AI Console

View pipeline run history:

```
https://console.cloud.google.com/vertex-ai/pipelines?project=<GCP_PROJECT_ID>
```
Use the GCP_PROJECT_ID from your .env file

## File Structure

```
Entity-Resolution/
├── Data-Pipeline/                              # Airflow DAG for data generation
├── Model-Pipeline/
│   ├── pipeline.py                             # ML training pipeline
│   ├── scripts/
│   │   ├── serve.py                            # Model serving + prediction logging
│   │   ├── train.py                            # DeBERTa + LoRA training
│   │   └── evaluate.py                         # Model evaluation
│   └── config/
│       └── training_config.yaml                # Thresholds, notifications, GCP config
├── Monitoring-Pipeline/
│   ├── monitoring_pipeline.py                  # Vertex AI monitoring pipeline
│   ├── scripts/
│   │   └── monitor.py                          # Core monitoring logic (ModelMonitor)
│   ├── grafana/
│   │   └── er-monitoring-dashboard.json        # Grafana dashboard export
│   └── README.md                               # This file
├── Initial_Setup/                              # Terraform GCP resources
└── README.md                                   # Project overview
```
## Tech Stack

| Component          | Technology                        |
|--------------------|-----------------------------------|
| Orchestration      | Vertex AI Pipelines (KFP v2)      |
| Scheduling         | Google Cloud Scheduler             |
| Drift Detection    | Evidently AI                       |
| Metrics Storage    | BigQuery                           |
| Dashboard          | Grafana                            |
| Notifications      | Slack (Incoming Webhooks)          |
| Configuration      | python-dotenv + `.env`             |
| Cloud              | Google Cloud Platform              |
