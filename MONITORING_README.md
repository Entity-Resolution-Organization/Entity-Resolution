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

## Components

### 1. Monitoring Pipeline (`monitoring_pipeline.py`)

Vertex AI KFP v2 pipeline that runs daily. Contains four components:

- **monitor_op**: Evaluates model performance on holdout data, detects data drift using Kolmogorov-Smirnov tests, checks thresholds, logs results to BigQuery, and saves a detailed summary to GCS.
- **trigger_data_pipeline_op**: Calls Airflow REST API to trigger the data pipeline for fresh training data. Polls for completion before proceeding.
- **trigger_retrain_op**: Submits the ML training pipeline (`pipeline.yaml`) to Vertex AI.
- **alert_op**: Reads the monitoring summary from GCS and sends a rich Slack notification with F1, precision, recall, drift ratio, and drifted feature counts.

### 2. Monitor Script (`scripts/monitor.py`)

Core monitoring logic used by `monitor_op`. The `ModelMonitor` class:

- Loads the deployed model and runs inference on holdout test data
- Computes metrics: F1, precision, recall, AUC, accuracy
- Detects data drift by comparing current feature distributions against a reference profile using the Kolmogorov-Smirnov test
- Makes a decision based on configurable thresholds:
  - `RETRAIN_DATA`: Data drift detected (>30% features drifted)
  - `RETRAIN_MODEL`: Model metrics below threshold but data looks fine
  - `HEALTHY`: All checks passed
- Logs results to BigQuery tables

### 3. Grafana Dashboard

A 5-panel dashboard connected to BigQuery:

| Panel | Type | Data Source |
|-------|------|-------------|
| Model Performance Over Time | Line chart | `monitoring_metrics` |
| Data Drift Over Time | Line chart | `drift_reports` |
| Prediction Confidence Over Time | Line chart | `prediction_logs` |
| Latest Drift Score by Feature | Bar chart | `drift_reports` |
| Current Model Metrics | Stat | `monitoring_metrics` |

Dashboard JSON is version-controlled at `monitoring/grafana/er-monitoring-dashboard.json`.

### 4. Slack Notifications

Three alert types with detailed metrics:

- **Data Drift Alert**: Entity type, F1, precision, recall, drift ratio, number of drifted features, action taken
- **Model Deployed Alert**: Model version, F1, precision, recall, AUC, endpoint URL
- **Healthy Check Alert**: Current F1 per entity type, timestamp

## BigQuery Tables

### `entity_resolution.monitoring_metrics`

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMP | When the monitoring run occurred |
| metric_name | STRING | Metric name (f1, precision, recall, etc.) |
| metric_value | FLOAT | Metric value |
| entity_type | STRING | Entity type (person) |
| triggered_retrain | BOOLEAN | Whether retraining was triggered |
| pipeline_run_id | STRING | Pipeline run identifier |

### `entity_resolution.drift_reports`

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMP | When drift was measured |
| feature_name | STRING | Feature that was tested |
| drift_score | FLOAT | KS statistic or PSI score |
| is_drifted | BOOLEAN | Whether drift threshold was exceeded |
| test_method | STRING | Statistical test used |
| entity_type | STRING | Entity type |
| pipeline_run_id | STRING | Pipeline run identifier |

### `entity_resolution.prediction_logs`

| Column | Type | Description |
|--------|------|-------------|
| prediction_id | STRING | Unique prediction ID |
| timestamp | TIMESTAMP | When prediction was made |
| name1, address1 | STRING | First record |
| name2, address2 | STRING | Second record |
| probability | FLOAT | Match probability |
| match | BOOLEAN | Predicted match/non-match |
| model_version | STRING | Model version used |
| threshold_used | FLOAT | Classification threshold |
| latency_ms | FLOAT | Inference latency in milliseconds |

## Configuration

Monitoring thresholds are defined in `config/training_config.yaml`:

```yaml
validation:
  min_f1: 0.85
  min_precision: 0.80
  min_recall: 0.80
  min_auc: 0.80
  classification_threshold: 0.5

notifications:
  enabled: true
  slack_webhook_url: ""  # Set via environment or locally
```

## Prerequisites

- GCP project with Vertex AI, BigQuery, Cloud Scheduler, and Cloud Storage APIs enabled
- Airflow running on a GCP VM (for data pipeline triggering)
- Grafana installed on the same VM
- Slack workspace with an incoming webhook configured
- Docker image `er-trainer:latest` pushed to Artifact Registry
- Service account with `roles/bigquery.dataEditor` and `roles/bigquery.user`

## Setup

### 1. BigQuery Tables

Create the monitoring tables if they don't exist:

```bash
bq mk --table entity_resolution.monitoring_metrics \
    timestamp:TIMESTAMP,metric_name:STRING,metric_value:FLOAT,entity_type:STRING,triggered_retrain:BOOLEAN,pipeline_run_id:STRING

bq mk --table entity_resolution.drift_reports \
    timestamp:TIMESTAMP,feature_name:STRING,drift_score:FLOAT,is_drifted:BOOLEAN,test_method:STRING,entity_type:STRING,pipeline_run_id:STRING

bq mk --table entity_resolution.prediction_logs \
    prediction_id:STRING,timestamp:TIMESTAMP,name1:STRING,address1:STRING,name2:STRING,address2:STRING,probability:FLOAT,match:BOOLEAN,model_version:STRING,threshold_used:FLOAT,latency_ms:FLOAT
```

### 2. Grafana Dashboard

Install Grafana on your VM:

```bash
sudo apt-get install -y apt-transport-https software-properties-common
sudo mkdir -p /etc/apt/keyrings/
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install -y grafana
```

Install the BigQuery plugin and start Grafana:

```bash
sudo grafana-cli plugins install grafana-bigquery-datasource
sudo systemctl enable grafana-server
sudo systemctl start grafana-server
```

Import the dashboard:

1. Open Grafana at `http://<vm-external-ip>:3001`
2. Login with `admin` / `admin`
3. Go to Connections > Data Sources > Add BigQuery
4. Select "GCE Default Service Account" for authentication
5. Go to Dashboards > Import
6. Upload `monitoring/grafana/er-monitoring-dashboard.json`

### 3. Slack Notifications

1. Go to https://api.slack.com/apps
2. Create a new app with Incoming Webhooks enabled
3. Add the webhook URL to `config/training_config.yaml`

### 4. IAM Permissions

Ensure the Compute Engine default service account has BigQuery write access:

```bash
gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member="serviceAccount:<PROJECT_NUMBER>-compute@developer.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"
```

## Running the Pipeline

### Compile and Upload

```bash
cd Model-Pipeline
python monitoring_pipeline.py --compile --upload
```

### Run Manually

```bash
python monitoring_pipeline.py --run
```

### Schedule with Cloud Scheduler

The pipeline is already scheduled to run daily at 2am UTC via Cloud Scheduler. To verify:

```bash
gcloud scheduler jobs list --location=us-central1
```

To trigger it manually:

```bash
gcloud scheduler jobs run er-monitoring-daily --location=us-central1
```

## Monitoring the Monitor

- **Grafana Dashboard**: `http://<vm-external-ip>:3001` — check panels for data freshness
- **Vertex AI Console**: https://console.cloud.google.com/vertex-ai/pipelines — check pipeline run history
- **Slack Channel**: `#all-mlops-project` — alerts appear here automatically
- **BigQuery**: Query `monitoring_metrics` to verify new rows are being written

## Troubleshooting

### Grafana shows "No data"

- Check the time range (set to "Last 90 days")
- Verify BigQuery data source connection (Connections > Data Sources > Save & Test)
- Ensure queries use `timestamp` column (not `date`)

### Slack alerts not sending

- Verify webhook URL in `config/training_config.yaml`
- Check `notifications.enabled` is `true`
- Test webhook manually:
  ```bash
  curl -X POST -H 'Content-Type: application/json' \
    --data '{"text":"Test message"}' \
    <YOUR_WEBHOOK_URL>
  ```

### Monitoring pipeline fails

- Check Vertex AI pipeline logs in the console
- Verify the `er-trainer:latest` Docker image is up to date
- Ensure GCS bucket `entity-resolution-bucket-1` is accessible

### Prediction logs not being written

- Verify service account has `roles/bigquery.dataEditor`
- Check `ENABLE_PREDICTION_LOG=true` environment variable on the endpoint
- Review `serve.py` logs in the Vertex AI endpoint

## File Structure

```
Model-Pipeline/
├── monitoring_pipeline.py      # Vertex AI monitoring pipeline definition
├── pipeline.py                 # Vertex AI ML training pipeline definition
├── scripts/
│   ├── monitor.py              # Core monitoring logic (ModelMonitor class)
│   ├── serve.py                # Model serving with prediction logging
│   ├── train.py                # DeBERTa + LoRA training
│   └── evaluate.py             # Model evaluation
├── config/
│   └── training_config.yaml    # Thresholds, notifications, GCP config
monitoring/
└── grafana/
    └── er-monitoring-dashboard.json  # Grafana dashboard export
```
