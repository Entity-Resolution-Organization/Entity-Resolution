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

## How to Access (Existing Setup)

### Grafana Dashboard

- **URL**: http://34.31.109.57:3001
- **Login**: admin / (ask team for password)
- **Dashboard**: Dashboards > ER Model Monitoring

The dashboard has 5 panels:

| Panel | What It Shows |
|-------|---------------|
| Model Performance Over Time | F1, precision, recall trends over days/weeks |
| Data Drift Over Time | Drift scores per feature over time |
| Prediction Confidence Over Time | Prediction probability and latency |
| Latest Drift Score by Feature | Bar chart of current drift per feature |
| Current Model Metrics | Latest F1, precision, recall values |

### Slack Notifications

Alerts are sent to `#all-mlops-project` in the **Mlops project** Slack workspace. Ask a team member for an invite.

Three alert types:

- **Data Drift Alert**: F1, precision, recall, drift ratio, drifted feature count
- **Model Deployed Alert**: Model version, metrics, endpoint URL
- **Healthy Check Alert**: Current F1 per entity type

### BigQuery

Query monitoring data in `entity-resolution-487121.entity_resolution`:

```sql
-- Latest model metrics
SELECT * FROM `entity_resolution.monitoring_metrics` ORDER BY timestamp DESC LIMIT 10

-- Recent drift reports
SELECT * FROM `entity_resolution.drift_reports` ORDER BY timestamp DESC LIMIT 20

-- Recent predictions
SELECT * FROM `entity_resolution.prediction_logs` ORDER BY timestamp DESC LIMIT 10
```

### Vertex AI Console

Pipeline run history: https://console.cloud.google.com/vertex-ai/pipelines?project=entity-resolution-487121

---

## Setup from Scratch

Follow these steps to recreate the entire monitoring system.

### Prerequisites

- GCP project with these APIs enabled: Vertex AI, BigQuery, Cloud Scheduler, Cloud Storage, Cloud Build, Cloud Resource Manager
- A GCP VM running Airflow (for data pipeline triggering)
- Docker image `er-trainer:latest` pushed to Artifact Registry
- Python 3.10+

### Step 1: Create BigQuery Tables

```bash
bq mk --dataset entity_resolution

bq mk --table entity_resolution.monitoring_metrics \
    timestamp:TIMESTAMP,metric_name:STRING,metric_value:FLOAT,entity_type:STRING,triggered_retrain:BOOLEAN,pipeline_run_id:STRING

bq mk --table entity_resolution.drift_reports \
    timestamp:TIMESTAMP,feature_name:STRING,drift_score:FLOAT,is_drifted:BOOLEAN,test_method:STRING,entity_type:STRING,pipeline_run_id:STRING

bq mk --table entity_resolution.prediction_logs \
    prediction_id:STRING,timestamp:TIMESTAMP,name1:STRING,address1:STRING,name2:STRING,address2:STRING,probability:FLOAT,match:BOOLEAN,model_version:STRING,threshold_used:FLOAT,latency_ms:FLOAT
```

### Step 2: Set IAM Permissions

Grant BigQuery write access to the Compute Engine default service account:

```bash
gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member="serviceAccount:<PROJECT_NUMBER>-compute@developer.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member="serviceAccount:<PROJECT_NUMBER>-compute@developer.gserviceaccount.com" \
    --role="roles/bigquery.user"
```

### Step 3: Generate Reference Dataset Profile

The monitoring pipeline compares current data against a reference profile. Generate it from your training data:

```bash
cd Model-Pipeline
python scripts/monitor.py --generate-reference
```

This saves the reference profile to `gs://<GCS_BUCKET>/monitoring/reference_profile.json`.

### Step 4: Set Up Slack Notifications

1. Go to https://api.slack.com/apps
2. Click **Create New App** > **From scratch**
3. Name it (e.g., `ER Monitoring`), pick your workspace, click Create
4. Click **Incoming Webhooks** in the left sidebar
5. Toggle **Activate** to On
6. Click **Add New Webhook to Workspace**
7. Pick a channel (e.g., `#monitoring`)
8. Copy the webhook URL

Add the webhook URL to your local `config/training_config.yaml`:

```yaml
notifications:
  enabled: true
  slack_webhook_url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

**Important**: Do not commit the webhook URL to GitHub. Keep it local only.

Test it:

```bash
curl -X POST -H 'Content-Type: application/json' \
    --data '{"text":"Test notification from ER Monitoring"}' \
    https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Step 5: Install Grafana

SSH into your GCP VM:

```bash
sudo apt-get install -y apt-transport-https software-properties-common
sudo mkdir -p /etc/apt/keyrings/
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install -y grafana
```

Configure the port (if 3000 is already used by Airflow):

```bash
sudo sed -i 's/;http_port = 3000/http_port = 3001/' /etc/grafana/grafana.ini
```

Install BigQuery plugin and start:

```bash
sudo grafana-cli plugins install grafana-bigquery-datasource
sudo systemctl enable grafana-server
sudo systemctl start grafana-server
```

Open a firewall rule:

```bash
gcloud compute firewall-rules create allow-grafana \
    --allow tcp:3001 \
    --description="Allow Grafana dashboard"
```

Enable Cloud Resource Manager API (required by BigQuery plugin):

```bash
gcloud services enable cloudresourcemanager.googleapis.com --project=<PROJECT_ID>
```

### Step 6: Configure Grafana Data Source

1. Open `http://<VM_EXTERNAL_IP>:3001`
2. Login with `admin` / `admin` (change password on first login)
3. Go to **Connections** > **Data sources** > **Add data source**
4. Search for **Google BigQuery**
5. Select **GCE Default Service Account** for authentication
6. Click **Save & Test** — should show "Data source is working"

### Step 7: Import Grafana Dashboard

1. Go to **Dashboards** > **Import**
2. Upload `monitoring/grafana/er-monitoring-dashboard.json`
3. Select the BigQuery data source when prompted
4. Click **Import**

### Step 8: Compile and Upload the Monitoring Pipeline

```bash
cd Model-Pipeline
python monitoring_pipeline.py --compile --upload
```

### Step 9: Set Up Cloud Scheduler

```bash
gcloud scheduler jobs create http er-monitoring-daily \
    --schedule="0 2 * * *" \
    --uri="https://us-central1-aiplatform.googleapis.com/v1/projects/<PROJECT_ID>/locations/us-central1/pipelineJobs" \
    --http-method=POST \
    --location=us-central1 \
    --time-zone="UTC"
```

### Step 10: Verify Everything

```bash
python monitoring_pipeline.py --run
```

Check:
- **Vertex AI Console**: Pipeline should appear and run successfully
- **BigQuery**: New rows in `monitoring_metrics` and `drift_reports`
- **Grafana**: Dashboard panels should show new data points
- **Slack**: Alert should appear in your channel

---

## Components

### Monitoring Pipeline (`monitoring_pipeline.py`)

Vertex AI KFP v2 pipeline with four components:

- **monitor_op**: Evaluates model, detects drift, checks thresholds, logs to BigQuery, saves summary to GCS
- **trigger_data_pipeline_op**: Calls Airflow REST API to trigger data pipeline, polls for completion
- **trigger_retrain_op**: Submits the ML training pipeline to Vertex AI
- **alert_op**: Reads summary from GCS, sends rich Slack notification

### Monitor Script (`scripts/monitor.py`)

The `ModelMonitor` class:

- Loads deployed model, runs inference on holdout data
- Computes metrics: F1, precision, recall, AUC, accuracy
- Detects data drift via Kolmogorov-Smirnov test against reference profile
- Decision logic:
  - `RETRAIN_DATA`: >30% features drifted
  - `RETRAIN_MODEL`: Metrics below threshold but data looks fine
  - `HEALTHY`: All checks passed
- Logs results to BigQuery

### Decision Logic

| Condition | Decision | Action |
|-----------|----------|--------|
| All metrics above threshold, drift < 30% | HEALTHY | Slack notification only |
| Metrics above threshold, drift > 30% | RETRAIN_DATA | Data pipeline > ML pipeline > Slack alert |
| Metrics below threshold, drift < 30% | RETRAIN_MODEL | ML pipeline only > Slack alert |

## BigQuery Schema

### `monitoring_metrics`

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMP | When the monitoring run occurred |
| metric_name | STRING | Metric name (f1, precision, recall, etc.) |
| metric_value | FLOAT | Metric value |
| entity_type | STRING | Entity type (person) |
| triggered_retrain | BOOLEAN | Whether retraining was triggered |
| pipeline_run_id | STRING | Pipeline run identifier |

### `drift_reports`

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMP | When drift was measured |
| feature_name | STRING | Feature that was tested |
| drift_score | FLOAT | KS statistic score |
| is_drifted | BOOLEAN | Whether drift threshold was exceeded |
| test_method | STRING | Statistical test used (kolmogorov_smirnov) |
| entity_type | STRING | Entity type |
| pipeline_run_id | STRING | Pipeline run identifier |

### `prediction_logs`

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

Thresholds in `config/training_config.yaml`:

```yaml
validation:
  min_f1: 0.85
  min_precision: 0.80
  min_recall: 0.80
  min_auc: 0.80
  classification_threshold: 0.5

notifications:
  enabled: true
  slack_webhook_url: ""  # Set locally, never commit to repo
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
gcloud scheduler jobs run er-monitoring-daily --location=us-central1
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Grafana shows "No data" | Set time range to "Last 90 days", verify BigQuery connection |
| Slack alerts not sending | Check webhook URL in config, verify `notifications.enabled: true` |
| Pipeline fails on Vertex AI | Check logs in Vertex AI Console, verify Docker image is up to date |
| Prediction logs not written | Verify service account has `roles/bigquery.dataEditor` |
| Grafana won't start | Check port: `sudo lsof -i :3001`, check logs: `sudo cat /var/log/grafana/grafana.log` |
| BigQuery plugin auth fails | Enable Cloud Resource Manager API: `gcloud services enable cloudresourcemanager.googleapis.com` |

## File Structure

```
Entity-Resolution/
├── Data-Pipeline/                          # Airflow DAG for data generation
├── Model-Pipeline/
│   ├── pipeline.py                         # ML training pipeline
│   ├── monitoring_pipeline.py              # Monitoring pipeline
│   ├── scripts/
│   │   ├── monitor.py                      # Core monitoring logic
│   │   ├── serve.py                        # Model serving + prediction logging
│   │   ├── train.py                        # DeBERTa + LoRA training
│   │   └── evaluate.py                     # Model evaluation
│   └── config/
│       └── training_config.yaml            # Thresholds, notifications, GCP config
├── monitoring/
│   └── grafana/
│       └── er-monitoring-dashboard.json    # Grafana dashboard export
└── MONITORING_README.md
```
