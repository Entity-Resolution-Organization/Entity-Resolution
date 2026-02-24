# GCP Service Account Setup

This directory contains GCP service account credentials for cloud operations.

## Setup Instructions

1. **Go to GCP Console:**
   https://console.cloud.google.com/iam-admin/serviceaccounts?project=YOUR_PROJECT_ID

2. **Create Service Account:**
   - Name: `airflow-pipeline-sa`
   - Description: "Service account for Airflow data pipeline"

3. **Grant Required Roles:**
   - `Storage Object Admin` - For GCS uploads
   - `BigQuery Data Editor` - For BigQuery table writes
   - `BigQuery Job User` - For running BigQuery jobs

4. **Create JSON Key:**
   - Click on the service account
   - Go to "Keys" tab
   - Click "Add Key" > "Create new key"
   - Select JSON format
   - Download the key file

5. **Save Key File:**
   ```bash
   # Save the downloaded key as:
   secrets/gcp-sa-key.json
   ```

6. **Restart Airflow:**
   ```bash
   docker-compose restart
   ```

## File Structure

```
secrets/
├── README.md           # This file
├── .gitkeep            # Keeps directory in git
└── gcp-sa-key.json     # Your service account key (DO NOT COMMIT)
```

## Security Notes

- **NEVER commit `gcp-sa-key.json` to version control**
- The `.gitignore` file excludes `*.json` in this directory
- For CI/CD, use GitHub Secrets or environment variables
- Rotate keys regularly (every 90 days recommended)

## Environment Variables

The following environment variables are used:

| Variable | Description | Default |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | GCP Project ID | (set in .env) |
| `GCS_BUCKET` | GCS bucket name | (set in .env) |
| `BQ_DATASET` | BigQuery dataset | (set in .env) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account key | `/opt/airflow/secrets/gcp-sa-key.json` |
| `EXECUTION_MODE` | `local` or `cloud` | `local` |

## Verification

Test your setup:

```bash
# Enter Airflow container
docker exec -it data-pipeline-airflow-scheduler-1 bash

# Test GCS access
python -c "
from google.cloud import storage
client = storage.Client()
buckets = list(client.list_buckets())
print(f'Found {len(buckets)} buckets')
"

# Test BigQuery access
python -c "
from google.cloud import bigquery
client = bigquery.Client()
datasets = list(client.list_datasets())
print(f'Found {len(datasets)} datasets')
"
```
