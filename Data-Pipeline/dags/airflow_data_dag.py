"""
Entity Resolution Data Pipeline - Split Validation & Transformation
Supports both local testing (LOCAL_MODE=true) and GCS/BigQuery production mode.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import yaml
import pandas as pd
import os
import sys
import json

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')

from dataset_factory import get_dataset_handler
from preprocessing import preprocess_dataset
from bias_detection import BiasDetector

# Mode Configuration - set LOCAL_MODE=false for GCS/BigQuery production
LOCAL_MODE = os.getenv('LOCAL_MODE', 'true').lower() == 'true'
LOCAL_DATA_DIR = '/opt/airflow/data'
TMP_DIR = '/tmp/laundrograph'

# GCP Configuration (from dev branch - actual project settings)
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'entity-resolution-487121')
GCS_BUCKET = os.getenv('GCS_BUCKET', 'entity-resolution-bucket-1')
BQ_DATASET = os.getenv('BQ_DATASET', 'entity_resolution_bq')


def load_data(**context):
    """Download and load raw data."""
    with open('/opt/airflow/config/datasets.yaml') as f:
        config = yaml.safe_load(f)

    active_dataset = config['active_dataset']
    dataset_config = config['datasets'][active_dataset].copy()

    print(f"Loading dataset: {active_dataset}")
    print(f"[Mode] {'LOCAL' if LOCAL_MODE else 'PRODUCTION (GCS/BigQuery)'}")

    # Download data
    handler = get_dataset_handler(active_dataset, dataset_config)
    raw_df = handler.download()
    raw_df = handler.normalize_schema(raw_df)
    raw_df = handler.subsample(raw_df)

    # Save based on mode
    if LOCAL_MODE:
        os.makedirs(f'{LOCAL_DATA_DIR}/raw', exist_ok=True)
        data_path = f'{LOCAL_DATA_DIR}/raw/data.csv'
        raw_df.to_csv(data_path, index=False)
        print(f"[LOCAL] Saved {len(raw_df)} records to {data_path}")
    else:
        # Production: save to /tmp/ for later GCS upload
        os.makedirs(TMP_DIR, exist_ok=True)
        data_path = f'{TMP_DIR}/raw_data.csv'
        raw_df.to_csv(data_path, index=False)
        print(f"[PROD] Saved {len(raw_df)} records to {data_path}")

    context['task_instance'].xcom_push(key='raw_data_path', value=data_path)
    context['task_instance'].xcom_push(key='dataset_config', value=dataset_config)

    return data_path


def data_validation(**context):
    """Validate raw data quality and schema."""
    ti = context['task_instance']

    raw_data_path = ti.xcom_pull(task_ids='load_data_task', key='raw_data_path')

    # Load raw data
    raw_df = pd.read_csv(raw_data_path)
    print(f"[Validation] Validating {len(raw_df)} records from {raw_data_path}")

    # Schema validation
    required_fields = ['id', 'name', 'address']
    missing_fields = [f for f in required_fields if f not in raw_df.columns]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")

    # Null check
    null_counts = raw_df[required_fields].isnull().sum()
    null_pct = (null_counts / len(raw_df) * 100).round(2)
    print(f"[Validation] Null percentages: {null_pct.to_dict()}")

    if (null_counts > len(raw_df) * 0.5).any():
        raise ValueError(f"Excessive nulls detected: {null_counts[null_counts > len(raw_df) * 0.5].to_dict()}")

    # Distribution check
    print(f"[Validation] Unique IDs: {raw_df['id'].nunique()}")
    print(f"[Validation] Unique names: {raw_df['name'].nunique()}")

    # Data quality metrics
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'total_records': len(raw_df),
        'null_counts': null_counts.to_dict(),
        'null_percentages': null_pct.to_dict(),
        'unique_ids': int(raw_df['id'].nunique()),
        'unique_names': int(raw_df['name'].nunique()),
        'passed': True
    }

    # Save validation results
    metrics_dir = LOCAL_DATA_DIR + '/metrics' if LOCAL_MODE else TMP_DIR
    os.makedirs(metrics_dir, exist_ok=True)
    validation_path = f'{metrics_dir}/validation.json'
    with open(validation_path, 'w') as f:
        json.dump(validation_results, f, indent=2)
    print(f"[Validation] Saved results to {validation_path}")

    print(f"[Validation] ✓ All checks passed")

    ti.xcom_push(key='validation_results', value=validation_results)
    return validation_results


def data_transformation(**context):
    """Transform data: normalize, corrupt, generate pairs."""
    ti = context['task_instance']

    raw_data_path = ti.xcom_pull(task_ids='load_data_task', key='raw_data_path')
    dataset_config = ti.xcom_pull(task_ids='load_data_task', key='dataset_config')

    # Load raw data
    raw_df = pd.read_csv(raw_data_path)
    print(f"[Transformation] Processing {len(raw_df)} records from {raw_data_path}")

    # Run preprocessing pipeline
    accounts_df, pairs_df = preprocess_dataset(raw_df, dataset_config)

    print(f"[Transformation] Generated {len(accounts_df)} accounts, {len(pairs_df)} pairs")

    # Save based on mode
    if LOCAL_MODE:
        os.makedirs(f'{LOCAL_DATA_DIR}/processed', exist_ok=True)
        accounts_path = f'{LOCAL_DATA_DIR}/processed/accounts.csv'
        pairs_path = f'{LOCAL_DATA_DIR}/processed/er_pairs.csv'
    else:
        # Production: save to /tmp/ for GCS upload
        os.makedirs(TMP_DIR, exist_ok=True)
        accounts_path = f'{TMP_DIR}/accounts.csv'
        pairs_path = f'{TMP_DIR}/er_pairs.csv'

    accounts_df.to_csv(accounts_path, index=False)
    pairs_df.to_csv(pairs_path, index=False)
    print(f"[Transformation] Saved to: {accounts_path}, {pairs_path}")

    ti.xcom_push(key='accounts_csv', value=accounts_path)
    ti.xcom_push(key='pairs_csv', value=pairs_path)

    return {'accounts': accounts_path, 'pairs': pairs_path}


def bias_detection(**context):
    """Detect bias in processed data."""
    ti = context['task_instance']

    accounts_path = ti.xcom_pull(task_ids='data_transformation_task', key='accounts_csv')
    pairs_path = ti.xcom_pull(task_ids='data_transformation_task', key='pairs_csv')

    # Load data
    accounts_df = pd.read_csv(accounts_path)
    pairs_df = pd.read_csv(pairs_path)

    print(f"[Bias Detection] Analyzing {len(accounts_df)} accounts, {len(pairs_df)} pairs")

    # Run bias detection
    metrics_dir = LOCAL_DATA_DIR + '/metrics' if LOCAL_MODE else TMP_DIR
    detector = BiasDetector(output_dir=metrics_dir)
    report = detector.generate_bias_report(accounts_df, pairs_df)

    # Log summary
    print(f"[Bias Detection] Overall risk: {report['summary']['overall_bias_risk']}")
    print(f"[Bias Detection] Issues found: {report['summary']['total_issues']}")

    if report['summary']['bias_issues']:
        print(f"[Bias Detection] Affected areas: {', '.join(report['summary']['bias_issues'])}")

    # Alert if high risk
    if report['summary']['overall_bias_risk'] == 'HIGH':
        print("[Bias Detection] WARNING: High bias risk detected! Review data sources.")

    ti.xcom_push(key='bias_report', value=report)

    return report


# DAG configuration
default_args = {
    "owner": "Entity Resolution Team",
    "start_date": datetime(2026, 2, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="er_data_pipeline",
    default_args=default_args,
    description="Entity Resolution Data Pipeline with validation, transformation, and bias detection",
    schedule=None,
    catchup=False,
    tags=["entity-resolution", "data-pipeline"],
) as dag:

    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data,
    )

    data_validation_task = PythonOperator(
        task_id="data_validation_task",
        python_callable=data_validation,
    )

    data_transformation_task = PythonOperator(
        task_id="data_transformation_task",
        python_callable=data_transformation,
    )

    bias_detection_task = PythonOperator(
        task_id="bias_detection_task",
        python_callable=bias_detection,
    )

    # Pipeline flow depends on mode
    if LOCAL_MODE:
        # Local mode: skip GCS/BigQuery tasks
        pipeline_complete = EmptyOperator(
            task_id='pipeline_complete',
        )
        load_data_task >> data_validation_task >> data_transformation_task >> bias_detection_task >> pipeline_complete
    else:
        # Production mode: upload to GCS, then load to BigQuery
        upload_accounts = LocalFilesystemToGCSOperator(
            task_id='upload_accounts_to_gcs',
            src=f'{TMP_DIR}/accounts.csv',
            dst='processed/accounts.csv',
            bucket=GCS_BUCKET,
        )

        upload_pairs = LocalFilesystemToGCSOperator(
            task_id='upload_pairs_to_gcs',
            src=f'{TMP_DIR}/er_pairs.csv',
            dst='processed/er_pairs.csv',
            bucket=GCS_BUCKET,
        )

        load_accounts_bq = BigQueryInsertJobOperator(
            task_id='load_accounts_bigquery',
            configuration={
                'load': {
                    'sourceUris': [f'gs://{GCS_BUCKET}/processed/accounts.csv'],
                    'destinationTable': {
                        'projectId': GCP_PROJECT_ID,
                        'datasetId': BQ_DATASET,
                        'tableId': 'accounts'
                    },
                    'sourceFormat': 'CSV',
                    'skipLeadingRows': 1,
                    'autodetect': True,
                    'writeDisposition': 'WRITE_TRUNCATE',
                }
            }
        )

        load_pairs_bq = BigQueryInsertJobOperator(
            task_id='load_pairs_bigquery',
            configuration={
                'load': {
                    'sourceUris': [f'gs://{GCS_BUCKET}/processed/er_pairs.csv'],
                    'destinationTable': {
                        'projectId': GCP_PROJECT_ID,
                        'datasetId': BQ_DATASET,
                        'tableId': 'er_pairs'
                    },
                    'sourceFormat': 'CSV',
                    'skipLeadingRows': 1,
                    'autodetect': True,
                    'writeDisposition': 'WRITE_TRUNCATE',
                }
            }
        )

        # Pipeline flow: load → validate → transform → bias → [upload to GCS] → [load to BigQuery]
        load_data_task >> data_validation_task >> data_transformation_task >> bias_detection_task
        bias_detection_task >> [upload_accounts, upload_pairs]
        upload_accounts >> load_accounts_bq
        upload_pairs >> load_pairs_bq


if __name__ == "__main__":
    dag.test()

