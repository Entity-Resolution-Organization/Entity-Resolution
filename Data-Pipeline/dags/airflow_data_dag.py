"""
Entity Resolution Data Pipeline - Split Validation & Transformation
Supports both local testing and GCS/BigQuery production mode.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import yaml
import pandas as pd
import os
import sys
import json
from pathlib import Path

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')

from dataset_factory import get_dataset_handler
from preprocessing import preprocess_dataset
from bias_detection import BiasDetector

# Mode Configuration - set LOCAL_MODE=true for local testing without GCS
LOCAL_MODE = os.getenv('LOCAL_MODE', 'true').lower() == 'true'
LOCAL_DATA_DIR = '/opt/airflow/data'

# GCP Configuration (only used when LOCAL_MODE=false)
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'project-id')
GCS_BUCKET = os.getenv('GCS_BUCKET', 'laundrograph-data')
BQ_DATASET = os.getenv('BQ_DATASET', 'laundrograph')

# Local test configuration
LOCAL_TEST_RECORDS = int(os.getenv('LOCAL_TEST_RECORDS', '1000'))


def load_data(**context):
    """Download and load raw data."""
    with open('/opt/airflow/config/datasets.yaml') as f:
        config = yaml.safe_load(f)

    active_dataset = config['active_dataset']
    dataset_config = config['datasets'][active_dataset].copy()

    # Override records count for local testing
    if LOCAL_MODE:
        dataset_config['base_records'] = LOCAL_TEST_RECORDS
        print(f"[LOCAL MODE] Using {LOCAL_TEST_RECORDS} records for testing")

    print(f"Loading dataset: {active_dataset}")

    # Download data
    handler = get_dataset_handler(active_dataset, dataset_config)
    raw_df = handler.download()
    raw_df = handler.normalize_schema(raw_df)
    raw_df = handler.subsample(raw_df)

    # Save to local or GCS
    if LOCAL_MODE:
        os.makedirs(f'{LOCAL_DATA_DIR}/raw', exist_ok=True)
        data_path = f'{LOCAL_DATA_DIR}/raw/data.parquet'
        raw_df.to_parquet(data_path, index=False)
        print(f"[LOCAL MODE] Saved {len(raw_df)} records to {data_path}")
    else:
        data_path = f'gs://{GCS_BUCKET}/raw/data.parquet'
        raw_df.to_parquet(data_path, index=False)
        print(f"Loaded {len(raw_df)} records to {data_path}")

    context['task_instance'].xcom_push(key='raw_data_path', value=data_path)
    context['task_instance'].xcom_push(key='dataset_config', value=dataset_config)

    return data_path


def data_validation(**context):
    """Validate raw data quality and schema."""
    ti = context['task_instance']

    raw_data_path = ti.xcom_pull(task_ids='load_data_task', key='raw_data_path')
    dataset_config = ti.xcom_pull(task_ids='load_data_task', key='dataset_config')

    # Load raw data from GCS
    raw_df = pd.read_parquet(raw_data_path)
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
    if LOCAL_MODE:
        os.makedirs(f'{LOCAL_DATA_DIR}/metrics', exist_ok=True)
        validation_path = f'{LOCAL_DATA_DIR}/metrics/validation.json'
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
    raw_df = pd.read_parquet(raw_data_path)
    print(f"[Transformation] Processing {len(raw_df)} records from {raw_data_path}")

    # Run preprocessing pipeline
    accounts_df, pairs_df = preprocess_dataset(raw_df, dataset_config)

    print(f"[Transformation] Generated {len(accounts_df)} accounts, {len(pairs_df)} pairs")

    # Save to local or GCS
    if LOCAL_MODE:
        os.makedirs(f'{LOCAL_DATA_DIR}/processed', exist_ok=True)
        accounts_path = f'{LOCAL_DATA_DIR}/processed/accounts.csv'
        pairs_path = f'{LOCAL_DATA_DIR}/processed/er_pairs.csv'
        accounts_df.to_csv(accounts_path, index=False)
        pairs_df.to_csv(pairs_path, index=False)
        print(f"[LOCAL MODE] Saved to: {accounts_path}, {pairs_path}")
    else:
        accounts_path = f'gs://{GCS_BUCKET}/processed/accounts.csv'
        pairs_path = f'gs://{GCS_BUCKET}/processed/er_pairs.csv'
        accounts_df.to_csv(accounts_path, index=False)
        pairs_df.to_csv(pairs_path, index=False)
        print(f"[Transformation] Saved to GCS: {accounts_path}, {pairs_path}")

    ti.xcom_push(key='accounts_csv', value=accounts_path)
    ti.xcom_push(key='pairs_csv', value=pairs_path)

    return {'accounts': accounts_path, 'pairs': pairs_path}


def bias_detection(**context):
    """Detect bias in processed data."""
    ti = context['task_instance']

    accounts_path = ti.xcom_pull(task_ids='data_transformation_task', key='accounts_csv')
    pairs_path = ti.xcom_pull(task_ids='data_transformation_task', key='pairs_csv')

    # Load data from GCS
    accounts_df = pd.read_csv(accounts_path)
    pairs_df = pd.read_csv(pairs_path)

    print(f"[Bias Detection] Analyzing {len(accounts_df)} accounts, {len(pairs_df)} pairs")

    # Run bias detection
    detector = BiasDetector(output_dir='/opt/airflow/data/metrics')
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
    dag_id="laundrograph_data_pipeline",
    default_args=default_args,
    description="Data Pipeline with separate validation and transformation stages",
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
        # Local mode: skip BigQuery tasks
        pipeline_complete = EmptyOperator(
            task_id='pipeline_complete',
        )
        load_data_task >> data_validation_task >> data_transformation_task >> bias_detection_task >> pipeline_complete
    else:
        # Production mode: load to BigQuery
        from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

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

        # load_data → validation → transformation → bias_detection → BigQuery loads
        load_data_task >> data_validation_task >> data_transformation_task >> bias_detection_task >> [load_accounts_bq, load_pairs_bq]


if __name__ == "__main__":
    dag.test()