"""
Entity Resolution Data Pipeline - Fixed version using CSV for temp storage
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import yaml
import pandas as pd
import os
import sys

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')

from dataset_factory import get_dataset_handler
from preprocessing import preprocess_dataset

# GCP Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'project-id')
GCS_BUCKET = os.getenv('GCS_BUCKET', 'laundrograph-data')
BQ_DATASET = os.getenv('BQ_DATASET', 'laundrograph')


def load_data(**context):
    """Download and load raw data to /tmp/ as CSV."""
    with open('/opt/airflow/config/datasets.yaml') as f:
        config = yaml.safe_load(f)

    active_dataset = config['active_dataset']
    dataset_config = config['datasets'][active_dataset]

    print(f"Loading dataset: {active_dataset}")

    # Download data
    handler = get_dataset_handler(active_dataset, dataset_config)
    raw_df = handler.download()
    raw_df = handler.normalize_schema(raw_df)
    raw_df = handler.subsample(raw_df)

    # Save to /tmp/ as CSV (avoid PyArrow GCS issues)
    output_path = '/tmp/laundrograph/raw_data.csv'
    os.makedirs('/tmp/laundrograph', exist_ok=True)
    raw_df.to_csv(output_path, index=False)

    print(f"Loaded {len(raw_df)} records to {output_path}")

    context['task_instance'].xcom_push(key='raw_data_path', value=output_path)
    context['task_instance'].xcom_push(key='dataset_config', value=dataset_config)

    return output_path


def data_validation(**context):
    """Validate raw data quality and schema."""
    ti = context['task_instance']
    
    raw_data_path = ti.xcom_pull(task_ids='load_data_task', key='raw_data_path')
    dataset_config = ti.xcom_pull(task_ids='load_data_task', key='dataset_config')
    
    # Load raw data from /tmp/
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
        'total_records': len(raw_df),
        'null_counts': null_counts.to_dict(),
        'unique_ids': int(raw_df['id'].nunique()),
        'passed': True
    }
    
    print(f"[Validation] ✓ All checks passed")
    
    ti.xcom_push(key='validation_results', value=validation_results)
    return validation_results


def data_transformation(**context):
    """Transform data: normalize, corrupt, generate pairs."""
    ti = context['task_instance']
    
    raw_data_path = ti.xcom_pull(task_ids='load_data_task', key='raw_data_path')
    dataset_config = ti.xcom_pull(task_ids='load_data_task', key='dataset_config')
    
    # Load raw data from /tmp/
    raw_df = pd.read_csv(raw_data_path)
    print(f"[Transformation] Processing {len(raw_df)} records from {raw_data_path}")
    
    # Run preprocessing pipeline
    accounts_df, pairs_df = preprocess_dataset(raw_df, dataset_config)
    
    print(f"[Transformation] Generated {len(accounts_df)} accounts, {len(pairs_df)} pairs")
    
    # Save to /tmp/ as CSV
    os.makedirs('/tmp/laundrograph', exist_ok=True)
    accounts_csv = '/tmp/laundrograph/accounts.csv'
    pairs_csv = '/tmp/laundrograph/er_pairs.csv'
    
    accounts_df.to_csv(accounts_csv, index=False)
    pairs_df.to_csv(pairs_csv, index=False)
    
    print(f"[Transformation] Saved to {accounts_csv}, {pairs_csv}")
    
    ti.xcom_push(key='accounts_csv', value=accounts_csv)
    ti.xcom_push(key='pairs_csv', value=pairs_csv)
    
    return {'accounts': accounts_csv, 'pairs': pairs_csv}


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
    description="Entity Resolution Data Pipeline - Production Ready",
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

    upload_accounts = LocalFilesystemToGCSOperator(
        task_id='upload_accounts_to_gcs',
        src='/tmp/laundrograph/accounts.csv',
        dst='processed/accounts.csv',
        bucket=GCS_BUCKET,
    )

    upload_pairs = LocalFilesystemToGCSOperator(
        task_id='upload_pairs_to_gcs',
        src='/tmp/laundrograph/er_pairs.csv',
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

    # Pipeline flow
    load_data_task >> data_validation_task >> data_transformation_task >> [upload_accounts, upload_pairs]
    upload_accounts >> load_accounts_bq
    upload_pairs >> load_pairs_bq


if __name__ == "__main__":
    dag.test()
