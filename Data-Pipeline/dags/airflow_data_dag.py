"""
Entity Resolution Data Pipeline
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
    """Download and load raw data."""
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

    # Save to temp
    output_path = '/tmp/laundrograph/raw_data.parquet'
    os.makedirs('/tmp/laundrograph', exist_ok=True)
    raw_df.to_parquet(output_path, index=False)

    print(f"Loaded {len(raw_df)} records")

    context['task_instance'].xcom_push(key='raw_data_path', value=output_path)
    context['task_instance'].xcom_push(key='dataset_config', value=dataset_config)

    return output_path


def data_preprocessing(**context):
    """Preprocess data with simple (non-batched) processing."""
    ti = context['task_instance']

    raw_data_path = ti.xcom_pull(task_ids='load_data_task', key='raw_data_path')
    dataset_config = ti.xcom_pull(task_ids='load_data_task', key='dataset_config')

    # Load raw data
    raw_df = pd.read_parquet(raw_data_path)
    print(f"Processing {len(raw_df)} records")

    # Simple preprocessing - returns DataFrames
    accounts_df, pairs_df = preprocess_dataset(raw_df, dataset_config)

    print(f"Generated {len(accounts_df)} accounts, {len(pairs_df)} pairs")

    # Convert to CSV
    os.makedirs('/tmp/laundrograph', exist_ok=True)
    accounts_csv = '/tmp/laundrograph/accounts.csv'
    pairs_csv = '/tmp/laundrograph/er_pairs.csv'

    accounts_df.to_csv(accounts_csv, index=False)
    pairs_df.to_csv(pairs_csv, index=False)

    print(f"Saved: {accounts_csv}, {pairs_csv}")

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
    description="Data Pipeline - Simple processing for datasets < 100K records",
    schedule=None,
    catchup=False,
    tags=["entity-resolution", "data-pipeline"],
) as dag:

    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data,
    )

    data_preprocessing_task = PythonOperator(
        task_id="data_preprocessing_task",
        python_callable=data_preprocessing,
    )

    upload_accounts = LocalFilesystemToGCSOperator(
        task_id='upload_accounts_to_gcs',
        src='/tmp/laundrograph/accounts.csv',
        dst='data/accounts.csv',
        bucket=GCS_BUCKET,
    )

    upload_pairs = LocalFilesystemToGCSOperator(
        task_id='upload_pairs_to_gcs',
        src='/tmp/laundrograph/er_pairs.csv',
        dst='data/er_pairs.csv',
        bucket=GCS_BUCKET,
    )

    load_accounts_bq = BigQueryInsertJobOperator(
        task_id='load_accounts_bigquery',
        configuration={
            'load': {
                'sourceUris': [f'gs://{GCS_BUCKET}/data/accounts.csv'],
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
                'sourceUris': [f'gs://{GCS_BUCKET}/data/er_pairs.csv'],
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
    load_data_task >> data_preprocessing_task >> [upload_accounts, upload_pairs]
    upload_accounts >> load_accounts_bq
    upload_pairs >> load_pairs_bq


if __name__ == "__main__":
    dag.test()