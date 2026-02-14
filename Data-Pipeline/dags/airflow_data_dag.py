from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# TO DO: Replace with actual imports for different functions
# from scripts import (
#     load_data,
#     data_preprocessing,
#     build_save_model,
#     load_model_elbow
# )

default_args = {
    "owner": "Entity Resolution Team",
    "start_date": datetime(2026, 2, 13),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# TO DO: Sample structure, replace with actual 
with DAG(
    dag_id="entity_resolution_data_pipeline",
    default_args=default_args,
    description="Data Pipeline to import, process and load datasets and store in GCP BigQuery.",
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
        op_args=[load_data_task.output],
    )

    load_data_task >> data_preprocessing_task

if __name__ == "__main__":
    dag.test()