"""
Entity Resolution Data Pipeline - Multi-Domain Processing
Supports 6 datasets across 3 entity types (PERSON, PRODUCT, PUBLICATION)
for domain-specific LoRA adapter training.
"""

import json
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

# Add scripts to path
sys.path.insert(0, "/opt/airflow/scripts")

from bias_detection import BiasDetector
from dataset_factory import get_dataset_handler
from preprocessing import preprocess_dataset

from schema_validation import SchemaValidator

# Mode Configuration - set LOCAL_MODE=false for GCS/BigQuery production
LOCAL_MODE = os.getenv("LOCAL_MODE", "true").lower() == "true"
LOCAL_DATA_DIR = "/opt/airflow/data"
TMP_DIR = "/tmp/laundrograph"

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "entity-resolution-487121")
GCS_BUCKET = os.getenv("GCS_BUCKET", "entity-resolution-bucket-1")
BQ_DATASET = os.getenv("BQ_DATASET", "entity_resolution_bq")


def load_data(**context):
    """Download and load raw data from ALL active datasets."""
    with open("/opt/airflow/config/datasets.yaml") as f:
        config = yaml.safe_load(f)

    # Support both single dataset (legacy) and multiple datasets
    active_datasets = config.get(
        "active_datasets", [config.get("active_dataset", "pseudopeople")]
    )
    if isinstance(active_datasets, str):
        active_datasets = [active_datasets]

    print(f"[Mode] {'LOCAL' if LOCAL_MODE else 'PRODUCTION (GCS/BigQuery)'}")
    print(
        f"[Multi-Domain] Processing {len(active_datasets)} datasets: {active_datasets}"
    )

    all_raw_data = []
    dataset_configs = {}

    for dataset_name in active_datasets:
        if dataset_name not in config["datasets"]:
            print(f"[WARNING] Dataset '{dataset_name}' not found in config, skipping")
            continue

        dataset_config = config["datasets"][dataset_name].copy()
        entity_type = dataset_config.get("entity_type", "UNKNOWN")

        print(f"\n[Loading] {dataset_name} ({entity_type})")

        try:
            # Download data using appropriate handler
            handler = get_dataset_handler(dataset_name, dataset_config)
            raw_df = handler.download()
            raw_df = handler.normalize_schema(raw_df)
            raw_df = handler.subsample(raw_df)

            # Add metadata columns for multi-domain support
            raw_df["entity_type"] = entity_type
            raw_df["source_dataset"] = dataset_name

            print(f"[Loaded] {dataset_name}: {len(raw_df)} records ({entity_type})")

            all_raw_data.append(raw_df)
            dataset_configs[dataset_name] = dataset_config

        except Exception as e:
            print(f"[ERROR] Failed to load {dataset_name}: {e}")
            continue

    if not all_raw_data:
        raise ValueError("No datasets were successfully loaded!")

    # Combine all datasets
    combined_df = pd.concat(all_raw_data, ignore_index=True)
    print(
        f"\n[Combined] Total: {len(combined_df)} records from {len(all_raw_data)} datasets"
    )

    # Log entity type distribution
    entity_dist = combined_df["entity_type"].value_counts().to_dict()
    print(f"[Entity Distribution] {entity_dist}")

    # Save based on mode
    if LOCAL_MODE:
        os.makedirs(f"{LOCAL_DATA_DIR}/raw", exist_ok=True)
        data_path = f"{LOCAL_DATA_DIR}/raw/data.csv"
    else:
        os.makedirs(TMP_DIR, exist_ok=True)
        data_path = f"{TMP_DIR}/raw_data.csv"

    combined_df.to_csv(data_path, index=False)
    print(f"[Saved] {len(combined_df)} records to {data_path}")

    context["task_instance"].xcom_push(key="raw_data_path", value=data_path)
    context["task_instance"].xcom_push(key="dataset_configs", value=dataset_configs)
    context["task_instance"].xcom_push(key="entity_distribution", value=entity_dist)

    return data_path


def data_validation(**context):
    """Validate raw data quality and schema for multi-domain data."""
    ti = context["task_instance"]

    raw_data_path = ti.xcom_pull(task_ids="load_data_task", key="raw_data_path")
    entity_distribution = ti.xcom_pull(
        task_ids="load_data_task", key="entity_distribution"
    )

    # Load raw data
    raw_df = pd.read_csv(raw_data_path)
    print(f"[Validation] Validating {len(raw_df)} records from {raw_data_path}")

    # Schema validation - core fields
    required_fields = ["id", "name", "address"]
    missing_fields = [f for f in required_fields if f not in raw_df.columns]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")

    # Check for metadata columns
    metadata_fields = ["entity_type", "source_dataset"]
    missing_metadata = [f for f in metadata_fields if f not in raw_df.columns]
    if missing_metadata:
        print(f"[Validation] WARNING: Missing metadata columns: {missing_metadata}")

    # Null check on core fields
    null_counts = raw_df[required_fields].isnull().sum()
    null_pct = (null_counts / len(raw_df) * 100).round(2)
    print(f"[Validation] Null percentages: {null_pct.to_dict()}")

    if (null_counts > len(raw_df) * 0.5).any():
        raise ValueError(
            f"Excessive nulls detected: {null_counts[null_counts > len(raw_df) * 0.5].to_dict()}"
        )

    # Multi-domain validation
    if "entity_type" in raw_df.columns:
        entity_types = raw_df["entity_type"].unique().tolist()
        print(f"[Validation] Entity types present: {entity_types}")

    if "source_dataset" in raw_df.columns:
        sources = raw_df["source_dataset"].unique().tolist()
        print(f"[Validation] Source datasets: {sources}")

    # Data quality metrics
    validation_results = {
        "timestamp": datetime.now().isoformat(),
        "total_records": len(raw_df),
        "null_counts": null_counts.to_dict(),
        "null_percentages": null_pct.to_dict(),
        "unique_ids": int(raw_df["id"].nunique()),
        "unique_names": int(raw_df["name"].nunique()),
        "entity_distribution": entity_distribution,
        "source_datasets": (
            raw_df["source_dataset"].nunique()
            if "source_dataset" in raw_df.columns
            else 1
        ),
        "passed": True,
    }

    # Save validation results
    metrics_dir = LOCAL_DATA_DIR + "/metrics" if LOCAL_MODE else TMP_DIR
    os.makedirs(metrics_dir, exist_ok=True)
    validation_path = f"{metrics_dir}/validation.json"
    with open(validation_path, "w") as f:
        json.dump(validation_results, f, indent=2)
    print(f"[Validation] Saved results to {validation_path}")

    print(f"[Validation] All checks passed")

    ti.xcom_push(key="validation_results", value=validation_results)
    return validation_results


def data_transformation(**context):
    """Transform data: normalize, corrupt, generate pairs for all entity types."""
    ti = context["task_instance"]

    raw_data_path = ti.xcom_pull(task_ids="load_data_task", key="raw_data_path")
    dataset_configs = ti.xcom_pull(task_ids="load_data_task", key="dataset_configs")

    # Load combined raw data
    raw_df = pd.read_csv(raw_data_path)
    print(f"[Transformation] Processing {len(raw_df)} records from {raw_data_path}")

    all_accounts = []
    all_pairs = []

    # Process each source dataset separately to maintain proper pairing
    if "source_dataset" in raw_df.columns:
        source_datasets = raw_df["source_dataset"].unique()
    else:
        source_datasets = ["default"]
        raw_df["source_dataset"] = "default"

    for source in source_datasets:
        source_df = raw_df[raw_df["source_dataset"] == source].copy()

        # Get config for this source (use first available config as fallback)
        if source in dataset_configs:
            source_config = dataset_configs[source]
        else:
            source_config = list(dataset_configs.values())[0] if dataset_configs else {}

        entity_type = (
            source_df["entity_type"].iloc[0]
            if "entity_type" in source_df.columns
            else "UNKNOWN"
        )

        print(
            f"\n[Transformation] Processing {source} ({entity_type}): {len(source_df)} records"
        )

        try:
            # Run preprocessing pipeline
            accounts_df, pairs_df = preprocess_dataset(source_df, source_config)

            # Ensure metadata columns are preserved/added
            accounts_df["entity_type"] = entity_type
            accounts_df["source_dataset"] = source
            pairs_df["entity_type"] = entity_type
            pairs_df["source_dataset"] = source

            print(
                f"[Transformation] {source}: {len(accounts_df)} accounts, {len(pairs_df)} pairs"
            )

            all_accounts.append(accounts_df)
            all_pairs.append(pairs_df)

        except Exception as e:
            print(f"[ERROR] Failed to transform {source}: {e}")
            continue

    if not all_accounts:
        raise ValueError("No datasets were successfully transformed!")

    # Combine all transformed data
    combined_accounts = pd.concat(all_accounts, ignore_index=True)
    combined_pairs = pd.concat(all_pairs, ignore_index=True)

    print(f"\n[Combined Output]")
    print(f"  - Total accounts: {len(combined_accounts)}")
    print(f"  - Total pairs: {len(combined_pairs)}")

    # Log entity type distribution in output
    if "entity_type" in combined_accounts.columns:
        account_dist = combined_accounts["entity_type"].value_counts().to_dict()
        pair_dist = combined_pairs["entity_type"].value_counts().to_dict()
        print(f"  - Account entity distribution: {account_dist}")
        print(f"  - Pair entity distribution: {pair_dist}")

    # Save based on mode
    if LOCAL_MODE:
        os.makedirs(f"{LOCAL_DATA_DIR}/processed", exist_ok=True)
        accounts_path = f"{LOCAL_DATA_DIR}/processed/accounts.csv"
        pairs_path = f"{LOCAL_DATA_DIR}/processed/er_pairs.csv"
    else:
        os.makedirs(TMP_DIR, exist_ok=True)
        accounts_path = f"{TMP_DIR}/accounts.csv"
        pairs_path = f"{TMP_DIR}/er_pairs.csv"

    combined_accounts.to_csv(accounts_path, index=False)
    combined_pairs.to_csv(pairs_path, index=False)
    print(f"[Transformation] Saved to: {accounts_path}, {pairs_path}")

    ti.xcom_push(key="accounts_csv", value=accounts_path)
    ti.xcom_push(key="pairs_csv", value=pairs_path)

    return {"accounts": accounts_path, "pairs": pairs_path}


def schema_validation(**context):
    """Validate schema and data quality for multi-domain processed data."""
    ti = context["task_instance"]

    accounts_path = ti.xcom_pull(
        task_ids="data_transformation_task", key="accounts_csv"
    )
    pairs_path = ti.xcom_pull(task_ids="data_transformation_task", key="pairs_csv")

    # Load data
    accounts_df = pd.read_csv(accounts_path)
    pairs_df = pd.read_csv(pairs_path)

    print(
        f"[Schema Validation] Validating {len(accounts_df)} accounts, {len(pairs_df)} pairs"
    )

    # Log multi-domain info
    if "entity_type" in accounts_df.columns:
        entity_types = accounts_df["entity_type"].unique().tolist()
        print(f"[Schema Validation] Entity types: {entity_types}")

    # Run validation
    metrics_dir = LOCAL_DATA_DIR + "/metrics" if LOCAL_MODE else TMP_DIR
    validator = SchemaValidator(output_dir=metrics_dir)
    results = validator.validate_all(accounts_df, pairs_df)

    # Add multi-domain metadata to results
    results["multi_domain"] = {
        "entity_types": (
            accounts_df["entity_type"].unique().tolist()
            if "entity_type" in accounts_df.columns
            else []
        ),
        "source_datasets": (
            accounts_df["source_dataset"].unique().tolist()
            if "source_dataset" in accounts_df.columns
            else []
        ),
        "entity_distribution": (
            accounts_df["entity_type"].value_counts().to_dict()
            if "entity_type" in accounts_df.columns
            else {}
        ),
    }

    # Save results
    validator.save_results(results)

    # Log summary
    print(f"[Schema Validation] Overall success: {results['overall_success']}")
    print(
        f"[Schema Validation] Accounts: {results['accounts_validation']['statistics']['success_rate']}% pass rate"
    )
    print(
        f"[Schema Validation] Pairs: {results['pairs_validation']['statistics']['success_rate']}% pass rate"
    )

    if not results["overall_success"]:
        failed_count = results["summary"]["failed_expectations"]
        print(f"[Schema Validation] WARNING: {failed_count} expectations failed!")
        for exp in results["accounts_validation"]["failed_expectations"]:
            print(f"  - Accounts: {exp['expectation']} failed")
        for exp in results["pairs_validation"]["failed_expectations"]:
            print(f"  - Pairs: {exp['expectation']} failed")

    ti.xcom_push(key="schema_validation_results", value=results)

    return results


def bias_detection(**context):
    """Detect bias in multi-domain processed data."""
    ti = context["task_instance"]

    accounts_path = ti.xcom_pull(
        task_ids="data_transformation_task", key="accounts_csv"
    )
    pairs_path = ti.xcom_pull(task_ids="data_transformation_task", key="pairs_csv")

    # Load data
    accounts_df = pd.read_csv(accounts_path)
    pairs_df = pd.read_csv(pairs_path)

    print(
        f"[Bias Detection] Analyzing {len(accounts_df)} accounts, {len(pairs_df)} pairs"
    )

    # Log multi-domain info
    if "entity_type" in accounts_df.columns:
        entity_dist = accounts_df["entity_type"].value_counts()
        print(f"[Bias Detection] Entity type distribution:")
        for et, count in entity_dist.items():
            pct = count / len(accounts_df) * 100
            print(f"  - {et}: {count} ({pct:.1f}%)")

    if "source_dataset" in accounts_df.columns:
        source_dist = accounts_df["source_dataset"].value_counts()
        print(f"[Bias Detection] Source dataset distribution:")
        for src, count in source_dist.items():
            pct = count / len(accounts_df) * 100
            print(f"  - {src}: {count} ({pct:.1f}%)")

    # Run bias detection
    metrics_dir = LOCAL_DATA_DIR + "/metrics" if LOCAL_MODE else TMP_DIR
    detector = BiasDetector(output_dir=metrics_dir)
    report = detector.generate_bias_report(accounts_df, pairs_df)

    # Add multi-domain analysis to report
    report["multi_domain_analysis"] = {
        "entity_type_distribution": (
            accounts_df["entity_type"].value_counts().to_dict()
            if "entity_type" in accounts_df.columns
            else {}
        ),
        "source_dataset_distribution": (
            accounts_df["source_dataset"].value_counts().to_dict()
            if "source_dataset" in accounts_df.columns
            else {}
        ),
        "entity_types_present": (
            accounts_df["entity_type"].nunique()
            if "entity_type" in accounts_df.columns
            else 0
        ),
        "source_datasets_present": (
            accounts_df["source_dataset"].nunique()
            if "source_dataset" in accounts_df.columns
            else 0
        ),
    }

    # Check entity type balance
    if "entity_type" in accounts_df.columns:
        entity_counts = accounts_df["entity_type"].value_counts()
        max_count = entity_counts.max()
        min_count = entity_counts.min()
        imbalance_ratio = max_count / min_count if min_count > 0 else float("inf")

        report["multi_domain_analysis"]["entity_type_imbalance_ratio"] = round(
            imbalance_ratio, 2
        )

        if imbalance_ratio > 3:
            print(
                f"[Bias Detection] WARNING: Entity type imbalance detected (ratio: {imbalance_ratio:.2f})"
            )
            if "bias_issues" not in report["summary"]:
                report["summary"]["bias_issues"] = []
            report["summary"]["bias_issues"].append("entity_type_imbalance")

    # Save updated report
    report_path = f"{metrics_dir}/bias_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    # Log summary
    print(f"\n[Bias Detection] Overall risk: {report['summary']['overall_bias_risk']}")
    print(f"[Bias Detection] Issues found: {report['summary']['total_issues']}")

    if report["summary"]["bias_issues"]:
        print(
            f"[Bias Detection] Affected areas: {', '.join(report['summary']['bias_issues'])}"
        )

    if report["summary"]["overall_bias_risk"] == "HIGH":
        print("[Bias Detection] WARNING: High bias risk detected! Review data sources.")

    ti.xcom_push(key="bias_report", value=report)

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
    description="Multi-Domain Entity Resolution Pipeline (PERSON, PRODUCT, PUBLICATION)",
    schedule=None,
    catchup=False,
    tags=["entity-resolution", "data-pipeline", "multi-domain"],
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

    schema_validation_task = PythonOperator(
        task_id="schema_validation_task",
        python_callable=schema_validation,
    )

    bias_detection_task = PythonOperator(
        task_id="bias_detection_task",
        python_callable=bias_detection,
    )

    # Pipeline flow depends on mode
    if LOCAL_MODE:
        # Local mode: skip GCS/BigQuery tasks
        pipeline_complete = EmptyOperator(
            task_id="pipeline_complete",
        )
        (
            load_data_task
            >> data_validation_task
            >> data_transformation_task
            >> schema_validation_task
            >> bias_detection_task
            >> pipeline_complete
        )
    else:
        # Production mode: upload to GCS, then load to BigQuery
        upload_accounts = LocalFilesystemToGCSOperator(
            task_id="upload_accounts_to_gcs",
            src=f"{TMP_DIR}/accounts.csv",
            dst="processed/accounts.csv",
            bucket=GCS_BUCKET,
        )

        upload_pairs = LocalFilesystemToGCSOperator(
            task_id="upload_pairs_to_gcs",
            src=f"{TMP_DIR}/er_pairs.csv",
            dst="processed/er_pairs.csv",
            bucket=GCS_BUCKET,
        )

        load_accounts_bq = BigQueryInsertJobOperator(
            task_id="load_accounts_bigquery",
            configuration={
                "load": {
                    "sourceUris": [f"gs://{GCS_BUCKET}/processed/accounts.csv"],
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BQ_DATASET,
                        "tableId": "accounts",
                    },
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "autodetect": True,
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )

        load_pairs_bq = BigQueryInsertJobOperator(
            task_id="load_pairs_bigquery",
            configuration={
                "load": {
                    "sourceUris": [f"gs://{GCS_BUCKET}/processed/er_pairs.csv"],
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BQ_DATASET,
                        "tableId": "er_pairs",
                    },
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "autodetect": True,
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )

        # Pipeline flow
        (
            load_data_task
            >> data_validation_task
            >> data_transformation_task
            >> schema_validation_task
            >> bias_detection_task
        )
        bias_detection_task >> [upload_accounts, upload_pairs]
        upload_accounts >> load_accounts_bq
        upload_pairs >> load_pairs_bq


if __name__ == "__main__":
    dag.test()
