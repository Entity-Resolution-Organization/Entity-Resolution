"""
Entity Resolution Data Pipeline - Optimized Multi-Domain Architecture

ARCHITECTURAL DESIGN:
This pipeline serves TWO distinct purposes requiring DIFFERENT data organizations:

1. MODEL TRAINING (Domain-Specific LoRA Adapters):
   - Data SEPARATED by entity type
   - Train 3 LoRA adapters: person_adapter, product_adapter, publication_adapter
   - Output: data/training/{entity_type}/train.csv

2. BIAS DETECTION (Cross-Domain Analytics):
   - ALL datasets MERGED together (287K records)
   - Analyze bias ACROSS entity types, geographies, sources
   - Output: data/analytics/merged_all.csv

Pipeline creates BOTH outputs in parallel - intentional multi-purpose design.

OPTIMIZED TASK STRUCTURE (~18 tasks):
- 6 load tasks (parallel)
- 1 validate_all_raw task (consolidated)
- 6 transform tasks (parallel)
- 1 organize_datasets task (creates training + analytics outputs)
- 3 validation tasks (parallel): training_splits, schema, bias
- Quality gate + cloud tasks
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yaml


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types."""

    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)
        return super().default(obj)


from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

# Add scripts to path
sys.path.insert(0, "/opt/airflow/scripts")

from bias_detection import BiasDetector  # noqa: E402
from data_validation import (  # noqa: E402
    DatasetValidator,
    QualityGate,
    TrainingSplitValidator,
)
from dataset_factory import get_dataset_handler  # noqa: E402
from preprocessing import preprocess_dataset  # noqa: E402
from schema_validation import SchemaValidator  # noqa: E402

# =============================================================================
# CONFIGURATION
# =============================================================================

LOCAL_MODE = os.getenv("LOCAL_MODE", "true").lower() == "true"
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "local")
LOCAL_DATA_DIR = "/opt/airflow/data"
TMP_DIR = "/tmp/laundrograph"

# GCP Configuration (set via environment variables or .env file)
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-gcs-bucket")
BQ_DATASET = os.getenv("BQ_DATASET", "your-bq-dataset")

# Dataset Configuration
DATASETS = [
    "pseudopeople",
    "nc_voters",
    "ofac_sdn",
    "wdc_products",
    "amazon_2018",
    "dblp_acm",
]

ENTITY_TYPE_MAP = {
    "pseudopeople": "PERSON",
    "nc_voters": "PERSON",
    "ofac_sdn": "PERSON",
    "wdc_products": "PRODUCT",
    "amazon_2018": "PRODUCT",
    "dblp_acm": "PUBLICATION",
}

ENTITY_GROUPS = {
    "person": ["pseudopeople", "nc_voters", "ofac_sdn"],
    "product": ["wdc_products", "amazon_2018"],
    "publication": ["dblp_acm"],
}

MIN_RECORDS_PER_DATASET = 100


def get_base_dir():
    """Get base directory based on mode."""
    return LOCAL_DATA_DIR if LOCAL_MODE else TMP_DIR


def load_config():
    """Load dataset configuration."""
    with open("/opt/airflow/config/datasets.yaml") as f:
        return yaml.safe_load(f)


# =============================================================================
# PHASE 1: LOAD TASKS (6 parallel)
# =============================================================================


def load_dataset(dataset_name: str, **context):
    """Load a single dataset."""
    config = load_config()

    if dataset_name not in config["datasets"]:
        raise ValueError(f"Dataset {dataset_name} not found in config")

    dataset_config = config["datasets"][dataset_name]
    entity_type = dataset_config.get(
        "entity_type", ENTITY_TYPE_MAP.get(dataset_name, "UNKNOWN")
    )

    print(f"[Load] {dataset_name} ({entity_type})")

    try:
        handler = get_dataset_handler(dataset_name, dataset_config)
        raw_df = handler.download()
        raw_df = handler.normalize_schema(raw_df)
        raw_df = handler.subsample(raw_df)

        raw_df["entity_type"] = entity_type
        raw_df["source_dataset"] = dataset_name

        print(f"[Load] {dataset_name}: {len(raw_df)} records")

        base_dir = get_base_dir()
        raw_dir = f"{base_dir}/raw/{dataset_name}"
        os.makedirs(raw_dir, exist_ok=True)

        raw_path = f"{raw_dir}/data.csv"
        raw_df.to_csv(raw_path, index=False)

        metadata = {
            "dataset_name": dataset_name,
            "entity_type": entity_type,
            "record_count": len(raw_df),
            "timestamp": datetime.now().isoformat(),
        }
        with open(f"{raw_dir}/metadata.json", "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        ti = context["task_instance"]
        ti.xcom_push(key=f"{dataset_name}_raw_path", value=raw_path)
        ti.xcom_push(key=f"{dataset_name}_config", value=dataset_config)
        ti.xcom_push(key=f"{dataset_name}_entity_type", value=entity_type)

        return raw_path

    except Exception as e:
        print(f"[ERROR] Failed to load {dataset_name}: {e}")
        raise


# =============================================================================
# PHASE 2: CONSOLIDATED VALIDATION (1 task for all datasets)
# =============================================================================


def validate_all_raw_datasets(**context):
    """
    Validate ALL raw datasets in one consolidated task.
    Replaces 6 individual validate_raw_X tasks.
    """
    ti = context["task_instance"]
    base_dir = get_base_dir()
    validator = DatasetValidator(output_dir=f"{base_dir}/metrics")

    print("[Validate All] Validating all 6 datasets...")

    all_results = {}
    failures = []

    for dataset_name in DATASETS:
        raw_path = ti.xcom_pull(
            task_ids=f"load_{dataset_name}", key=f"{dataset_name}_raw_path"
        )
        entity_type = ti.xcom_pull(
            task_ids=f"load_{dataset_name}", key=f"{dataset_name}_entity_type"
        )

        if not raw_path:
            raw_path = f"{base_dir}/raw/{dataset_name}/data.csv"

        print(f"[Validate All] Checking {dataset_name}...")

        results = validator.validate_raw_dataset(
            dataset_name=dataset_name,
            data_path=raw_path,
            entity_type=entity_type or ENTITY_TYPE_MAP.get(dataset_name, "UNKNOWN"),
            min_records=MIN_RECORDS_PER_DATASET,
        )

        all_results[dataset_name] = results

        if not results["success"]:
            failures.append(f"{dataset_name}: {results.get('critical_failures', [])}")
        else:
            print(f"[Validate All] {dataset_name}: PASSED")

    # Save consolidated results
    output_path = f"{base_dir}/metrics/validate_all_raw.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(
        f"\n[Validate All] Summary: {len(DATASETS) - len(failures)}/{len(DATASETS)} passed"
    )

    if failures:
        raise ValueError(f"Raw validation failed for: {failures}")

    ti.xcom_push(key="all_validation_results", value=all_results)
    return all_results


# =============================================================================
# PHASE 3: TRANSFORM TASKS (6 parallel)
# =============================================================================


def transform_dataset(dataset_name: str, **context):
    """Transform a single dataset."""
    ti = context["task_instance"]

    raw_path = ti.xcom_pull(
        task_ids=f"load_{dataset_name}", key=f"{dataset_name}_raw_path"
    )
    dataset_config = ti.xcom_pull(
        task_ids=f"load_{dataset_name}", key=f"{dataset_name}_config"
    )

    if not raw_path:
        raise ValueError(f"No raw data path for {dataset_name}")

    raw_df = pd.read_csv(raw_path)
    entity_type = (
        raw_df["entity_type"].iloc[0] if "entity_type" in raw_df.columns else "UNKNOWN"
    )

    print(f"[Transform] {dataset_name}: {len(raw_df)} records")

    accounts_df, pairs_df = preprocess_dataset(raw_df, dataset_config)

    accounts_df["entity_type"] = entity_type
    accounts_df["source_dataset"] = dataset_name
    pairs_df["entity_type"] = entity_type
    pairs_df["source_dataset"] = dataset_name

    print(
        f"[Transform] {dataset_name}: {len(accounts_df)} accounts, {len(pairs_df)} pairs"
    )

    base_dir = get_base_dir()
    output_dir = f"{base_dir}/processed/{dataset_name}"
    os.makedirs(output_dir, exist_ok=True)

    accounts_path = f"{output_dir}/accounts.csv"
    pairs_path = f"{output_dir}/pairs.csv"

    accounts_df.to_csv(accounts_path, index=False)
    pairs_df.to_csv(pairs_path, index=False)

    metadata = {
        "dataset_name": dataset_name,
        "entity_type": entity_type,
        "accounts_count": len(accounts_df),
        "pairs_count": len(pairs_df),
        "timestamp": datetime.now().isoformat(),
    }
    with open(f"{output_dir}/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    ti.xcom_push(key=f"{dataset_name}_accounts_path", value=accounts_path)
    ti.xcom_push(key=f"{dataset_name}_pairs_path", value=pairs_path)
    ti.xcom_push(key=f"{dataset_name}_metadata", value=metadata)

    return {"accounts": accounts_path, "pairs": pairs_path}


# =============================================================================
# PHASE 4: ORGANIZE DATASETS (1 task creates BOTH outputs)
# =============================================================================


def organize_datasets(**context):
    """
    Create BOTH data organizations from transformed datasets:
    1. Training Data: Separated by entity type with train/val/test splits
    2. Analytics Data: All merged for bias detection

    This single task replaces multiple combine/merge/split tasks.
    """
    ti = context["task_instance"]
    base_dir = get_base_dir()

    print("[Organize] Creating training and analytics data organizations...")

    # Collect all transformed data
    all_accounts = []
    all_pairs = []
    entity_pairs = {"person": [], "product": [], "publication": []}

    for dataset_name in DATASETS:
        accounts_path = ti.xcom_pull(
            task_ids=f"transform_{dataset_name}", key=f"{dataset_name}_accounts_path"
        )
        pairs_path = ti.xcom_pull(
            task_ids=f"transform_{dataset_name}", key=f"{dataset_name}_pairs_path"
        )

        if not accounts_path or not pairs_path:
            print(f"[Organize] WARNING: Missing data for {dataset_name}")
            continue

        accounts_df = pd.read_csv(accounts_path)
        pairs_df = pd.read_csv(pairs_path)

        all_accounts.append(accounts_df)
        all_pairs.append(pairs_df)

        # Group by entity type for training
        entity_type = ENTITY_TYPE_MAP.get(dataset_name, "UNKNOWN").lower()
        if entity_type in entity_pairs:
            entity_pairs[entity_type].append(pairs_df)

        print(
            f"[Organize] {dataset_name}: {len(accounts_df)} accounts, {len(pairs_df)} pairs"
        )

    # =========================================================================
    # OUTPUT 1: ANALYTICS DATA (All merged for bias detection)
    # =========================================================================
    analytics_dir = f"{base_dir}/analytics"
    os.makedirs(analytics_dir, exist_ok=True)

    merged_accounts = pd.concat(all_accounts, ignore_index=True)
    merged_pairs = pd.concat(all_pairs, ignore_index=True)

    merged_accounts_path = f"{analytics_dir}/merged_all.csv"
    merged_pairs_path = f"{analytics_dir}/merged_pairs.csv"

    merged_accounts.to_csv(merged_accounts_path, index=False)
    merged_pairs.to_csv(merged_pairs_path, index=False)

    # Also save to processed/merged for backward compatibility
    merged_dir = f"{base_dir}/processed/merged"
    os.makedirs(merged_dir, exist_ok=True)
    merged_accounts.to_csv(f"{merged_dir}/accounts.csv", index=False)
    merged_pairs.to_csv(f"{merged_dir}/er_pairs.csv", index=False)

    analytics_metadata = {
        "total_accounts": len(merged_accounts),
        "total_pairs": len(merged_pairs),
        "datasets_merged": len(DATASETS),
        "entity_distribution": (
            merged_accounts["entity_type"].value_counts().to_dict()
            if "entity_type" in merged_accounts.columns
            else {}
        ),
        "purpose": "Cross-domain bias analysis",
        "timestamp": datetime.now().isoformat(),
    }
    with open(f"{analytics_dir}/metadata.json", "w") as f:
        json.dump(analytics_metadata, f, indent=2)
    with open(f"{merged_dir}/metadata.json", "w") as f:
        json.dump(analytics_metadata, f, indent=2)

    print("\n[Organize] ANALYTICS OUTPUT:")
    print(f"  - {len(merged_accounts)} accounts -> {merged_accounts_path}")
    print(f"  - {len(merged_pairs)} pairs -> {merged_pairs_path}")

    # =========================================================================
    # OUTPUT 2: TRAINING DATA (Separated by entity type with splits)
    # =========================================================================
    training_dir = f"{base_dir}/training"
    split_summary = {}

    for entity_type, pairs_list in entity_pairs.items():
        if not pairs_list:
            print(f"[Organize] No pairs for {entity_type}, skipping")
            continue

        entity_dir = f"{training_dir}/{entity_type}"
        os.makedirs(entity_dir, exist_ok=True)

        # Combine all pairs for this entity type
        combined_pairs = pd.concat(pairs_list, ignore_index=True)

        # Deduplicate to prevent leakage
        original_count = len(combined_pairs)
        combined_pairs = combined_pairs.drop_duplicates(
            subset=["id1", "id2"], keep="first"
        )
        total_pairs = len(combined_pairs)

        if original_count > total_pairs:
            print(
                f"[Organize] {entity_type}: deduplicated {original_count} -> {total_pairs}"
            )

        # Save all_pairs.csv
        combined_pairs.to_csv(f"{entity_dir}/all_pairs.csv", index=False)

        # Create stratified train/val/test splits (70/15/15)
        if "label" in combined_pairs.columns and total_pairs > 0:
            positive = combined_pairs[combined_pairs["label"] == 1].sample(
                frac=1, random_state=42
            )
            negative = combined_pairs[combined_pairs["label"] == 0].sample(
                frac=1, random_state=42
            )

            def split_df(df):
                n = len(df)
                return (
                    df[: int(n * 0.7)],
                    df[int(n * 0.7) : int(n * 0.85)],
                    df[int(n * 0.85) :],
                )

            pos_train, pos_val, pos_test = split_df(positive)
            neg_train, neg_val, neg_test = split_df(negative)

            train_df = pd.concat([pos_train, neg_train]).sample(frac=1, random_state=42)
            val_df = pd.concat([pos_val, neg_val]).sample(frac=1, random_state=42)
            test_df = pd.concat([pos_test, neg_test]).sample(frac=1, random_state=42)
        else:
            # Random split without stratification
            combined_pairs = combined_pairs.sample(frac=1, random_state=42)
            n = total_pairs
            train_df = combined_pairs[: int(n * 0.7)]
            val_df = combined_pairs[int(n * 0.7) : int(n * 0.85)]
            test_df = combined_pairs[int(n * 0.85) :]

        # Save splits
        train_df.to_csv(f"{entity_dir}/train.csv", index=False)
        val_df.to_csv(f"{entity_dir}/val.csv", index=False)
        test_df.to_csv(f"{entity_dir}/test.csv", index=False)

        # Save metadata
        entity_metadata = {
            "entity_type": entity_type.upper(),
            "total_pairs": total_pairs,
            "train_count": len(train_df),
            "val_count": len(val_df),
            "test_count": len(test_df),
            "source_datasets": [
                d for d in DATASETS if ENTITY_TYPE_MAP.get(d, "").lower() == entity_type
            ],
            "purpose": f"LoRA adapter training for {entity_type} domain",
            "timestamp": datetime.now().isoformat(),
        }
        with open(f"{entity_dir}/metadata.json", "w") as f:
            json.dump(entity_metadata, f, indent=2)

        split_summary[entity_type] = {
            "total": total_pairs,
            "train": len(train_df),
            "val": len(val_df),
            "test": len(test_df),
        }

        print(f"\n[Organize] TRAINING OUTPUT ({entity_type.upper()}):")
        print(
            f"  - train.csv: {len(train_df)} pairs ({len(train_df)/total_pairs*100:.1f}%)"
        )
        print(f"  - val.csv: {len(val_df)} pairs ({len(val_df)/total_pairs*100:.1f}%)")
        print(
            f"  - test.csv: {len(test_df)} pairs ({len(test_df)/total_pairs*100:.1f}%)"
        )

    # Push results for downstream tasks
    ti.xcom_push(key="merged_accounts_path", value=merged_accounts_path)
    ti.xcom_push(key="merged_pairs_path", value=merged_pairs_path)
    ti.xcom_push(key="analytics_metadata", value=analytics_metadata)
    ti.xcom_push(key="split_summary", value=split_summary)

    return {
        "analytics": analytics_metadata,
        "training": split_summary,
    }


# =============================================================================
# PHASE 5: VALIDATION TASKS (3 parallel)
# =============================================================================


def validate_training_splits(**context):
    """Validate training splits for data leakage and label balance."""
    ti = context["task_instance"]
    base_dir = get_base_dir()
    training_dir = f"{base_dir}/training"

    print("[Validate Splits] Checking training data quality...")

    validator = TrainingSplitValidator(output_dir=f"{base_dir}/metrics")
    all_results = {}

    for entity_type in ["person", "product", "publication"]:
        entity_dir = f"{training_dir}/{entity_type}"

        if not os.path.exists(f"{entity_dir}/train.csv"):
            print(f"[Validate Splits] No splits for {entity_type}, skipping")
            continue

        results = validator.validate_entity_splits(
            entity_type=entity_type,
            entity_dir=entity_dir,
        )

        all_results[entity_type] = results
        print(
            f"[Validate Splits] {entity_type}: {'PASSED' if results['success'] else 'FAILED'}"
        )

    # Aggregate results into summary format expected by QualityGate
    total_entities = len(all_results)
    passed_entities = sum(1 for r in all_results.values() if r.get("success", False))
    success_rate = (passed_entities / total_entities * 100) if total_entities > 0 else 0

    # Collect critical failures (data leakage is critical)
    critical_failures = []
    for entity_type, results in all_results.items():
        if results.get("data_leakage", {}).get("has_leakage", False):
            critical_failures.append(f"{entity_type}: data leakage detected")

    all_results["summary"] = {
        "success_rate": success_rate,
        "critical_failures": critical_failures,
        "total_entities": total_entities,
        "passed_entities": passed_entities,
    }

    print(f"[Validate Splits] Summary: {passed_entities}/{total_entities} passed ({success_rate:.0f}%)")

    output_path = f"{base_dir}/metrics/training_split_validation.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, cls=NumpyEncoder)

    ti.xcom_push(key="split_validation_results", value=all_results)
    return all_results


def schema_validation(**context):
    """Validate schema of merged analytics data."""
    ti = context["task_instance"]
    base_dir = get_base_dir()

    print("[Schema] Validating analytics data schema...")

    validator = SchemaValidator(output_dir=f"{base_dir}/metrics")

    # Validate merged accounts
    accounts_path = f"{base_dir}/analytics/merged_all.csv"
    pairs_path = f"{base_dir}/analytics/merged_pairs.csv"

    if not os.path.exists(accounts_path):
        accounts_path = f"{base_dir}/processed/merged/accounts.csv"
        pairs_path = f"{base_dir}/processed/merged/er_pairs.csv"

    accounts_df = pd.read_csv(accounts_path)
    pairs_df = pd.read_csv(pairs_path)

    results = validator.validate_all(accounts_df, pairs_df)

    validator.save_results(results, "schema_validation_results.json")

    print(
        f"[Schema] Validation: {'PASSED' if results['overall_success'] else 'FAILED'}"
    )

    ti.xcom_push(key="schema_results", value=results)
    return results


def bias_detection(**context):
    """Run bias detection on merged analytics data."""
    ti = context["task_instance"]
    base_dir = get_base_dir()

    print("[Bias] Analyzing bias in merged data...")

    accounts_path = f"{base_dir}/analytics/merged_all.csv"
    pairs_path = f"{base_dir}/analytics/merged_pairs.csv"

    if not os.path.exists(accounts_path):
        accounts_path = f"{base_dir}/processed/merged/accounts.csv"
        pairs_path = f"{base_dir}/processed/merged/er_pairs.csv"

    accounts_df = pd.read_csv(accounts_path)
    pairs_df = pd.read_csv(pairs_path) if os.path.exists(pairs_path) else None

    detector = BiasDetector(output_dir=f"{base_dir}/metrics")
    report = detector.generate_bias_report(accounts_df, pairs_df)

    overall_risk = report.get("summary", {}).get("overall_bias_risk", "UNKNOWN")
    print(f"[Bias] Overall risk: {overall_risk}")

    ti.xcom_push(key="bias_report", value=report)
    return report


# =============================================================================
# PHASE 6: QUALITY GATE
# =============================================================================


def quality_gate_check(**context):
    """Aggregate all validation results and make go/no-go decision."""
    ti = context["task_instance"]
    base_dir = get_base_dir()

    print("[Quality Gate] Evaluating pipeline quality...")

    gate = QualityGate()

    # Collect validation results
    split_results = ti.xcom_pull(
        task_ids="validate_training_splits", key="split_validation_results"
    )
    schema_results = ti.xcom_pull(task_ids="schema_validation", key="schema_results")
    bias_report = ti.xcom_pull(task_ids="bias_detection", key="bias_report")

    decision = gate.evaluate(
        schema_results=schema_results,
        training_results=split_results,
        bias_results=bias_report,
    )

    output_path = f"{base_dir}/metrics/quality_gate_results.json"
    with open(output_path, "w") as f:
        json.dump(decision, f, indent=2)

    print(f"\n[Quality Gate] Decision: {decision['decision']}")

    if decision["decision"] == "NO-GO":
        failure_msg = f"Quality gate BLOCKED: {decision.get('failures', [])}"
        print(f"[Quality Gate] {failure_msg}")
        raise ValueError(failure_msg)

    ti.xcom_push(key="quality_gate_decision", value=decision)
    return decision


# =============================================================================
# PHASE 8: CLOUD TASKS
# =============================================================================


def upload_to_gcs(**context):
    """Upload data to GCS."""
    print(f"[GCS] Starting upload to gs://{GCS_BUCKET}/")

    try:
        from google.cloud import storage
    except ImportError:
        raise ImportError("google-cloud-storage required for cloud mode")

    base_dir = get_base_dir()
    ti = context["task_instance"]
    timestamp = datetime.now().strftime("%Y-%m-%d")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    uploads = []
    upload_files = [
        (
            f"{base_dir}/analytics/merged_all.csv",
            f"analytics/{timestamp}/merged_all.csv",
        ),
        (
            f"{base_dir}/analytics/merged_pairs.csv",
            f"analytics/{timestamp}/merged_pairs.csv",
        ),
        (
            f"{base_dir}/metrics/bias_report.json",
            f"metrics/{timestamp}/bias_report.json",
        ),
        (
            f"{base_dir}/metrics/quality_gate_results.json",
            f"metrics/{timestamp}/quality_gate.json",
        ),
    ]

    # Add training splits
    for entity_type in ["person", "product", "publication"]:
        for filename in ["train.csv", "val.csv", "test.csv", "metadata.json"]:
            local_path = f"{base_dir}/training/{entity_type}/{filename}"
            if os.path.exists(local_path):
                upload_files.append(
                    (local_path, f"training/{timestamp}/{entity_type}/{filename}")
                )

    for local_path, gcs_path in upload_files:
        if os.path.exists(local_path):
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            uploads.append(f"gs://{GCS_BUCKET}/{gcs_path}")
            print(f"[GCS] Uploaded -> {gcs_path}")

    print(f"[GCS] Completed: {len(uploads)} files")
    ti.xcom_push(key="gcs_uploads", value=uploads)
    ti.xcom_push(key="gcs_timestamp", value=timestamp)
    return uploads


def load_to_bigquery(**context):
    """Load data to BigQuery."""
    print("[BigQuery] Loading tables...")

    try:
        from google.cloud import bigquery
    except ImportError:
        raise ImportError("google-cloud-bigquery required for cloud mode")

    ti = context["task_instance"]
    base_dir = get_base_dir()
    client = bigquery.Client()

    # Load accounts
    accounts_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.accounts"
    accounts_path = f"{base_dir}/analytics/merged_all.csv"

    if not os.path.exists(accounts_path):
        accounts_path = f"{base_dir}/processed/merged/accounts.csv"

    # Read CSV to get column names and create STRING schema
    # (autodetect fails on partial date formats like "1946")
    df = pd.read_csv(accounts_path, nrows=1)
    schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    with open(accounts_path, "rb") as f:
        job = client.load_table_from_file(f, accounts_table, job_config=job_config)
    job.result()

    table = client.get_table(accounts_table)
    print(f"[BigQuery] Loaded {table.num_rows} rows to {accounts_table}")

    ti.xcom_push(key="bq_row_count", value=table.num_rows)
    return table.num_rows


def verify_and_complete(**context):
    """Verify cloud loads and complete pipeline."""
    ti = context["task_instance"]

    gcs_uploads = ti.xcom_pull(task_ids="upload_to_gcs", key="gcs_uploads") or []
    bq_rows = ti.xcom_pull(task_ids="load_to_bigquery", key="bq_row_count") or 0

    print(f"[Verify] GCS uploads: {len(gcs_uploads)}")
    print(f"[Verify] BigQuery rows: {bq_rows}")
    print("[Verify] Pipeline complete!")

    return {"gcs_files": len(gcs_uploads), "bq_rows": bq_rows}


# =============================================================================
# PHASE 9: DVC DATA VERSIONING
# =============================================================================


def dvc_track_and_version(**context):
    """
    Track all pipeline outputs with DVC and push to remote storage.

    This task:
    1. Tracks all pipeline outputs with DVC (data/processed, training, analytics, metrics)
    2. Pushes data to DVC remote storage
    3. Commits DVC tracking files to git with run metadata
    4. Logs DVC commit hash and data sizes

    This completes the MLOps loop: Airflow orchestrates, DVC versions.

    Note: In Docker development mode, DVC tracking may be skipped if git/dvc
    are not properly configured. Run `dvc add` manually on the host for local dev.
    """
    ti = context["task_instance"]
    run_id = context.get("run_id", "unknown")
    base_dir = get_base_dir()
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # DVC repo root is /opt/airflow (where .dvc folder is mounted)
    # Data directory is /opt/airflow/data
    dvc_repo_root = "/opt/airflow"

    print("[DVC] Starting data versioning...")
    print(f"[DVC] Repo root: {dvc_repo_root}")
    print(f"[DVC] Data dir: {base_dir}")

    # Check if we're in a valid DVC/git environment
    dvc_available = os.path.isdir(f"{dvc_repo_root}/.dvc")
    git_available = os.path.isdir(f"{dvc_repo_root}/.git")

    print(f"[DVC] DVC config available: {dvc_available}")
    print(f"[DVC] Git repo available: {git_available}")

    # Directories to track with DVC (relative paths from repo root)
    dvc_targets = [
        "data/processed",
        "data/training",
        "data/analytics",
        "data/metrics",
    ]

    tracked_items = []
    total_size_bytes = 0

    def run_cmd(cmd, cwd=None, check=True):
        """Run shell command and return output."""
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            cwd=cwd or dvc_repo_root,
        )
        if check and result.returncode != 0:
            print(f"[DVC] Command failed: {cmd}")
            print(f"[DVC] stderr: {result.stderr}")
        return result

    def get_dir_size(path):
        """Get total size of directory in bytes."""
        total = 0
        if os.path.isdir(path):
            for dirpath, _, filenames in os.walk(path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    if os.path.exists(fp):
                        total += os.path.getsize(fp)
        elif os.path.isfile(path):
            total = os.path.getsize(path)
        return total

    # Step 1: Calculate sizes and track with DVC
    print("[DVC] Step 1: Calculating output sizes...")
    for target in dvc_targets:
        # Full path for existence check and size calculation
        full_path = os.path.join(dvc_repo_root, target)

        if os.path.exists(full_path):
            size = get_dir_size(full_path)
            total_size_bytes += size
            size_mb = size / (1024 * 1024)
            tracked_items.append({"path": target, "size_mb": round(size_mb, 2)})
            print(f"[DVC] Found: {target} ({size_mb:.2f} MB)")
        else:
            print(f"[DVC] Skipping (not found): {full_path}")

    # Only attempt DVC operations if properly configured
    if dvc_available and git_available:
        print("\n[DVC] Step 2: Tracking outputs with DVC...")
        for item in tracked_items:
            result = run_cmd(f"dvc add {item['path']}", check=False)
            if result.returncode == 0:
                print(f"[DVC] Tracked: {item['path']}")
            else:
                print(f"[DVC] Could not track {item['path']}: {result.stderr.strip()}")

        # Push to DVC remote
        print("\n[DVC] Step 3: Pushing to remote storage...")
        push_result = run_cmd("dvc push", check=False)

        if push_result.returncode == 0:
            print("[DVC] Successfully pushed to remote storage")
            push_status = "success"
        elif "Everything is up to date" in (push_result.stderr + push_result.stdout):
            print("[DVC] Remote already up to date")
            push_status = "up_to_date"
        else:
            print(f"[DVC] Push warning: {push_result.stderr}")
            push_status = "failed"

        # Git commit DVC tracking files
        print("\n[DVC] Step 4: Committing DVC tracking files to git...")

        # Stage .dvc files and .gitignore updates
        run_cmd("git add *.dvc .gitignore data/**/*.dvc data/.gitignore 2>/dev/null || true")

        # Create commit message with run metadata
        commit_msg = f"""DVC: Auto-version pipeline outputs

Run ID: {run_id}
Timestamp: {timestamp}
Total Size: {total_size_bytes / (1024 * 1024):.2f} MB
Tracked Items: {len(tracked_items)}

Outputs:
- data/processed/: Per-dataset transformed outputs
- data/training/: Train/val/test splits by entity type
- data/analytics/: Merged data for bias detection
- data/metrics/: Validation and quality reports

Automated by Airflow er_data_pipeline DAG
Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"""

        # Check if there are changes to commit
        status_result = run_cmd("git status --porcelain")
        if status_result.stdout.strip():
            commit_result = run_cmd(
                f'git commit -m "{commit_msg}"',
                check=False,
            )
            if commit_result.returncode == 0:
                print("[DVC] Git commit created successfully")
                git_status = "committed"
            else:
                print(f"[DVC] Git commit warning: {commit_result.stderr}")
                git_status = "failed" if "nothing to commit" not in commit_result.stderr else "no_changes"
        else:
            print("[DVC] No changes to commit (data unchanged)")
            git_status = "no_changes"
    else:
        # Development mode - DVC/git not configured in Docker
        print("\n[DVC] DEVELOPMENT MODE: DVC/git not fully configured in container")
        print("[DVC] To version this data, run on host:")
        print("  cd Data-Pipeline && dvc add data/processed data/training data/analytics data/metrics")
        print("  dvc push && git add *.dvc && git commit -m 'DVC: version pipeline outputs'")
        push_status = "skipped_dev_mode"
        git_status = "skipped_dev_mode"

    # Step 4: Get DVC status and git hash
    print("\n[DVC] Step 4: Collecting version info...")

    git_hash_result = run_cmd("git rev-parse HEAD", check=False)
    git_hash = git_hash_result.stdout.strip() if git_hash_result.returncode == 0 else "unknown"

    dvc_status_result = run_cmd("dvc status", check=False)

    # Compile results
    version_info = {
        "timestamp": timestamp,
        "run_id": str(run_id),
        "git_commit": git_hash,
        "git_status": git_status,
        "dvc_push_status": push_status,
        "tracked_items": tracked_items,
        "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
        "dvc_status": dvc_status_result.stdout.strip() if dvc_status_result.returncode == 0 else "unavailable",
    }

    # Save version info
    version_file = f"{base_dir}/metrics/dvc_version_info.json"
    os.makedirs(os.path.dirname(version_file), exist_ok=True)
    with open(version_file, "w") as f:
        json.dump(version_info, f, indent=2)

    # Print summary
    print("\n" + "=" * 60)
    print("[DVC] DATA VERSIONING COMPLETE")
    print("=" * 60)
    print(f"  Git Commit: {git_hash[:12] if git_hash != 'unknown' else 'N/A'}")
    print(f"  Total Size: {version_info['total_size_mb']:.2f} MB")
    print(f"  Items Tracked: {len(tracked_items)}")
    print(f"  Push Status: {push_status}")
    print(f"  Git Status: {git_status}")
    print("=" * 60)

    ti.xcom_push(key="dvc_version_info", value=version_info)
    return version_info


# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    "owner": "Entity Resolution Team",
    "start_date": datetime(2026, 2, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="er_data_pipeline",
    default_args=default_args,
    description="Optimized Multi-Domain Pipeline for LoRA Training and Bias Analysis",
    schedule=None,
    catchup=False,
    tags=["entity-resolution", "multi-domain", "optimized"],
    doc_md="""
    # Entity Resolution Data Pipeline (Optimized)

    ## Architecture
    Dual-output pipeline serving two purposes:
    - **Training Branch**: Domain-specific data for LoRA adapter training
    - **Analytics Branch**: Merged data for cross-domain bias analysis

    ## Task Structure (~25 tasks)
    1. Load (6 parallel) → Validate All (1) → Transform (6 parallel)
    2. Organize Datasets (1) - Creates BOTH training + analytics outputs
    3. Validation (3 parallel): Training splits, Schema, Bias
    4. Quality Gate →  DVC Versioning Cloud  → Upload (conditional) 

    ## DVC Integration
    - Every pipeline run is automatically versioned with DVC
    - Data pushed to remote storage (../dvc-storage or GCS)
    - Git commits created with run metadata
    - Enables rollback to any previous run's data

    ## Outputs
    - `data/training/{person,product,publication}/train.csv` - For LoRA adapters
    - `data/analytics/merged_all.csv` - For bias detection
    """,
) as dag:

    # Start
    start = EmptyOperator(task_id="start")

    # Phase 1: Load (6 parallel)
    load_tasks = {}
    for dataset_name in DATASETS:
        load_tasks[dataset_name] = PythonOperator(
            task_id=f"load_{dataset_name}",
            python_callable=load_dataset,
            op_kwargs={"dataset_name": dataset_name},
        )
        start >> load_tasks[dataset_name]

    # Phase 2: Consolidated validation (1 task)
    validate_all_task = PythonOperator(
        task_id="validate_all_raw",
        python_callable=validate_all_raw_datasets,
    )
    for dataset_name in DATASETS:
        load_tasks[dataset_name] >> validate_all_task

    # Phase 3: Transform (6 parallel)
    transform_tasks = {}
    for dataset_name in DATASETS:
        transform_tasks[dataset_name] = PythonOperator(
            task_id=f"transform_{dataset_name}",
            python_callable=transform_dataset,
            op_kwargs={"dataset_name": dataset_name},
        )
        validate_all_task >> transform_tasks[dataset_name]

    # Phase 4: Organize datasets (1 task creates both outputs)
    organize_task = PythonOperator(
        task_id="organize_datasets",
        python_callable=organize_datasets,
    )
    for dataset_name in DATASETS:
        transform_tasks[dataset_name] >> organize_task

    # Phase 5: Validation (3 parallel)
    validate_splits_task = PythonOperator(
        task_id="validate_training_splits",
        python_callable=validate_training_splits,
    )
    schema_task = PythonOperator(
        task_id="schema_validation",
        python_callable=schema_validation,
    )
    bias_task = PythonOperator(
        task_id="bias_detection",
        python_callable=bias_detection,
    )
    organize_task >> [validate_splits_task, schema_task, bias_task]

    # Phase 6: Quality gate
    quality_gate_task = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_check,
    )
    [validate_splits_task, schema_task, bias_task] >> quality_gate_task

    # Phase 7: DVC Data Versioning (runs after either path completes)
    dvc_track_task = PythonOperator(
        task_id="dvc_track_and_version",
        python_callable=dvc_track_and_version,
        trigger_rule="none_failed_min_one_success",
    )

    # Cloud tasks
    upload_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )
    load_bq_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )
    verify_task = PythonOperator(
        task_id="verify_and_complete",
        python_callable=verify_and_complete,
    )

    # Final completion
    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="none_failed_min_one_success",
    )

    quality_gate_task >> dvc_track_task >> upload_gcs_task >> load_bq_task >> verify_task >> pipeline_complete


if __name__ == "__main__":
    dag.test()
