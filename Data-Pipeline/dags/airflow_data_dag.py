"""
Entity Resolution Data Pipeline - Production-Grade with Validation Gates

Architecture:
- 6 parallel load tasks (one per dataset)
- 6 parallel validation tasks (quality gate after load)
- 6 parallel transform tasks (dataset-specific outputs)
- Merge task (creates merged + entity-type aggregated datasets)
- Split task (train/val/test splits for model training)
- Validation tasks (schema + training splits)
- Quality gate (go/no-go decision)
- Conditional cloud upload (GCS + BigQuery)

Output Structure:
- data/processed/{dataset_name}/  - Per-dataset outputs
- data/processed/merged/          - Combined analytics data
- data/training/{entity_type}/    - Training splits per entity type
"""

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

# Add scripts to path
sys.path.insert(0, "/opt/airflow/scripts")

from bias_detection import BiasDetector
from data_validation import (DatasetValidator, QualityGate,
                             TrainingSplitValidator)
from dataset_factory import get_dataset_handler
from preprocessing import preprocess_dataset

from schema_validation import SchemaValidator

# =============================================================================
# CONFIGURATION
# =============================================================================

# Mode Configuration
LOCAL_MODE = os.getenv("LOCAL_MODE", "true").lower() == "true"
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "local")  # "local" or "cloud"
LOCAL_DATA_DIR = "/opt/airflow/data"
TMP_DIR = "/tmp/laundrograph"

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "entity-resolution-487121")
GCS_BUCKET = os.getenv("GCS_BUCKET", "entity-resolution-bucket-1")
BQ_DATASET = os.getenv("BQ_DATASET", "entity_resolution_bq")

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

# Validation thresholds
MIN_RECORDS_PER_DATASET = 100


def get_base_dir():
    """Get base directory based on mode."""
    return LOCAL_DATA_DIR if LOCAL_MODE else TMP_DIR


def load_config():
    """Load dataset configuration."""
    with open("/opt/airflow/config/datasets.yaml") as f:
        return yaml.safe_load(f)


# =============================================================================
# PHASE 1: PARALLEL LOAD TASKS
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
    print(f"[Mode] {'LOCAL' if LOCAL_MODE else 'PRODUCTION'}")

    try:
        handler = get_dataset_handler(dataset_name, dataset_config)
        raw_df = handler.download()
        raw_df = handler.normalize_schema(raw_df)
        raw_df = handler.subsample(raw_df)

        # Add metadata columns
        raw_df["entity_type"] = entity_type
        raw_df["source_dataset"] = dataset_name

        print(f"[Load] {dataset_name}: {len(raw_df)} records loaded")

        # Save raw data for this dataset
        base_dir = get_base_dir()
        raw_dir = f"{base_dir}/raw/{dataset_name}"
        os.makedirs(raw_dir, exist_ok=True)

        raw_path = f"{raw_dir}/data.csv"
        raw_df.to_csv(raw_path, index=False)

        # Save metadata
        metadata = {
            "dataset_name": dataset_name,
            "entity_type": entity_type,
            "record_count": len(raw_df),
            "timestamp": datetime.now().isoformat(),
            "config": dataset_config,
        }
        with open(f"{raw_dir}/metadata.json", "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        context["task_instance"].xcom_push(
            key=f"{dataset_name}_raw_path", value=raw_path
        )
        context["task_instance"].xcom_push(
            key=f"{dataset_name}_config", value=dataset_config
        )
        context["task_instance"].xcom_push(
            key=f"{dataset_name}_entity_type", value=entity_type
        )

        return raw_path

    except Exception as e:
        print(f"[ERROR] Failed to load {dataset_name}: {e}")
        raise


# =============================================================================
# PHASE 1.5: PER-DATASET VALIDATION GATES
# =============================================================================


def validate_raw_dataset(dataset_name: str, **context):
    """
    Validate raw dataset after load, before transform.
    This is a quality gate - fails the task if critical issues found.
    """
    ti = context["task_instance"]
    base_dir = get_base_dir()

    raw_path = ti.xcom_pull(
        task_ids=f"load_{dataset_name}", key=f"{dataset_name}_raw_path"
    )
    entity_type = ti.xcom_pull(
        task_ids=f"load_{dataset_name}", key=f"{dataset_name}_entity_type"
    )

    if not raw_path:
        raw_path = f"{base_dir}/raw/{dataset_name}/data.csv"

    print(f"[Validate Raw] {dataset_name}")
    print(f"[Validate Raw] Path: {raw_path}")

    validator = DatasetValidator(output_dir=f"{base_dir}/metrics")
    results = validator.validate_raw_dataset(
        dataset_name=dataset_name,
        data_path=raw_path,
        entity_type=entity_type or ENTITY_TYPE_MAP.get(dataset_name, "UNKNOWN"),
        min_records=MIN_RECORDS_PER_DATASET,
    )

    # Save validation results
    output_path = f"{base_dir}/metrics/validate_raw_{dataset_name}.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"[Validate Raw] {dataset_name} - Success: {results['success']}")

    if not results["success"]:
        failure_msg = f"Raw data validation failed for {dataset_name}: {results['critical_failures']}"
        print(f"[Validate Raw] FAILED: {failure_msg}")
        raise ValueError(failure_msg)

    ti.xcom_push(key=f"{dataset_name}_validation_results", value=results)
    return results


# =============================================================================
# PHASE 2: PARALLEL TRANSFORM TASKS
# =============================================================================


def transform_dataset(dataset_name: str, **context):
    """Transform a single dataset and save to dataset-specific directory."""
    ti = context["task_instance"]

    raw_path = ti.xcom_pull(
        task_ids=f"load_{dataset_name}", key=f"{dataset_name}_raw_path"
    )
    dataset_config = ti.xcom_pull(
        task_ids=f"load_{dataset_name}", key=f"{dataset_name}_config"
    )

    if not raw_path:
        raise ValueError(f"No raw data path found for {dataset_name}")

    raw_df = pd.read_csv(raw_path)
    entity_type = (
        raw_df["entity_type"].iloc[0] if "entity_type" in raw_df.columns else "UNKNOWN"
    )

    print(f"[Transform] {dataset_name} ({entity_type}): {len(raw_df)} records")

    # Run preprocessing
    accounts_df, pairs_df = preprocess_dataset(raw_df, dataset_config)

    # Ensure metadata columns
    accounts_df["entity_type"] = entity_type
    accounts_df["source_dataset"] = dataset_name
    pairs_df["entity_type"] = entity_type
    pairs_df["source_dataset"] = dataset_name

    print(
        f"[Transform] {dataset_name}: {len(accounts_df)} accounts, {len(pairs_df)} pairs"
    )

    # Save to dataset-specific directory
    base_dir = get_base_dir()
    output_dir = f"{base_dir}/processed/{dataset_name}"
    os.makedirs(output_dir, exist_ok=True)

    accounts_path = f"{output_dir}/accounts.csv"
    pairs_path = f"{output_dir}/pairs.csv"

    accounts_df.to_csv(accounts_path, index=False)
    pairs_df.to_csv(pairs_path, index=False)

    # Save metadata
    metadata = {
        "dataset_name": dataset_name,
        "entity_type": entity_type,
        "accounts_count": len(accounts_df),
        "pairs_count": len(pairs_df),
        "positive_pairs": (
            int((pairs_df["label"] == 1).sum()) if "label" in pairs_df.columns else 0
        ),
        "negative_pairs": (
            int((pairs_df["label"] == 0).sum()) if "label" in pairs_df.columns else 0
        ),
        "timestamp": datetime.now().isoformat(),
    }
    with open(f"{output_dir}/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"[Transform] Saved to {output_dir}/")

    # Push paths for downstream tasks
    ti.xcom_push(key=f"{dataset_name}_accounts_path", value=accounts_path)
    ti.xcom_push(key=f"{dataset_name}_pairs_path", value=pairs_path)
    ti.xcom_push(key=f"{dataset_name}_metadata", value=metadata)

    return {"accounts": accounts_path, "pairs": pairs_path}


# =============================================================================
# PHASE 3: MERGE TASK
# =============================================================================


def merge_datasets(**context):
    """
    Merge all datasets into:
    1. data/processed/merged/ - Combined analytics data
    2. data/training/{entity_type}/ - Entity-type aggregated for model training
    """
    ti = context["task_instance"]
    base_dir = get_base_dir()

    print("[Merge] Collecting all dataset outputs...")

    all_accounts = []
    all_pairs = []
    entity_type_pairs = {"PERSON": [], "PRODUCT": [], "PUBLICATION": []}

    for dataset_name in DATASETS:
        try:
            accounts_path = ti.xcom_pull(
                task_ids=f"transform_{dataset_name}",
                key=f"{dataset_name}_accounts_path",
            )
            pairs_path = ti.xcom_pull(
                task_ids=f"transform_{dataset_name}", key=f"{dataset_name}_pairs_path"
            )

            if not accounts_path or not pairs_path:
                print(f"[Merge] WARNING: No data for {dataset_name}, skipping")
                continue

            accounts_df = pd.read_csv(accounts_path)
            pairs_df = pd.read_csv(pairs_path)

            entity_type = ENTITY_TYPE_MAP.get(dataset_name, "UNKNOWN")

            print(
                f"[Merge] {dataset_name}: {len(accounts_df)} accounts, {len(pairs_df)} pairs"
            )

            all_accounts.append(accounts_df)
            all_pairs.append(pairs_df)

            # Collect pairs by entity type for training splits
            if entity_type in entity_type_pairs:
                entity_type_pairs[entity_type].append(pairs_df)

        except Exception as e:
            print(f"[Merge] ERROR loading {dataset_name}: {e}")
            continue

    if not all_accounts:
        raise ValueError("No datasets were successfully loaded for merging!")

    # ===================
    # STRUCTURE 1: Merged (for analytics/BigQuery)
    # ===================
    merged_dir = f"{base_dir}/processed/merged"
    os.makedirs(merged_dir, exist_ok=True)

    merged_accounts = pd.concat(all_accounts, ignore_index=True)
    merged_pairs = pd.concat(all_pairs, ignore_index=True)

    merged_accounts_path = f"{merged_dir}/accounts.csv"
    merged_pairs_path = f"{merged_dir}/er_pairs.csv"

    merged_accounts.to_csv(merged_accounts_path, index=False)
    merged_pairs.to_csv(merged_pairs_path, index=False)

    print(f"\n[Merge] MERGED OUTPUT:")
    print(f"  - Accounts: {len(merged_accounts)} records -> {merged_accounts_path}")
    print(f"  - Pairs: {len(merged_pairs)} records -> {merged_pairs_path}")

    # Entity distribution in merged data
    entity_dist = {}
    if "entity_type" in merged_accounts.columns:
        entity_dist = merged_accounts["entity_type"].value_counts().to_dict()
        print(f"  - Entity distribution: {entity_dist}")

    # Save merged metadata
    merged_metadata = {
        "total_accounts": len(merged_accounts),
        "total_pairs": len(merged_pairs),
        "datasets_merged": len(all_accounts),
        "entity_distribution": entity_dist,
        "source_datasets": (
            merged_accounts["source_dataset"].unique().tolist()
            if "source_dataset" in merged_accounts.columns
            else []
        ),
        "timestamp": datetime.now().isoformat(),
    }
    with open(f"{merged_dir}/metadata.json", "w") as f:
        json.dump(merged_metadata, f, indent=2)

    # ===================
    # STRUCTURE 2: Entity-Type Aggregated (for model training)
    # ===================
    training_dir = f"{base_dir}/training"

    for entity_type, pairs_list in entity_type_pairs.items():
        if not pairs_list:
            print(f"[Merge] No pairs for {entity_type}, skipping training output")
            continue

        entity_dir = f"{training_dir}/{entity_type.lower()}"
        os.makedirs(entity_dir, exist_ok=True)

        # Combine all pairs for this entity type
        entity_pairs = pd.concat(pairs_list, ignore_index=True)
        all_pairs_path = f"{entity_dir}/all_pairs.csv"
        entity_pairs.to_csv(all_pairs_path, index=False)

        print(f"\n[Merge] TRAINING OUTPUT ({entity_type}):")
        print(f"  - All pairs: {len(entity_pairs)} -> {all_pairs_path}")

        # Save entity-type metadata
        entity_metadata = {
            "entity_type": entity_type,
            "total_pairs": len(entity_pairs),
            "positive_pairs": (
                int((entity_pairs["label"] == 1).sum())
                if "label" in entity_pairs.columns
                else 0
            ),
            "negative_pairs": (
                int((entity_pairs["label"] == 0).sum())
                if "label" in entity_pairs.columns
                else 0
            ),
            "source_datasets": (
                entity_pairs["source_dataset"].unique().tolist()
                if "source_dataset" in entity_pairs.columns
                else []
            ),
            "timestamp": datetime.now().isoformat(),
        }
        with open(f"{entity_dir}/metadata.json", "w") as f:
            json.dump(entity_metadata, f, indent=2)

    # Push paths for downstream tasks
    ti.xcom_push(key="merged_accounts_path", value=merged_accounts_path)
    ti.xcom_push(key="merged_pairs_path", value=merged_pairs_path)
    ti.xcom_push(key="merged_metadata", value=merged_metadata)

    return merged_metadata


# =============================================================================
# PHASE 4: TRAIN/VAL/TEST SPLIT TASK
# =============================================================================


def create_training_splits(**context):
    """
    Create train/val/test splits for each entity type.
    Split ratio: 70% train, 15% val, 15% test (stratified by label)
    """
    base_dir = get_base_dir()
    training_dir = f"{base_dir}/training"

    print("[Split] Creating train/val/test splits...")

    split_summary = {}

    for entity_type in ["person", "product", "publication"]:
        entity_dir = f"{training_dir}/{entity_type}"
        all_pairs_path = f"{entity_dir}/all_pairs.csv"

        if not os.path.exists(all_pairs_path):
            print(f"[Split] No pairs file for {entity_type}, skipping")
            continue

        pairs_df = pd.read_csv(all_pairs_path)
        original_count = len(pairs_df)

        if original_count == 0:
            print(f"[Split] Empty pairs for {entity_type}, skipping")
            continue

        # Deduplicate pairs to prevent data leakage between splits
        # Keep first occurrence of each (id1, id2) pair
        pairs_df = pairs_df.drop_duplicates(subset=["id1", "id2"], keep="first")
        total_pairs = len(pairs_df)

        if original_count > total_pairs:
            print(
                f"\n[Split] {entity_type.upper()}: {original_count} pairs, "
                f"deduplicated to {total_pairs} unique pairs"
            )
        else:
            print(f"\n[Split] {entity_type.upper()}: {total_pairs} total pairs")

        # Stratified split by label if available
        if "label" in pairs_df.columns:
            # Separate positive and negative pairs
            positive = pairs_df[pairs_df["label"] == 1].copy()
            negative = pairs_df[pairs_df["label"] == 0].copy()

            # Shuffle
            positive = positive.sample(frac=1, random_state=42).reset_index(drop=True)
            negative = negative.sample(frac=1, random_state=42).reset_index(drop=True)

            # Split each class
            def split_data(df, train_ratio=0.7, val_ratio=0.15):
                n = len(df)
                train_end = int(n * train_ratio)
                val_end = int(n * (train_ratio + val_ratio))
                return df[:train_end], df[train_end:val_end], df[val_end:]

            pos_train, pos_val, pos_test = split_data(positive)
            neg_train, neg_val, neg_test = split_data(negative)

            # Combine and shuffle
            train_df = pd.concat([pos_train, neg_train]).sample(frac=1, random_state=42)
            val_df = pd.concat([pos_val, neg_val]).sample(frac=1, random_state=42)
            test_df = pd.concat([pos_test, neg_test]).sample(frac=1, random_state=42)

        else:
            # Random split without stratification
            pairs_df = pairs_df.sample(frac=1, random_state=42).reset_index(drop=True)
            n = len(pairs_df)
            train_end = int(n * 0.7)
            val_end = int(n * 0.85)

            train_df = pairs_df[:train_end]
            val_df = pairs_df[train_end:val_end]
            test_df = pairs_df[val_end:]

        # Save splits
        train_path = f"{entity_dir}/train.csv"
        val_path = f"{entity_dir}/val.csv"
        test_path = f"{entity_dir}/test.csv"

        train_df.to_csv(train_path, index=False)
        val_df.to_csv(val_path, index=False)
        test_df.to_csv(test_path, index=False)

        print(
            f"  - train.csv: {len(train_df)} pairs ({len(train_df)/total_pairs*100:.1f}%)"
        )
        print(f"  - val.csv: {len(val_df)} pairs ({len(val_df)/total_pairs*100:.1f}%)")
        print(
            f"  - test.csv: {len(test_df)} pairs ({len(test_df)/total_pairs*100:.1f}%)"
        )

        # Label distribution check
        if "label" in train_df.columns:
            train_pos = (train_df["label"] == 1).sum()
            train_neg = (train_df["label"] == 0).sum()
            print(
                f"  - Train label balance: {train_pos} pos / {train_neg} neg ({train_pos/(train_pos+train_neg)*100:.1f}% pos)"
            )

        split_summary[entity_type] = {
            "total": total_pairs,
            "train": len(train_df),
            "val": len(val_df),
            "test": len(test_df),
        }

        # Update metadata
        metadata_path = f"{entity_dir}/metadata.json"
        if os.path.exists(metadata_path):
            with open(metadata_path) as f:
                metadata = json.load(f)
        else:
            metadata = {}

        metadata["splits"] = split_summary[entity_type]
        metadata["split_timestamp"] = datetime.now().isoformat()

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    print(f"\n[Split] Summary: {split_summary}")

    context["task_instance"].xcom_push(key="split_summary", value=split_summary)
    return split_summary


# =============================================================================
# PHASE 5: VALIDATION TASKS
# =============================================================================


def schema_validation(**context):
    """Validate schema on merged data."""
    ti = context["task_instance"]
    base_dir = get_base_dir()

    merged_accounts_path = ti.xcom_pull(
        task_ids="merge_datasets", key="merged_accounts_path"
    )
    merged_pairs_path = ti.xcom_pull(task_ids="merge_datasets", key="merged_pairs_path")

    if not merged_accounts_path:
        merged_accounts_path = f"{base_dir}/processed/merged/accounts.csv"
        merged_pairs_path = f"{base_dir}/processed/merged/er_pairs.csv"

    accounts_df = pd.read_csv(merged_accounts_path)
    pairs_df = pd.read_csv(merged_pairs_path)

    print(
        f"[Schema Validation] Validating {len(accounts_df)} accounts, {len(pairs_df)} pairs"
    )

    metrics_dir = f"{base_dir}/metrics"
    os.makedirs(metrics_dir, exist_ok=True)

    validator = SchemaValidator(output_dir=metrics_dir)
    results = validator.validate_all(accounts_df, pairs_df)

    # Add multi-domain metadata
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

    validator.save_results(results)

    print(f"[Schema Validation] Overall success: {results['overall_success']}")

    ti.xcom_push(key="schema_validation_results", value=results)
    return results


def validate_training_splits(**context):
    """Validate training splits for all entity types."""
    ti = context["task_instance"]
    base_dir = get_base_dir()
    training_dir = f"{base_dir}/training"

    print("[Training Split Validation] Starting...")

    validator = TrainingSplitValidator(output_dir=f"{base_dir}/metrics")
    results = validator.validate_all_splits(training_dir)

    # Save results
    validator.save_results(results)

    print(f"[Training Split Validation] Overall success: {results['overall_success']}")
    print(
        f"[Training Split Validation] Success rate: {results['summary']['success_rate']}%"
    )

    if not results["overall_success"]:
        print(
            f"[Training Split Validation] Failures: {results['summary']['critical_failures']}"
        )

    ti.xcom_push(key="training_validation_results", value=results)
    return results


def bias_detection(**context):
    """Detect bias in merged data."""
    ti = context["task_instance"]
    base_dir = get_base_dir()

    merged_accounts_path = ti.xcom_pull(
        task_ids="merge_datasets", key="merged_accounts_path"
    )
    merged_pairs_path = ti.xcom_pull(task_ids="merge_datasets", key="merged_pairs_path")

    if not merged_accounts_path:
        merged_accounts_path = f"{base_dir}/processed/merged/accounts.csv"
        merged_pairs_path = f"{base_dir}/processed/merged/er_pairs.csv"

    accounts_df = pd.read_csv(merged_accounts_path)
    pairs_df = pd.read_csv(merged_pairs_path)

    print(
        f"[Bias Detection] Analyzing {len(accounts_df)} accounts, {len(pairs_df)} pairs"
    )

    # Log distributions
    if "entity_type" in accounts_df.columns:
        entity_dist = accounts_df["entity_type"].value_counts()
        print(f"[Bias Detection] Entity type distribution:")
        for et, count in entity_dist.items():
            print(f"  - {et}: {count} ({count/len(accounts_df)*100:.1f}%)")

    metrics_dir = f"{base_dir}/metrics"
    os.makedirs(metrics_dir, exist_ok=True)

    detector = BiasDetector(output_dir=metrics_dir)
    report = detector.generate_bias_report(accounts_df, pairs_df)

    # Add multi-domain analysis
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
    }

    # Save report
    report_path = f"{metrics_dir}/bias_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"[Bias Detection] Overall risk: {report['summary']['overall_bias_risk']}")

    ti.xcom_push(key="bias_report", value=report)
    return report


# =============================================================================
# PHASE 6: QUALITY GATE
# =============================================================================


def quality_gate_check(**context):
    """
    Quality gate - aggregates all validation results and makes go/no-go decision.
    Fails the task if critical issues are found.
    """
    ti = context["task_instance"]
    base_dir = get_base_dir()

    print("[Quality Gate] Evaluating validation results...")

    # Get all validation results
    schema_results = ti.xcom_pull(
        task_ids="schema_validation", key="schema_validation_results"
    )
    training_results = ti.xcom_pull(
        task_ids="validate_training_splits", key="training_validation_results"
    )
    bias_results = ti.xcom_pull(task_ids="bias_detection", key="bias_report")

    # Run quality gate evaluation
    gate = QualityGate(output_dir=f"{base_dir}/metrics")
    decision = gate.evaluate(
        schema_results=schema_results,
        training_results=training_results,
        bias_results=bias_results,
        fail_on_high_bias=False,  # Warn but don't fail on high bias
    )

    # Save results
    gate.save_results(decision)

    print(f"[Quality Gate] Decision: {decision['decision']}")
    print(f"[Quality Gate] Passed: {decision['passed']}")

    if decision["warnings"]:
        print(f"[Quality Gate] Warnings: {decision['warnings']}")

    if not decision["passed"]:
        failure_msg = f"Quality gate failed: {decision['failures']}"
        print(f"[Quality Gate] FAILED: {failure_msg}")
        raise ValueError(failure_msg)

    ti.xcom_push(key="quality_gate_decision", value=decision)
    return decision


# =============================================================================
# PHASE 7: EXECUTION PATH BRANCHING
# =============================================================================


def decide_execution_path(**context):
    """
    Branch task to decide between local and cloud execution paths.
    Returns the task_id to execute next.
    """
    mode = os.getenv("EXECUTION_MODE", "local")
    print(f"[Execution Path] Mode: {mode}")

    if mode == "cloud":
        print("[Execution Path] Proceeding to cloud upload...")
        return "cloud_tasks.upload_to_gcs"
    else:
        print("[Execution Path] Skipping cloud tasks, completing locally...")
        return "pipeline_complete_local"


# =============================================================================
# PHASE 8: CLOUD UPLOAD TASKS
# =============================================================================


def upload_to_gcs(**context):
    """Upload both merged and training data to GCS."""
    print(f"[GCS Upload] Starting upload to gs://{GCS_BUCKET}/")

    try:
        from google.cloud import storage
    except ImportError:
        print("[GCS Upload] ERROR: google-cloud-storage not installed")
        raise ImportError("google-cloud-storage package required for cloud mode")

    base_dir = get_base_dir()
    ti = context["task_instance"]

    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
    except Exception as e:
        print(f"[GCS Upload] ERROR: Failed to connect to GCS: {e}")
        raise

    uploads = []
    timestamp = datetime.now().strftime("%Y-%m-%d")

    # Files to upload
    upload_files = [
        # Merged data
        (
            f"{base_dir}/processed/merged/accounts.csv",
            f"processed/{timestamp}/accounts.csv",
        ),
        (
            f"{base_dir}/processed/merged/er_pairs.csv",
            f"processed/{timestamp}/er_pairs.csv",
        ),
        (
            f"{base_dir}/processed/merged/metadata.json",
            f"processed/{timestamp}/metadata.json",
        ),
        # Metrics
        (
            f"{base_dir}/metrics/schema_validation_results.json",
            f"metrics/{timestamp}/schema_validation.json",
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
        entity_dir = f"{base_dir}/training/{entity_type}"
        if os.path.exists(entity_dir):
            for filename in [
                "all_pairs.csv",
                "train.csv",
                "val.csv",
                "test.csv",
                "metadata.json",
            ]:
                local_path = f"{entity_dir}/{filename}"
                if os.path.exists(local_path):
                    gcs_path = f"training/{timestamp}/{entity_type}/{filename}"
                    upload_files.append((local_path, gcs_path))

    # Perform uploads
    for local_path, gcs_path in upload_files:
        if os.path.exists(local_path):
            try:
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_path)
                print(f"[GCS] Uploaded {local_path} -> gs://{GCS_BUCKET}/{gcs_path}")
                uploads.append(f"gs://{GCS_BUCKET}/{gcs_path}")
            except Exception as e:
                print(f"[GCS] ERROR uploading {local_path}: {e}")
        else:
            print(f"[GCS] Skipping (not found): {local_path}")

    print(f"[GCS Upload] Completed: {len(uploads)} files uploaded")

    ti.xcom_push(key="gcs_uploads", value=uploads)
    ti.xcom_push(key="gcs_timestamp", value=timestamp)
    return uploads


def load_accounts_to_bigquery(**context):
    """Load accounts data to BigQuery."""
    print("[BigQuery] Loading accounts table...")

    try:
        from google.cloud import bigquery
    except ImportError:
        print("[BigQuery] ERROR: google-cloud-bigquery not installed")
        raise ImportError("google-cloud-bigquery package required for cloud mode")

    ti = context["task_instance"]
    base_dir = get_base_dir()
    timestamp = ti.xcom_pull(task_ids="cloud_tasks.upload_to_gcs", key="gcs_timestamp")

    try:
        client = bigquery.Client()
    except Exception as e:
        print(f"[BigQuery] ERROR: Failed to connect: {e}")
        raise

    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.accounts"

    # Define schema
    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("dob", "STRING"),
        bigquery.SchemaField("cluster_id", "STRING"),
        bigquery.SchemaField("entity_type", "STRING"),
        bigquery.SchemaField("source_dataset", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load from local file
    accounts_path = f"{base_dir}/processed/merged/accounts.csv"
    with open(accounts_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    load_job.result()  # Wait for completion

    table = client.get_table(table_id)
    print(f"[BigQuery] Loaded {table.num_rows} rows to {table_id}")

    ti.xcom_push(key="accounts_row_count", value=table.num_rows)
    return table.num_rows


def load_pairs_to_bigquery(**context):
    """Load pairs data to BigQuery."""
    print("[BigQuery] Loading pairs table...")

    try:
        from google.cloud import bigquery
    except ImportError:
        print("[BigQuery] ERROR: google-cloud-bigquery not installed")
        raise ImportError("google-cloud-bigquery package required for cloud mode")

    ti = context["task_instance"]
    base_dir = get_base_dir()

    try:
        client = bigquery.Client()
    except Exception as e:
        print(f"[BigQuery] ERROR: Failed to connect: {e}")
        raise

    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.er_pairs"

    # Define schema
    schema = [
        bigquery.SchemaField("id1", "STRING"),
        bigquery.SchemaField("id2", "STRING"),
        bigquery.SchemaField("name1", "STRING"),
        bigquery.SchemaField("name2", "STRING"),
        bigquery.SchemaField("address1", "STRING"),
        bigquery.SchemaField("address2", "STRING"),
        bigquery.SchemaField("label", "INTEGER"),
        bigquery.SchemaField("entity_type", "STRING"),
        bigquery.SchemaField("source_dataset", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load from local file
    pairs_path = f"{base_dir}/processed/merged/er_pairs.csv"
    with open(pairs_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    load_job.result()  # Wait for completion

    table = client.get_table(table_id)
    print(f"[BigQuery] Loaded {table.num_rows} rows to {table_id}")

    ti.xcom_push(key="pairs_row_count", value=table.num_rows)
    return table.num_rows


def verify_bigquery_load(**context):
    """Verify BigQuery load by comparing row counts."""
    print("[BigQuery Verify] Checking row counts...")

    ti = context["task_instance"]
    base_dir = get_base_dir()

    # Get expected counts from merged metadata
    merged_metadata_path = f"{base_dir}/processed/merged/metadata.json"
    with open(merged_metadata_path) as f:
        metadata = json.load(f)

    expected_accounts = metadata["total_accounts"]
    expected_pairs = metadata["total_pairs"]

    # Get actual counts from BigQuery
    actual_accounts = ti.xcom_pull(
        task_ids="cloud_tasks.load_accounts_bq", key="accounts_row_count"
    )
    actual_pairs = ti.xcom_pull(
        task_ids="cloud_tasks.load_pairs_bq", key="pairs_row_count"
    )

    print(
        f"[BigQuery Verify] Accounts: expected={expected_accounts}, actual={actual_accounts}"
    )
    print(f"[BigQuery Verify] Pairs: expected={expected_pairs}, actual={actual_pairs}")

    # Check within 5% tolerance
    accounts_diff = abs(actual_accounts - expected_accounts) / expected_accounts
    pairs_diff = abs(actual_pairs - expected_pairs) / expected_pairs

    if accounts_diff > 0.05:
        raise ValueError(
            f"Accounts count mismatch: expected {expected_accounts}, got {actual_accounts} ({accounts_diff*100:.1f}% difference)"
        )

    if pairs_diff > 0.05:
        raise ValueError(
            f"Pairs count mismatch: expected {expected_pairs}, got {actual_pairs} ({pairs_diff*100:.1f}% difference)"
        )

    print("[BigQuery Verify] All counts verified successfully!")

    return {
        "accounts": {"expected": expected_accounts, "actual": actual_accounts},
        "pairs": {"expected": expected_pairs, "actual": actual_pairs},
    }


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
    description="Production-Grade Pipeline with Validation Gates and Cloud Integration",
    schedule=None,
    catchup=False,
    tags=["entity-resolution", "data-pipeline", "multi-domain", "production"],
    doc_md="""
    # Entity Resolution Data Pipeline

    ## Overview
    Production-grade pipeline for multi-domain entity resolution data processing.

    ## Features
    - 6 parallel dataset processing (PERSON, PRODUCT, PUBLICATION)
    - Per-dataset validation gates
    - Training split validation (70/15/15)
    - Quality gate with go/no-go decision
    - Conditional cloud upload (GCS + BigQuery)

    ## Execution Modes
    - **local**: Process data, skip cloud upload
    - **cloud**: Process data + upload to GCS + BigQuery

    ## Environment Variables
    - `EXECUTION_MODE`: "local" or "cloud"
    - `LOCAL_MODE`: "true" for synthetic data
    - `GCP_PROJECT_ID`: GCP project ID
    - `GCS_BUCKET`: GCS bucket name
    - `BQ_DATASET`: BigQuery dataset name
    """,
) as dag:

    # ===================
    # START
    # ===================
    start = EmptyOperator(task_id="start")

    # ===================
    # PARALLEL LOAD + VALIDATE + TRANSFORM (per dataset)
    # ===================
    load_tasks = {}
    validate_raw_tasks = {}
    transform_tasks = {}

    for dataset_name in DATASETS:
        # Load task
        load_tasks[dataset_name] = PythonOperator(
            task_id=f"load_{dataset_name}",
            python_callable=load_dataset,
            op_kwargs={"dataset_name": dataset_name},
        )

        # Validate raw task (quality gate)
        validate_raw_tasks[dataset_name] = PythonOperator(
            task_id=f"validate_raw_{dataset_name}",
            python_callable=validate_raw_dataset,
            op_kwargs={"dataset_name": dataset_name},
        )

        # Transform task
        transform_tasks[dataset_name] = PythonOperator(
            task_id=f"transform_{dataset_name}",
            python_callable=transform_dataset,
            op_kwargs={"dataset_name": dataset_name},
        )

        # Dependencies: start >> load >> validate >> transform
        start >> load_tasks[dataset_name]
        load_tasks[dataset_name] >> validate_raw_tasks[dataset_name]
        validate_raw_tasks[dataset_name] >> transform_tasks[dataset_name]

    # ===================
    # MERGE TASK (waits for all transforms)
    # ===================
    merge_task = PythonOperator(
        task_id="merge_datasets",
        python_callable=merge_datasets,
    )
    for dataset_name in DATASETS:
        transform_tasks[dataset_name] >> merge_task

    # ===================
    # TRAINING SPLITS TASK
    # ===================
    split_task = PythonOperator(
        task_id="create_training_splits",
        python_callable=create_training_splits,
    )
    merge_task >> split_task

    # ===================
    # VALIDATION TASKS (parallel after merge/split)
    # ===================
    schema_task = PythonOperator(
        task_id="schema_validation",
        python_callable=schema_validation,
    )

    validate_splits_task = PythonOperator(
        task_id="validate_training_splits",
        python_callable=validate_training_splits,
    )

    bias_task = PythonOperator(
        task_id="bias_detection",
        python_callable=bias_detection,
    )

    merge_task >> schema_task
    split_task >> validate_splits_task
    merge_task >> bias_task

    # ===================
    # QUALITY GATE (waits for all validations)
    # ===================
    quality_gate_task = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_check,
    )
    [schema_task, validate_splits_task, bias_task] >> quality_gate_task

    # ===================
    # EXECUTION PATH BRANCHING
    # ===================
    decide_path_task = BranchPythonOperator(
        task_id="decide_execution_path",
        python_callable=decide_execution_path,
    )
    quality_gate_task >> decide_path_task

    # ===================
    # LOCAL COMPLETION
    # ===================
    pipeline_complete_local = EmptyOperator(
        task_id="pipeline_complete_local",
    )
    decide_path_task >> pipeline_complete_local

    # ===================
    # CLOUD TASKS (TaskGroup)
    # ===================
    with TaskGroup(
        "cloud_tasks", tooltip="Cloud upload and BigQuery load"
    ) as cloud_group:

        upload_gcs_task = PythonOperator(
            task_id="upload_to_gcs",
            python_callable=upload_to_gcs,
        )

        load_accounts_bq_task = PythonOperator(
            task_id="load_accounts_bq",
            python_callable=load_accounts_to_bigquery,
        )

        load_pairs_bq_task = PythonOperator(
            task_id="load_pairs_bq",
            python_callable=load_pairs_to_bigquery,
        )

        verify_bq_task = PythonOperator(
            task_id="verify_bigquery",
            python_callable=verify_bigquery_load,
        )

        upload_gcs_task >> [load_accounts_bq_task, load_pairs_bq_task] >> verify_bq_task

    decide_path_task >> cloud_group

    # ===================
    # FINAL COMPLETION
    # ===================
    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="none_failed_min_one_success",
    )

    [pipeline_complete_local, cloud_group] >> pipeline_complete


if __name__ == "__main__":
    dag.test()
