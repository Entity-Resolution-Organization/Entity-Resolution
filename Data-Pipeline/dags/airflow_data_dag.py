"""
Entity Resolution Data Pipeline - PERSON Domain

Pipeline creates training data and analytics outputs for PERSON entity resolution.

1. MODEL TRAINING (LoRA Adapter):
   - Output: data/training/person/train.csv, val.csv, test.csv

2. BIAS DETECTION (Cross-Dataset Analytics):
   - All PERSON datasets merged for bias analysis
   - Output: data/analytics/merged_all.csv

TASK STRUCTURE:
- N load tasks (parallel, driven by config)
- 1 validate_all_raw task
- N transform tasks (parallel)
- 1 organize_datasets task (creates training + analytics outputs)
- 3 validation tasks (parallel): training_splits, schema, bias
- Quality gate → DVC versioning → GCS upload → Done
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yaml

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# PYTHONPATH is set in docker-compose.yml; this is a fallback for safety
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
# CONFIGURATION — loaded from datasets.yaml (single source of truth)
# =============================================================================
BASE_DIR = "/opt/airflow/data"
CONFIG_PATH = "/opt/airflow/config/datasets.yaml"

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
DVC_GCS_BUCKET = os.getenv("DVC_GCS_BUCKET", "entity-resolution-dvc-bucket")

MIN_RECORDS_PER_DATASET = 100


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types. TODO: move to shared utils."""

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


def load_config():
    """Load dataset configuration from YAML."""
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_active_datasets():
    """Get active dataset names and their entity types from config."""
    config = load_config()
    active = config.get("active_datasets", [])
    entity_type_map = {}
    for name in active:
        dataset_conf = config["datasets"].get(name, {})
        entity_type_map[name] = dataset_conf.get("entity_type", "UNKNOWN")
    return active, entity_type_map


DATASETS, ENTITY_TYPE_MAP = get_active_datasets()


# =============================================================================
# PHASE 1: LOAD TASKS (parallel)
# =============================================================================
def load_dataset(dataset_name: str, **context):
    """Load a single dataset using its configured handler."""
    config = load_config()
    if dataset_name not in config["datasets"]:
        raise ValueError(f"Dataset {dataset_name} not found in config")

    dataset_config = config["datasets"][dataset_name]
    entity_type = ENTITY_TYPE_MAP.get(dataset_name, "UNKNOWN")
    print(f"[Load] {dataset_name} ({entity_type})")

    handler = get_dataset_handler(dataset_name, dataset_config)
    raw_df = handler.download()
    raw_df = handler.normalize_schema(raw_df)
    raw_df = handler.subsample(raw_df)
    raw_df["entity_type"] = entity_type
    raw_df["source_dataset"] = dataset_name

    print(f"[Load] {dataset_name}: {len(raw_df)} records")

    raw_dir = f"{BASE_DIR}/raw/{dataset_name}"
    os.makedirs(raw_dir, exist_ok=True)
    raw_path = f"{raw_dir}/data.csv"
    raw_df.to_csv(raw_path, index=False)

    ti = context["task_instance"]
    ti.xcom_push(key=f"{dataset_name}_raw_path", value=raw_path)
    ti.xcom_push(key=f"{dataset_name}_config", value=dataset_config)
    ti.xcom_push(key=f"{dataset_name}_entity_type", value=entity_type)
    return raw_path


# =============================================================================
# PHASE 2: CONSOLIDATED VALIDATION
# =============================================================================
def validate_all_raw_datasets(**context):
    """Validate ALL raw datasets in one consolidated task."""
    ti = context["task_instance"]
    validator = DatasetValidator(output_dir=f"{BASE_DIR}/metrics")

    print("[Validate All] Validating all datasets...")
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
            raw_path = f"{BASE_DIR}/raw/{dataset_name}/data.csv"

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

    output_path = f"{BASE_DIR}/metrics/validate_all_raw.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(
        f"\n[Validate All] Summary: {len(DATASETS) - len(failures)}/{len(DATASETS)} passed"
    )
    if failures:
        raise ValueError(f"Raw validation failed for: {failures}")

    ti.xcom_push(key="validation_results_path", value=output_path)
    return output_path


# =============================================================================
# PHASE 3: TRANSFORM TASKS (parallel)
# =============================================================================
def transform_dataset(dataset_name: str, **context):
    """Transform a single dataset: normalize, expand, generate pairs."""
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

    output_dir = f"{BASE_DIR}/processed/{dataset_name}"
    os.makedirs(output_dir, exist_ok=True)

    accounts_path = f"{output_dir}/accounts.csv"
    pairs_path = f"{output_dir}/pairs.csv"
    accounts_df.to_csv(accounts_path, index=False)
    pairs_df.to_csv(pairs_path, index=False)

    ti.xcom_push(key=f"{dataset_name}_accounts_path", value=accounts_path)
    ti.xcom_push(key=f"{dataset_name}_pairs_path", value=pairs_path)
    return {"accounts": accounts_path, "pairs": pairs_path}


# =============================================================================
# PHASE 4: ORGANIZE DATASETS
# =============================================================================
def organize_datasets(**context):
    """
    Create BOTH data organizations from transformed datasets:
    1. Analytics: All datasets merged for bias detection
    2. Training: Combined PERSON pairs with train/val/test splits
    """
    ti = context["task_instance"]

    print("[Organize] Creating training and analytics data organizations...")

    all_accounts = []
    all_pairs = []

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

        print(
            f"[Organize] {dataset_name}: {len(accounts_df)} accounts, {len(pairs_df)} pairs"
        )

    # =========================================================================
    # OUTPUT 1: ANALYTICS DATA (merged for bias detection)
    # =========================================================================
    analytics_dir = f"{BASE_DIR}/analytics"
    os.makedirs(analytics_dir, exist_ok=True)

    merged_accounts = pd.concat(all_accounts, ignore_index=True)
    merged_pairs = pd.concat(all_pairs, ignore_index=True)

    merged_accounts_path = f"{analytics_dir}/merged_all.csv"
    merged_pairs_path = f"{analytics_dir}/merged_pairs.csv"
    merged_accounts.to_csv(merged_accounts_path, index=False)
    merged_pairs.to_csv(merged_pairs_path, index=False)

    print(
        f"\n[Organize] ANALYTICS: {len(merged_accounts)} accounts, "
        f"{len(merged_pairs)} pairs"
    )

    # =========================================================================
    # OUTPUT 2: TRAINING DATA (person entity with splits)
    # =========================================================================
    training_dir = f"{BASE_DIR}/training"
    entity_dir = f"{training_dir}/person"
    os.makedirs(entity_dir, exist_ok=True)

    combined_pairs = merged_pairs.copy()

    # Deduplicate to prevent leakage
    original_count = len(combined_pairs)
    combined_pairs = combined_pairs.drop_duplicates(subset=["id1", "id2"], keep="first")
    total_pairs = len(combined_pairs)
    if original_count > total_pairs:
        print(f"[Organize] Deduplicated {original_count} -> {total_pairs}")

    combined_pairs.to_csv(f"{entity_dir}/all_pairs.csv", index=False)

    # Stratified train/val/test splits (70/15/15)
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
        combined_pairs = combined_pairs.sample(frac=1, random_state=42)
        n = total_pairs
        train_df = combined_pairs[: int(n * 0.7)]
        val_df = combined_pairs[int(n * 0.7) : int(n * 0.85)]
        test_df = combined_pairs[int(n * 0.85) :]

    train_df.to_csv(f"{entity_dir}/train.csv", index=False)
    val_df.to_csv(f"{entity_dir}/val.csv", index=False)
    test_df.to_csv(f"{entity_dir}/test.csv", index=False)

    # Training metadata — consumed by GCS upload and model pipeline
    entity_metadata = {
        "entity_type": "PERSON",
        "total_pairs": total_pairs,
        "train_count": len(train_df),
        "val_count": len(val_df),
        "test_count": len(test_df),
        "source_datasets": list(DATASETS),
        "timestamp": datetime.now().isoformat(),
    }
    with open(f"{entity_dir}/metadata.json", "w") as f:
        json.dump(entity_metadata, f, indent=2)

    print(f"\n[Organize] TRAINING (PERSON):")
    print(f"  train: {len(train_df)} | val: {len(val_df)} | test: {len(test_df)}")

    ti.xcom_push(key="merged_accounts_path", value=merged_accounts_path)
    ti.xcom_push(key="merged_pairs_path", value=merged_pairs_path)
    return {
        "analytics_accounts": len(merged_accounts),
        "analytics_pairs": len(merged_pairs),
        "training_pairs": total_pairs,
    }


# =============================================================================
# PHASE 5: VALIDATION TASKS (3 parallel)
# =============================================================================
def validate_training_splits(**context):
    """Validate training splits for data leakage and label balance."""
    ti = context["task_instance"]

    training_dir = f"{BASE_DIR}/training"
    entity_dir = f"{training_dir}/person"

    print("[Validate Splits] Checking training data quality...")
    validator = TrainingSplitValidator(output_dir=f"{BASE_DIR}/metrics")

    all_results = {}
    if not os.path.exists(f"{entity_dir}/train.csv"):
        print("[Validate Splits] No splits found for person, skipping")
    else:
        results = validator.validate_entity_splits(
            entity_type="person",
            entity_dir=entity_dir,
        )
        all_results["person"] = results
        print(
            f"[Validate Splits] person: {'PASSED' if results['success'] else 'FAILED'}"
        )

    total_entities = len(all_results)
    passed_entities = sum(1 for r in all_results.values() if r.get("success", False))
    success_rate = (passed_entities / total_entities * 100) if total_entities > 0 else 0

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

    output_path = f"{BASE_DIR}/metrics/training_split_validation.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, cls=NumpyEncoder)

    ti.xcom_push(key="split_validation_path", value=output_path)
    return output_path


def schema_validation(**context):
    """Validate schema of merged analytics data."""
    ti = context["task_instance"]

    print("[Schema] Validating analytics data schema...")
    validator = SchemaValidator(output_dir=f"{BASE_DIR}/metrics")

    accounts_path = f"{BASE_DIR}/analytics/merged_all.csv"
    pairs_path = f"{BASE_DIR}/analytics/merged_pairs.csv"

    accounts_df = pd.read_csv(accounts_path)
    pairs_df = pd.read_csv(pairs_path)

    results = validator.validate_all(accounts_df, pairs_df)
    output_path = validator.save_results(results, "schema_validation_results.json")

    print(
        f"[Schema] Validation: {'PASSED' if results['overall_success'] else 'FAILED'}"
    )

    ti.xcom_push(key="schema_results_path", value=output_path)
    return output_path


def bias_detection(**context):
    """Run bias detection on merged analytics data."""
    ti = context["task_instance"]

    print("[Bias] Analyzing bias in merged data...")
    accounts_path = f"{BASE_DIR}/analytics/merged_all.csv"
    pairs_path = f"{BASE_DIR}/analytics/merged_pairs.csv"

    accounts_df = pd.read_csv(accounts_path)
    pairs_df = pd.read_csv(pairs_path) if os.path.exists(pairs_path) else None

    detector = BiasDetector(output_dir=f"{BASE_DIR}/metrics")
    report = detector.generate_bias_report(accounts_df, pairs_df)

    overall_risk = report.get("summary", {}).get("overall_bias_risk", "UNKNOWN")
    print(f"[Bias] Overall risk: {overall_risk}")

    output_path = f"{BASE_DIR}/metrics/bias_report.json"
    ti.xcom_push(key="bias_report_path", value=output_path)
    return output_path


# =============================================================================
# PHASE 6: QUALITY GATE
# =============================================================================
def quality_gate_check(**context):
    """Aggregate all validation results and make go/no-go decision."""
    ti = context["task_instance"]

    print("[Quality Gate] Evaluating pipeline quality...")
    gate = QualityGate()

    def load_json(path):
        if path and os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        return None

    split_path = ti.xcom_pull(
        task_ids="validate_training_splits", key="split_validation_path"
    )
    schema_path = ti.xcom_pull(task_ids="schema_validation", key="schema_results_path")
    bias_path = ti.xcom_pull(task_ids="bias_detection", key="bias_report_path")

    split_results = load_json(split_path)
    schema_results = load_json(schema_path)
    bias_report = load_json(bias_path)

    decision = gate.evaluate(
        schema_results=schema_results,
        training_results=split_results,
        bias_results=bias_report,
    )

    output_path = f"{BASE_DIR}/metrics/quality_gate_results.json"
    with open(output_path, "w") as f:
        json.dump(decision, f, indent=2)

    print(f"\n[Quality Gate] Decision: {decision['decision']}")

    if decision["decision"] == "NO-GO":
        failure_msg = f"Quality gate BLOCKED: {decision.get('failures', [])}"
        print(f"[Quality Gate] {failure_msg}")
        raise ValueError(failure_msg)

    return decision["decision"]


# =============================================================================
# PHASE 7: DVC DATA VERSIONING
# =============================================================================
def dvc_track_and_version(**context):
    """Track all pipeline outputs with DVC and push to GCS."""
    ti = context["task_instance"]
    run_id = context.get("run_id", "unknown")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    dvc_repo_root = "/opt/airflow"

    print("[DVC] Starting data versioning...")
    print(f"[DVC] DVC remote: gs://{DVC_GCS_BUCKET}")

    # Validate environment
    if not os.path.isdir(f"{dvc_repo_root}/.dvc"):
        raise RuntimeError(
            "DVC not configured: .dvc/ not found. "
            "Ensure ../.dvc is mounted in docker-compose.yml"
        )
    if not os.path.isdir(f"{dvc_repo_root}/.git"):
        raise RuntimeError(
            "Git not found: .git/ not found. "
            "Ensure ../.git is mounted in docker-compose.yml"
        )

    def run_cmd(cmd, cwd=None, check=True):
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

    # Track directories with DVC
    dvc_targets = ["data/processed", "data/training", "data/analytics", "data/metrics"]
    tracked_items = []
    total_size_bytes = 0

    for target in dvc_targets:
        full_path = os.path.join(dvc_repo_root, target)
        if os.path.exists(full_path):
            size = sum(
                os.path.getsize(os.path.join(dp, f))
                for dp, _, fns in os.walk(full_path)
                for f in fns
                if os.path.exists(os.path.join(dp, f))
            )
            total_size_bytes += size
            size_mb = size / (1024 * 1024)
            tracked_items.append({"path": target, "size_mb": round(size_mb, 2)})
            print(f"[DVC] Found: {target} ({size_mb:.2f} MB)")

            result = run_cmd(f"dvc add {target}", check=False)
            if result.returncode == 0:
                print(f"[DVC] Tracked: {target}")
            else:
                print(f"[DVC] FAILED: {target} — {result.stderr.strip()}")

    if not tracked_items:
        print("[DVC] WARNING: No pipeline outputs found!")
        return {"status": "no_outputs"}

    # Push to GCS
    print(f"\n[DVC] Pushing to gs://{DVC_GCS_BUCKET}...")
    push_result = run_cmd("dvc push", check=False)
    combined = push_result.stdout + push_result.stderr

    if push_result.returncode == 0:
        push_status = "success"
    elif "Everything is up to date" in combined:
        push_status = "up_to_date"
    else:
        push_status = "failed"
    print(f"[DVC] Push status: {push_status}")

    # Save version metadata
    git_hash_result = run_cmd("git rev-parse HEAD", check=False)
    git_hash = (
        git_hash_result.stdout.strip() if git_hash_result.returncode == 0 else "unknown"
    )

    version_info = {
        "timestamp": timestamp,
        "run_id": str(run_id),
        "git_commit_at_run": git_hash,
        "dvc_push_status": push_status,
        "dvc_remote": f"gs://{DVC_GCS_BUCKET}",
        "tracked_items": tracked_items,
        "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
    }

    version_file = f"{BASE_DIR}/metrics/dvc_version_info.json"
    os.makedirs(os.path.dirname(version_file), exist_ok=True)
    with open(version_file, "w") as f:
        json.dump(version_info, f, indent=2)

    print(
        f"\n[DVC] Complete — {len(tracked_items)} items, "
        f"{version_info['total_size_mb']:.2f} MB"
    )
    print(f"[DVC] To commit on host:")
    print(f"  cd ENTITY-RESOLUTION")
    print(f"  git add Data-Pipeline/data/*.dvc Data-Pipeline/data/.gitignore")
    print(f"  git commit -m 'DVC: version run {run_id}'")

    return version_info


# =============================================================================
# PHASE 8: GCS UPLOAD
# =============================================================================
def upload_to_gcs(**context):
    """Upload data to GCS."""
    if not GCS_BUCKET:
        print("[GCS] GCS_BUCKET not configured, skipping upload")
        return []

    print(f"[GCS] Starting upload to gs://{GCS_BUCKET}/")

    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    uploads = []

    upload_files = [
        # Analytics
        (f"{BASE_DIR}/analytics/merged_all.csv", "analytics/merged_all.csv"),
        (f"{BASE_DIR}/analytics/merged_pairs.csv", "analytics/merged_pairs.csv"),
        # Metrics
        (f"{BASE_DIR}/metrics/bias_report.json", "metrics/bias_report.json"),
        (f"{BASE_DIR}/metrics/quality_gate_results.json", "metrics/quality_gate.json"),
        (
            f"{BASE_DIR}/metrics/schema_validation_results.json",
            "metrics/schema_validation.json",
        ),
        (
            f"{BASE_DIR}/metrics/training_split_validation.json",
            "metrics/training_split_validation.json",
        ),
        # Training splits
        (f"{BASE_DIR}/training/person/train.csv", "training/train.csv"),
        (f"{BASE_DIR}/training/person/val.csv", "training/val.csv"),
        (f"{BASE_DIR}/training/person/test.csv", "training/test.csv"),
        (f"{BASE_DIR}/training/person/metadata.json", "training/metadata.json"),
    ]

    for local_path, gcs_path in upload_files:
        if os.path.exists(local_path):
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            uploads.append(f"gs://{GCS_BUCKET}/{gcs_path}")
            print(f"[GCS] Uploaded -> {gcs_path}")

    print(f"[GCS] Completed: {len(uploads)} files")
    return uploads


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
    description="PERSON Entity Resolution Pipeline for LoRA Training and Bias Analysis",
    schedule=None,
    catchup=False,
    tags=["entity-resolution", "person"],
    doc_md="""
    # Entity Resolution Data Pipeline
    ## Architecture
    Dual-output pipeline:
    - **Training**: PERSON domain data for LoRA adapter training
    - **Analytics**: Merged data for cross-dataset bias analysis
    ## Flow
    Load (parallel) → Validate → Transform (parallel) → Organize →
    Validate (parallel) → Quality Gate → DVC → GCS Upload
    """,
) as dag:

    # =========================================================================
    # TASK DEFINITIONS
    # =========================================================================
    start = EmptyOperator(task_id="start")

    # Phase 1: Load (parallel)
    load_tasks = {}
    for dataset_name in DATASETS:
        load_tasks[dataset_name] = PythonOperator(
            task_id=f"load_{dataset_name}",
            python_callable=load_dataset,
            op_kwargs={"dataset_name": dataset_name},
        )

    # Phase 2: Consolidated validation
    validate_all_task = PythonOperator(
        task_id="validate_all_raw",
        python_callable=validate_all_raw_datasets,
    )

    # Phase 3: Transform (parallel)
    transform_tasks = {}
    for dataset_name in DATASETS:
        transform_tasks[dataset_name] = PythonOperator(
            task_id=f"transform_{dataset_name}",
            python_callable=transform_dataset,
            op_kwargs={"dataset_name": dataset_name},
        )

    # Phase 4: Organize datasets
    organize_task = PythonOperator(
        task_id="organize_datasets",
        python_callable=organize_datasets,
    )

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

    # Phase 6: Quality gate
    quality_gate_task = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_check,
    )

    # Phase 7: DVC versioning
    dvc_track_task = PythonOperator(
        task_id="dvc_track_and_version",
        python_callable=dvc_track_and_version,
    )

    # Phase 8: GCS upload
    upload_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="none_failed_min_one_success",
    )

    # =========================================================================
    # TASK DEPENDENCIES
    # =========================================================================
    for dataset_name in DATASETS:
        start >> load_tasks[dataset_name] >> validate_all_task

    for dataset_name in DATASETS:
        validate_all_task >> transform_tasks[dataset_name] >> organize_task

    organize_task >> [validate_splits_task, schema_task, bias_task]
    [validate_splits_task, schema_task, bias_task] >> quality_gate_task
    quality_gate_task >> dvc_track_task >> upload_gcs_task >> pipeline_complete
