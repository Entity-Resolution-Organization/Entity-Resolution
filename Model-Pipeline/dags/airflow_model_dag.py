"""
Entity Resolution Model Pipeline DAG

Triggered by er_data_pipeline or manually.

Flow:
    start
      → train
      → evaluate
      → [bias_detection, sensitivity_analysis]  (parallel)
      → rollback_check                           (compare vs previous)
      → quality_gate                             (branch GO / NO-GO)
      → push_to_registry | alert
      → end
"""

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

SCRIPTS_DIR = "/opt/airflow/scripts"
CONFIG_PATH  = "/opt/airflow/config/training_config.yaml"

default_args = {
    "owner":             "entity-resolution",
    "start_date":        datetime(2026, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
}


# =============================================================================
# Helpers
# =============================================================================

def _run_script(script: str, env_extra: dict = None, **context) -> str:
    import subprocess
    env = os.environ.copy()
    env["CONFIG_PATH"] = CONFIG_PATH
    if env_extra:
        env.update(env_extra)
    result = subprocess.run(
        ["python", f"{SCRIPTS_DIR}/{script}"],
        env=env, capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"{script} failed — see logs above")
    return result.stdout


def _load_config():
    import yaml
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# =============================================================================
# Task callables
# =============================================================================

def run_training(**context):
    import re
    stdout = _run_script("train.py", **context)
    match  = re.search(r"\[MLflow\] Run ID: (\S+)", stdout)
    if match:
        context["task_instance"].xcom_push(
            key="mlflow_run_id", value=match.group(1)
        )


def run_evaluate(**context):
    run_id = context["task_instance"].xcom_pull(
        task_ids="train", key="mlflow_run_id"
    )
    extra = {"MLFLOW_RUN_ID": run_id} if run_id else {}
    _run_script("evaluate.py", env_extra=extra, **context)


def run_bias_detection(**context):
    _run_script("bias_detection.py", **context)


def run_sensitivity(**context):
    _run_script("sensitivity_analysis.py", **context)


# =============================================================================
# Rollback check
# =============================================================================

def check_rollback(**context) -> str:
    """
    Compare current F1 against best previous GO run in MLflow.
    Checks ALL entity types before deciding — branches to
    'quality_gate' (OK) or 'rollback' (any entity degraded > 2%).
    """
    import mlflow

    config       = _load_config()
    entity_types = config["data"]["entity_types"]
    base_dir     = config["output"]["base_dir"]

    mlflow.set_tracking_uri(config["mlflow"]["tracking_uri"])

    rollback_needed = False

    for entity_type in entity_types:
        metrics_path = f"{base_dir}/{entity_type}/results/test_metrics.json"
        with open(metrics_path) as f:
            current_metrics = json.load(f)

        current_f1 = current_metrics.get("test_f1", 0)

        experiment = mlflow.get_experiment_by_name(
            config["mlflow"]["experiment_name"]
        )
        if experiment is None:
            print(f"[Rollback] No previous experiment — skipping {entity_type}")
            continue

        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string=(
                f"tags.entity_type = '{entity_type}' "
                f"and tags.quality_gate = 'GO'"
            ),
            order_by=["metrics.test_f1 DESC"],
            max_results=1,
        )

        if runs.empty:
            print(f"[Rollback] No previous GO run for {entity_type} — skipping")
            continue

        best_f1     = runs.iloc[0].get("metrics.test_f1", 0)
        degradation = best_f1 - current_f1

        print(
            f"[Rollback] {entity_type}: "
            f"current={current_f1:.4f} | previous best={best_f1:.4f} | "
            f"degradation={degradation:.4f}"
        )

        context["task_instance"].xcom_push(
            key=f"rollback_{entity_type}",
            value={
                "current_f1":       current_f1,
                "best_previous_f1": best_f1,
                "degradation":      float(degradation),
                "rollback_needed":  degradation > 0.02,
            },
        )

        if degradation > 0.02:
            print(f"[Rollback] TRIGGERED for {entity_type} — F1 dropped {degradation:.4f}")
            rollback_needed = True

    if rollback_needed:
        return "rollback"

    print("[Rollback] OK — all entity types passed, proceeding to quality gate")
    return "quality_gate"


def perform_rollback(**context):
    """Re-tag previous image version as latest in Artifact Registry."""
    import subprocess
    import mlflow

    config = _load_config()
    gcp    = config["gcp"]
    ar     = gcp["artifact_registry"]

    mlflow.set_tracking_uri(config["mlflow"]["tracking_uri"])
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    for entity_type in config["data"]["entity_types"]:
        image_name = ar["image_name"].format(entity_type=entity_type)
        image_base = (
            f"{ar['location']}-docker.pkg.dev/"
            f"{gcp['project_id']}/{ar['repository']}/{image_name}"
        )

        log_path = (
            f"{config['output']['base_dir']}/{entity_type}/results/registry_log.json"
        )
        previous = None
        if os.path.exists(log_path):
            with open(log_path) as f:
                previous = json.load(f).get("previous_version")

        if previous:
            print(f"[Rollback] Re-tagging {image_base}:{previous} → latest")
            cmd = (
                f"gcloud artifacts docker tags add "
                f"{image_base}:{previous} {image_base}:latest "
                f"--project={gcp['project_id']}"
            )
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            print(result.stdout or result.stderr)
        else:
            print(f"[Rollback] No previous version found for {entity_type}")

        with mlflow.start_run(
            run_name=f"rollback_{entity_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            tags={"entity_type": entity_type, "stage": "rollback"},
        ):
            mlflow.set_tag("rollback_to", previous or "none")
            mlflow.set_tag("reason",      "F1 degradation > 2%")


# =============================================================================
# Quality gate
# =============================================================================

def check_quality_gate(**context) -> str:
    config       = _load_config()
    entity_types = config["data"]["entity_types"]
    thresholds   = config["validation"]
    base_dir     = config["output"]["base_dir"]

    for entity_type in entity_types:
        metrics_path = f"{base_dir}/{entity_type}/results/test_metrics.json"
        with open(metrics_path) as f:
            metrics = json.load(f)

        checks = [
            metrics.get("test_f1",        0) >= thresholds["min_f1"],
            metrics.get("test_precision", 0) >= thresholds["min_precision"],
            metrics.get("test_recall",    0) >= thresholds["min_recall"],
            metrics.get("test_auc",       0) >= thresholds["min_auc"],
        ]

        if not all(checks):
            print(f"[QualityGate] NO-GO for {entity_type}: {metrics}")
            return "alert"

    print("[QualityGate] GO")
    return "push_to_registry"


# =============================================================================
# Push + alert
# =============================================================================

def run_push_to_registry(**context):
    _run_script("push_to_registry.py", **context)


def send_alert(**context):
    config = _load_config()
    ti     = context["task_instance"]

    extra = ""
    for et in config["data"]["entity_types"]:
        info = ti.xcom_pull(task_ids="rollback_check", key=f"rollback_{et}")
        if info:
            extra += (
                f"\n  {et}: F1={info['current_f1']:.4f} "
                f"(was {info['best_previous_f1']:.4f})"
            )

    msg = f"[Pipeline] NO-GO: quality gate failed or rollback triggered.{extra}\nCheck MLflow."
    print(msg)

    notif = config.get("notifications", {})
    if notif.get("enabled") and notif.get("slack_webhook_url"):
        import urllib.request
        payload = json.dumps({"text": msg}).encode()
        req = urllib.request.Request(
            notif["slack_webhook_url"],
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req)


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="er_model_pipeline",
    default_args=default_args,
    description="ER model: train → evaluate → bias/sensitivity → rollback check → quality gate → registry",
    schedule=None,
    catchup=False,
    tags=["entity-resolution", "model-pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")

    train = PythonOperator(
        task_id="train",
        python_callable=run_training,
    )

    evaluate = PythonOperator(
        task_id="evaluate",
        python_callable=run_evaluate,
    )

    bias = PythonOperator(
        task_id="bias_detection",
        python_callable=run_bias_detection,
    )

    sensitivity = PythonOperator(
        task_id="sensitivity_analysis",
        python_callable=run_sensitivity,
    )

    rollback_check = BranchPythonOperator(
        task_id="rollback_check",
        python_callable=check_rollback,
    )

    rollback = PythonOperator(
        task_id="rollback",
        python_callable=perform_rollback,
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=check_quality_gate,
    )

    push_to_registry = PythonOperator(
        task_id="push_to_registry",
        python_callable=run_push_to_registry,
    )

    alert = PythonOperator(
        task_id="alert",
        python_callable=send_alert,
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # Flow
    # sensitivity runs in parallel but does NOT block rollback_check
    start >> train >> evaluate >> bias >> rollback_check
    evaluate >> sensitivity  # informational only — doesn't gate main flow
    rollback_check >> rollback >> alert >> end
    rollback_check >> quality_gate
    quality_gate >> push_to_registry >> end
    quality_gate >> alert >> end