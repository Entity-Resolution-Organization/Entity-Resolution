"""
Entity Resolution Model Pipeline DAG

Triggered by er_data_pipeline or manually.
Flow: train → evaluate → bias_detection → sensitivity → quality_gate → push/alert
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

SCRIPTS_DIR = "/opt/airflow/scripts"
CONFIG_PATH = "/opt/airflow/config/training_config.yaml"

default_args = {
    "owner": "entity-resolution",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
}


# =============================================================================
# Task functions
# =============================================================================

def run_training(**context):
    import subprocess
    env = os.environ.copy()
    env["CONFIG_PATH"] = CONFIG_PATH
    result = subprocess.run(
        ["python", f"{SCRIPTS_DIR}/train.py"],
        env=env, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"train.py failed:\n{result.stderr}")

    # Push run_id to XCom so evaluate can attach to same MLflow run
    import re
    match = re.search(r"\[MLflow\] Run ID: (\S+)", result.stdout)
    if match:
        context["task_instance"].xcom_push(key="mlflow_run_id", value=match.group(1))


def run_evaluate(**context):
    import subprocess
    run_id = context["task_instance"].xcom_pull(
        task_ids="train", key="mlflow_run_id"
    )
    env = os.environ.copy()
    env["CONFIG_PATH"] = CONFIG_PATH
    if run_id:
        env["MLFLOW_RUN_ID"] = run_id

    result = subprocess.run(
        ["python", f"{SCRIPTS_DIR}/evaluate.py"],
        env=env, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"evaluate.py failed:\n{result.stderr}")


def run_bias_detection(**context):
    import subprocess
    env = os.environ.copy()
    env["CONFIG_PATH"] = CONFIG_PATH
    result = subprocess.run(
        ["python", f"{SCRIPTS_DIR}/bias_detection.py"],
        env=env, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"bias_detection.py failed:\n{result.stderr}")


def run_sensitivity(**context):
    import subprocess
    env = os.environ.copy()
    env["CONFIG_PATH"] = CONFIG_PATH
    result = subprocess.run(
        ["python", f"{SCRIPTS_DIR}/sensitivity_analysis.py"],
        env=env, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"sensitivity_analysis.py failed:\n{result.stderr}")


def check_quality_gate(**context) -> str:
    """Read test_metrics.json and branch GO or NO-GO."""
    import json
    import yaml

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    entity_types = config["data"]["entity_types"]
    thresholds   = config["validation"]
    base_dir     = config["output"]["base_dir"]

    for entity_type in entity_types:
        model_dir   = f"{base_dir}/{entity_type}"
        results_dir = f"{model_dir}/results"
        metrics_path = f"{results_dir}/test_metrics.json"

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

    print("[QualityGate] GO — all thresholds passed")
    return "push_to_registry"


def run_push_to_registry(**context):
    import subprocess
    env = os.environ.copy()
    env["CONFIG_PATH"] = CONFIG_PATH
    result = subprocess.run(
        ["python", f"{SCRIPTS_DIR}/push_to_registry.py"],
        env=env, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"push_to_registry.py failed:\n{result.stderr}")


def send_alert(**context):
    """NO-GO branch — log failure and optionally notify."""
    import yaml
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    msg = "[Pipeline] NO-GO: model failed quality gate. Check MLflow for details."
    print(msg)

    notif = config.get("notifications", {})
    if notif.get("enabled") and notif.get("slack_webhook_url"):
        import urllib.request, json
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
    description="Entity Resolution model train → evaluate → bias → quality gate → registry",
    schedule=None,          # triggered by er_data_pipeline or manually
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
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # DAG flow
    start >> train >> evaluate >> [bias, sensitivity] >> quality_gate
    quality_gate >> push_to_registry >> end
    quality_gate >> alert >> end