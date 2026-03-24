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

Architecture:
    - Training runs on Vertex AI (GPU, ephemeral VM)
    - Post-training scripts run in er-trainer Docker container via DockerOperator
    - All scripts read config from mounted volume, write results to mounted volume
    - MLflow server runs on airflow-vm (always-on)
"""

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomJobOperator,
)
from airflow.operators.bash import BashOperator
from docker.types import Mount

# =============================================================================
# Constants
# =============================================================================

PROJECT_ID   = os.environ.get("GCP_PROJECT_ID", "entity-resolution-487121")
REGION       = os.environ.get("GCP_REGION", "us-central1")
IMAGE        = os.environ.get(
    "TRAINER_IMAGE",
    f"{REGION}-docker.pkg.dev/{PROJECT_ID}/ml-models/er-trainer:latest",
)
CONFIG_PATH_HOST      = "/opt/airflow/config/training_config.yaml"
CONFIG_DIR_HOST       = "/opt/airflow/config"
MODELS_DIR_HOST       = "/opt/airflow/models"
DATA_DIR_HOST         = "/opt/airflow/data"
CACHE_DIR_HOST        = "/opt/airflow/cache"
MLFLOW_URI            = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_URI_VERTEX     = "http://34.123.172.119:5000"

# Container paths (inside er-trainer image)
CONFIG_PATH_CONTAINER = "/app/config/training_config.yaml"

default_args = {
    "owner":             "entity-resolution",
    "start_date":        datetime(2026, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=6),
}


# =============================================================================
# Helpers
# =============================================================================

def _load_config():
    import yaml
    with open(CONFIG_PATH_HOST) as f:
        return yaml.safe_load(f)


def _docker_mounts():
    """Shared volume mounts for all DockerOperator tasks."""
    return [
        Mount(target="/app/config",  source=CONFIG_DIR_HOST,  type="bind"),
        Mount(target="/app/models",  source=MODELS_DIR_HOST,  type="bind"),
        Mount(target="/app/data",    source=DATA_DIR_HOST,    type="bind"),
        Mount(target="/app/cache",   source=CACHE_DIR_HOST,   type="bind"),
    ]


def _docker_env():
    """Shared environment variables for all DockerOperator tasks."""
    return {
        "CONFIG_PATH":          CONFIG_PATH_CONTAINER,
        "MLFLOW_TRACKING_URI":  MLFLOW_URI,
    }


# =============================================================================
# Rollback check
# =============================================================================

def check_rollback(**context) -> str:
    """
    Compare current F1 against best previous GO run in MLflow.
    Checks ALL entity types — branches to 'quality_gate' or 'rollback'.
    """
    import mlflow

    config       = _load_config()
    entity_types = config["data"]["entity_types"]
    base_dir     = MODELS_DIR_HOST

    mlflow.set_tracking_uri(MLFLOW_URI)

    rollback_needed = False

    for entity_type in entity_types:
        metrics_path = f"{base_dir}/{entity_type}/results/test_metrics.json"
        if not os.path.exists(metrics_path):
            print(f"[Rollback] No metrics found for {entity_type} — skipping")
            continue

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
            f"current={current_f1:.4f} | best={best_f1:.4f} | "
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

    print("[Rollback] OK — all entity types passed")
    return "quality_gate"


def perform_rollback(**context):
    """Re-tag previous image version as latest in Artifact Registry."""
    import subprocess
    import mlflow

    config = _load_config()
    gcp    = config["gcp"]
    ar     = gcp["artifact_registry"]

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    for entity_type in config["data"]["entity_types"]:
        image_name = ar["image_name"].format(entity_type=entity_type)
        image_base = (
            f"{ar['location']}-docker.pkg.dev/"
            f"{gcp['project_id']}/{ar['repository']}/{image_name}"
        )

        log_path = f"{MODELS_DIR_HOST}/{entity_type}/results/registry_log.json"
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
            print(f"[Rollback] No previous version for {entity_type}")

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
    config     = _load_config()
    thresholds = config["validation"]

    for entity_type in config["data"]["entity_types"]:
        metrics_path = f"{MODELS_DIR_HOST}/{entity_type}/results/test_metrics.json"
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

# def run_push_to_registry(**context):
#     """Push runs on Airflow VM directly — needs gcloud which is on the VM."""
#     import subprocess
#     env = os.environ.copy()
#     env["CONFIG_PATH"]         = CONFIG_PATH_HOST
#     env["MLFLOW_TRACKING_URI"] = MLFLOW_URI
#
#     result = subprocess.run(
#         ["python", "/opt/airflow/scripts/push_to_registry.py"],
#         env=env, capture_output=True, text=True,
#     )
#     print(result.stdout)
#     if result.returncode != 0:
#         print(result.stderr)
#         raise RuntimeError("push_to_registry.py failed")


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

    msg = f"[Pipeline] NO-GO: quality gate failed or rollback triggered.{extra}\nCheck MLflow: {MLFLOW_URI}"
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
    description="ER model: Vertex AI train → Docker evaluate/bias → rollback → quality gate → registry",
    schedule=None,
    catchup=False,
    tags=["entity-resolution", "model-pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -------------------------------------------------------------------------
    # Train — Vertex AI custom job
    # CI/CD builds image → Airflow submits job → Vertex AI VM trains → shuts down
    # Model weights uploaded to GCS by train.py automatically
    # -------------------------------------------------------------------------
    train = CreateCustomJobOperator(
        task_id="train",
        project_id=PROJECT_ID,
        region=REGION,
        custom_job={
            "display_name": "er-train-custom-job",
            "job_spec": {
                "worker_pool_specs": [
                    {
                        "machine_spec": {
                            "machine_type": "n1-standard-8",
                            "accelerator_type": "NVIDIA_TESLA_T4",
                            "accelerator_count": 1,
                        },
                        "replica_count": 1,
                        "container_spec": {
                            "image_uri": IMAGE,
                            "env": [
                                {"name": "CONFIG_PATH", "value": CONFIG_PATH_CONTAINER},
                                {"name": "MLFLOW_TRACKING_URI", "value": MLFLOW_URI_VERTEX},
                            ],
                        },
                    }
                ],
            },
        },
    )

    # -------------------------------------------------------------------------
    # Evaluate — runs in er-trainer container via DockerOperator
    # Downloads model weights from GCS (uploaded by train.py)
    # -------------------------------------------------------------------------
    evaluate = DockerOperator(
        task_id="evaluate",
        image=IMAGE,
        command="python scripts/evaluate.py",
        environment=_docker_env(),
        mounts=_docker_mounts(),
        docker_url="unix://var/run/docker.sock",
        network_mode="host",     # reach MLflow on localhost:5000
        auto_remove="success",
        device_requests=[        # use GPU if available
            {"capabilities": [["gpu"]], "count": -1}
        ],
    )

    # -------------------------------------------------------------------------
    # Bias detection — parallel with sensitivity, gates rollback_check
    # -------------------------------------------------------------------------
    bias = DockerOperator(
        task_id="bias_detection",
        image=IMAGE,
        command="python scripts/bias_detection.py",
        environment=_docker_env(),
        mounts=_docker_mounts(),
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        auto_remove="success",
    )

    # -------------------------------------------------------------------------
    # Sensitivity — parallel, does NOT block rollback_check
    # -------------------------------------------------------------------------
    sensitivity = DockerOperator(
        task_id="sensitivity_analysis",
        image=IMAGE,
        command="python scripts/sensitivity_analysis.py",
        environment=_docker_env(),
        mounts=_docker_mounts(),
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        auto_remove="success",
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

    push_to_registry = BashOperator(
        task_id="push_to_registry",
        bash_command=(
            f"CONFIG_PATH={CONFIG_PATH_HOST} "
            f"MLFLOW_TRACKING_URI={MLFLOW_URI} "
            "python /opt/airflow/scripts/push_to_registry.py"
        ),
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

    # -------------------------------------------------------------------------
    # Flow
    # -------------------------------------------------------------------------
    start >> train >> evaluate >> bias >> rollback_check
    evaluate >> sensitivity   # parallel, informational only
    rollback_check >> rollback >> alert >> end
    rollback_check >> quality_gate
    quality_gate >> push_to_registry >> end
    quality_gate >> alert >> end