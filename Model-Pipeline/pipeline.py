"""
Vertex AI Pipeline (KFP v2) — Entity Resolution Model Training
==============================================================

Replaces: Airflow er_model_pipeline DAG + aiplatform.CustomJob

Pipeline flow:
    train (GPU: n1-standard-8 + T4)
      → evaluate (CPU)
      → [bias_detection ∥ sensitivity_analysis] (CPU, parallel)
      → rollback_check
          ├─[ROLLBACK] → rollback → alert
          └─[GO]       → quality_gate
                            ├─[NO-GO] → alert
                            └─[GO]    → push_to_registry
                                          → register_model
                                              → deploy_to_endpoint

Inter-component data passing:
    All scripts share state via GCS under two prefixes:
      gs://{bucket}/models/{entity_type}/final_model/         ← train.py uploads here
      gs://{bucket}/pipeline-results/models/{entity_type}/    ← evaluate_op syncs here

Usage:
    python pipeline.py --compile
    python pipeline.py --compile --upload
    python pipeline.py --run

Dependencies:
    kfp==2.7.0
    google-cloud-aiplatform>=1.48.0
    google-cloud-storage
"""

import argparse
import os
from datetime import datetime

from kfp import compiler, dsl
from kfp.dsl import component
from google.cloud import aiplatform
from utils import get_mlflow_uri

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECT_ID     = os.environ.get("GCP_PROJECT_ID", "entity-resolution-487121")
REGION         = os.environ.get("GCP_REGION",      "us-central1")
GCS_BUCKET     = "entity-resolution-bucket-1"
STAGING_BUCKET = "gs://entity-resolution-staging-bucket"
PIPELINE_ROOT  = f"gs://{GCS_BUCKET}/pipeline-root"
PIPELINE_YAML  = "pipeline.yaml"
GCS_PIPELINE_YAML = f"gs://{GCS_BUCKET}/pipelines/pipeline.yaml"

TRAINER_IMAGE = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/ml-models/er-trainer:latest"
CONFIG_PATH   = "/app/config/training_config.yaml"


# =============================================================================
# Components
# =============================================================================

@component(base_image=TRAINER_IMAGE)
def train_op(mlflow_tracking_uri: str) -> str:
    """
    Fine-tunes DeBERTa+LoRA on GPU via train.py.
    train.py uploads weights to gs://{bucket}/models/{entity_type}/final_model/.
    """
    import os, subprocess
    env = {
        **os.environ,
        "CONFIG_PATH":         "/app/config/training_config.yaml",
        "MLFLOW_TRACKING_URI": mlflow_tracking_uri,
    }
    res = subprocess.run(["python", "/app/scripts/train.py"], env=env, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"train.py failed (exit {res.returncode})")
    return "done"


@component(base_image=TRAINER_IMAGE)
def evaluate_op(mlflow_tracking_uri: str, gcs_bucket: str) -> str:
    import os, pathlib, subprocess
    from google.cloud import storage

    client = storage.Client()

    # Downloads LoRA weights uploaded by train.py
    downloaded = 0
    for blob in client.list_blobs(gcs_bucket, prefix="models/"):
        if blob.name.endswith("/"):
            continue
        rel   = blob.name.replace("models/", "", 1)
        local = pathlib.Path("/app/models") / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local))
        downloaded += 1
    print(f"[evaluate_op] Downloaded {downloaded} weight files from GCS")

    env = {
        **os.environ,
        "CONFIG_PATH":         "/app/config/training_config.yaml",
        "MLFLOW_TRACKING_URI": mlflow_tracking_uri,
    }
    res = subprocess.run(["python", "/app/scripts/evaluate.py"], env=env, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"evaluate.py failed (exit {res.returncode})")

    # Syncs outputs to GCS for downstream components
    bucket = client.bucket(gcs_bucket)
    uploaded = 0
    for path in pathlib.Path("/app/models").rglob("*"):
        if path.is_file():
            rel = path.relative_to("/app/models")
            bucket.blob(f"pipeline-results/models/{rel}").upload_from_filename(str(path))
            uploaded += 1
    print(f"[evaluate_op] Uploaded {uploaded} files → gs://{gcs_bucket}/pipeline-results/models/")
    return "done"


@component(base_image=TRAINER_IMAGE)
def bias_detection_op(mlflow_tracking_uri: str, gcs_bucket: str) -> str:
    import os, pathlib, subprocess
    from google.cloud import storage

    client = storage.Client()
    downloaded = 0
    for blob in client.list_blobs(gcs_bucket, prefix="pipeline-results/models/"):
        if blob.name.endswith("/"):
            continue
        rel   = blob.name.replace("pipeline-results/models/", "", 1)
        local = pathlib.Path("/app/models") / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local))
        downloaded += 1
    print(f"[bias_detection_op] Downloaded {downloaded} files")

    env = {
        **os.environ,
        "CONFIG_PATH":         "/app/config/training_config.yaml",
        "MLFLOW_TRACKING_URI": mlflow_tracking_uri,
    }
    res = subprocess.run(["python", "/app/scripts/bias_detection.py"], env=env, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"bias_detection.py failed (exit {res.returncode})")
    return "done"


@component(base_image=TRAINER_IMAGE)
def sensitivity_analysis_op(mlflow_tracking_uri: str, gcs_bucket: str) -> str:
    import os, pathlib, subprocess
    from google.cloud import storage

    client = storage.Client()
    downloaded = 0
    for blob in client.list_blobs(gcs_bucket, prefix="pipeline-results/models/"):
        if blob.name.endswith("/"):
            continue
        rel   = blob.name.replace("pipeline-results/models/", "", 1)
        local = pathlib.Path("/app/models") / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local))
        downloaded += 1
    print(f"[sensitivity_analysis_op] Downloaded {downloaded} files")

    env = {
        **os.environ,
        "CONFIG_PATH":         "/app/config/training_config.yaml",
        "MLFLOW_TRACKING_URI": mlflow_tracking_uri,
    }
    res = subprocess.run(["python", "/app/scripts/sensitivity_analysis.py"], env=env, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"sensitivity_analysis.py failed (exit {res.returncode})")
    return "done"


@component(base_image=TRAINER_IMAGE)
def rollback_and_gate_op(
    mlflow_tracking_uri: str,
    gcs_bucket: str,
    rollback_threshold: float = 0.02,
) -> str:
    """Returns 'GO', 'NO-GO', or 'ROLLBACK'."""
    import json, pathlib, yaml
    from google.cloud import storage
    import mlflow

    client = storage.Client()
    metrics_dir = pathlib.Path("/tmp/models")
    for blob in client.list_blobs(gcs_bucket, prefix="pipeline-results/models/"):
        if "test_metrics.json" not in blob.name:
            continue
        rel   = blob.name.replace("pipeline-results/models/", "", 1)
        local = metrics_dir / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local))

    with open("/app/config/training_config.yaml") as f:
        config = yaml.safe_load(f)

    thresholds = config["validation"]

    # --- Rollback check ---
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    rollback_needed = False

    for entity_type in config["data"]["entity_types"]:
        metrics_path = metrics_dir / entity_type / "results" / "test_metrics.json"
        if not metrics_path.exists():
            print(f"[combined] No metrics for {entity_type} — skipping rollback check")
            continue

        metrics    = json.loads(metrics_path.read_text())
        current_f1 = metrics.get("test_f1", 0)

        experiment = mlflow.get_experiment_by_name(config["mlflow"]["experiment_name"])
        if experiment:
            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=(
                    f"tags.entity_type = '{entity_type}' "
                    f"and tags.quality_gate = 'GO'"
                ),
                order_by=["metrics.test_f1 DESC"],
                max_results=1,
            )
            if not runs.empty:
                best_f1     = runs.iloc[0].get("metrics.test_f1", 0)
                degradation = best_f1 - current_f1
                print(f"[combined] {entity_type}: best={best_f1:.4f} current={current_f1:.4f} drop={degradation:.4f}")
                if degradation > rollback_threshold:
                    rollback_needed = True

    if rollback_needed:
        print("[combined] → ROLLBACK")
        return "ROLLBACK"

    # --- Quality gate ---
    for entity_type in config["data"]["entity_types"]:
        metrics_path = metrics_dir / entity_type / "results" / "test_metrics.json"
        if not metrics_path.exists():
            print(f"[combined] No metrics for {entity_type} — NO-GO")
            return "NO-GO"

        metrics = json.loads(metrics_path.read_text())
        checks  = {
            "f1":        (metrics.get("test_f1",        0.0), thresholds["min_f1"]),
            "precision": (metrics.get("test_precision", 0.0), thresholds["min_precision"]),
            "recall":    (metrics.get("test_recall",    0.0), thresholds["min_recall"]),
            "auc":       (metrics.get("test_auc",       0.0), thresholds["min_auc"]),
        }
        for name, (val, thr) in checks.items():
            print(f"  {name}: {val:.4f} >= {thr} → {'PASS' if val >= thr else 'FAIL'}")
            if val < thr:
                print("[combined] → NO-GO")
                return "NO-GO"

    print("[combined] → GO")
    return "GO"


@component(base_image=TRAINER_IMAGE)
def rollback_op(mlflow_tracking_uri: str) -> None:
    """Re-tags previous image version as :latest in Artifact Registry."""
    import json, shutil, subprocess, yaml
    from datetime import datetime
    from google.cloud import storage
    import mlflow

    with open("/app/config/training_config.yaml") as f:
        config = yaml.safe_load(f)

    gcp    = config["gcp"]
    ar     = gcp["artifact_registry"]
    client = storage.Client()
    bucket = client.bucket("entity-resolution-bucket-1")

    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    for entity_type in config["data"]["entity_types"]:
        image_name = ar["image_name"].format(entity_type=entity_type)
        image_base = (
            f"{ar['location']}-docker.pkg.dev/"
            f"{gcp['project_id']}/{ar['repository']}/{image_name}"
        )

        previous = None
        try:
            log_data = json.loads(
                bucket.blob(
                    f"pipeline-results/{entity_type}/registry_log.json"
                ).download_as_text()
            )
            previous = log_data.get("previous_version")
        except Exception as exc:
            print(f"[rollback_op] Could not read registry log: {exc}")

        if previous:
            if shutil.which("gcloud"):
                cmd = (
                    f"gcloud artifacts docker tags add "
                    f"{image_base}:{previous} {image_base}:latest "
                    f"--project={gcp['project_id']}"
                )
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                print(result.stdout or result.stderr)
            else:
                print(
                    f"[rollback_op] WARNING: gcloud not in PATH. "
                    f"Manual re-tag needed: {image_base}:{previous} → :latest"
                )
        else:
            print(f"[rollback_op] No previous version for {entity_type}")

        with mlflow.start_run(
            run_name=f"rollback_{entity_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        ):
            mlflow.set_tag("entity_type", entity_type)
            mlflow.set_tag("stage",       "rollback")
            mlflow.set_tag("rollback_to", previous or "none")
            mlflow.set_tag("reason",      "F1 degradation > threshold")

@component(base_image=TRAINER_IMAGE)
def push_to_registry_op(mlflow_tracking_uri: str, gcs_bucket: str) -> str:
    import json, os, pathlib, shutil, subprocess, yaml
    from google.cloud import storage

    client = storage.Client()

    # LoRA weights (uploaded by train.py)
    for blob in client.list_blobs(gcs_bucket, prefix="models/"):
        if blob.name.endswith("/"):
            continue
        rel   = blob.name.replace("models/", "", 1)
        local = pathlib.Path("/app/models") / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local))

    # Evaluate outputs (metrics JSON, predictions CSV, plots)
    for blob in client.list_blobs(gcs_bucket, prefix="pipeline-results/models/"):
        if blob.name.endswith("/"):
            continue
        rel   = blob.name.replace("pipeline-results/models/", "", 1)
        local = pathlib.Path("/app/models") / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local))

    with open("/app/config/training_config.yaml") as f:
        config = yaml.safe_load(f)

    gcp = config["gcp"]
    ar  = gcp["artifact_registry"]
    bucket_obj = client.bucket(gcs_bucket)
    image_uri  = ""

    for entity_type in config["data"]["entity_types"]:
        # Build context in GCS — Cloud Build needs source in GCS
        image_name = ar["image_name"].format(entity_type=entity_type)
        image_uri  = (
            f"{ar['location']}-docker.pkg.dev/"
            f"{gcp['project_id']}/{ar['repository']}/{image_name}"
        )

        tmp_dir = pathlib.Path(f"/tmp/er_push_{entity_type}")
        tmp_dir.mkdir(parents=True, exist_ok=True)

        # Copy weights, serve.py, config into build context
        shutil.copytree(
            f"/app/models/{entity_type}/final_model",
            tmp_dir / "model_weights",
            dirs_exist_ok=True,
        )
        shutil.copy("/app/scripts/serve.py", tmp_dir / "serve.py")
        shutil.copytree("/app/config", tmp_dir / "config", dirs_exist_ok=True)

        # Write Dockerfile
        packages = ar["serving_packages"] + ["fastapi", "uvicorn[standard]", "pydantic"]
        (tmp_dir / "Dockerfile").write_text(f"""FROM {ar["serving_base_image"]}
WORKDIR /app
RUN pip install --no-cache-dir {" ".join(packages)}
COPY model_weights/ ./model_weights/
COPY serve.py ./scripts/serve.py
COPY config/ ./config/
ENV MODEL_DIR=/app/model_weights
ENV CONFIG_PATH=/app/config/training_config.yaml
EXPOSE 8080
CMD ["uvicorn", "scripts.serve:app", "--host", "0.0.0.0", "--port", "8080"]
""")

        # Upload build context to GCS
        gcs_build_prefix = f"cloudbuild/{entity_type}"
        for f in tmp_dir.rglob("*"):
            if f.is_file():
                blob_name = f"{gcs_build_prefix}/{f.relative_to(tmp_dir)}"
                bucket_obj.blob(blob_name).upload_from_filename(str(f))
        print(f"[push_to_registry_op] Build context → gs://{gcs_bucket}/{gcs_build_prefix}/")

        # Get next version
        list_result = subprocess.run(
            ["gcloud", "artifacts", "docker", "images", "list", image_uri,
             "--include-tags", "--format=json", f"--project={gcp['project_id']}"],
            capture_output=True, text=True,
        )
        previous = None
        new_version = "v1"
        if list_result.returncode == 0 and list_result.stdout.strip():
            try:
                imgs = json.loads(list_result.stdout)
                tags = [int(t[1:]) for img in imgs
                        for t in img.get("tags","").split(",")
                        if t.strip().startswith("v") and t.strip()[1:].isdigit()]
                if tags:
                    previous    = f"v{max(tags)}"
                    new_version = f"v{max(tags)+1}"
            except Exception:
                pass

        # Submit Cloud Build — no Docker daemon needed
        build_result = subprocess.run([
            "gcloud", "builds", "submit",
            f"--tag={image_uri}:{new_version}",
            f"--gcs-source-staging-dir=gs://{gcs_bucket}/cloudbuild-staging",
            f"--project={gcp['project_id']}",
            f"gs://{gcs_bucket}/{gcs_build_prefix}/",
        ], capture_output=True, text=True)

        print(build_result.stdout)
        if build_result.returncode != 0:
            print(build_result.stderr)
            raise RuntimeError(f"Cloud Build failed for {entity_type}")

        # Also tag as :latest
        subprocess.run([
            "gcloud", "artifacts", "docker", "tags", "add",
            f"{image_uri}:{new_version}", f"{image_uri}:latest",
            f"--project={gcp['project_id']}",
        ], check=True)

        # Save registry log
        log = {
            "entity_type":      entity_type,
            "version":          new_version,
            "previous_version": previous,
            "image_uri":        f"{image_uri}:{new_version}",
        }
        log_path = pathlib.Path(f"/app/models/{entity_type}/results/registry_log.json")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(json.dumps(log, indent=2))
        bucket_obj.blob(
            f"pipeline-results/{entity_type}/registry_log.json"
        ).upload_from_filename(str(log_path))

        print(f"[push_to_registry_op] Pushed: {image_uri}:{new_version}")

    return f"{image_uri}:latest"

@component(base_image=TRAINER_IMAGE)
def register_model_op(image_uri: str, gcs_bucket: str) -> str:
    """
    Registers the model serving container in Vertex AI Model Registry.

    The serving container (er_{entity}_deberta image built by push_to_registry.py)
    must expose two HTTP routes:
        GET  /health   → 200 OK
        POST /predict  → {"predictions": [...]}

    Vertex AI Model Registry tracks versions and lineage — this is the bridge
    between the Artifact Registry Docker image and Vertex AI's deployment layer.

    Returns the Vertex AI Model resource name forwarded to deploy_to_endpoint_op.
    """
    import yaml
    from google.cloud import aiplatform

    with open("/app/config/training_config.yaml") as f:
        config = yaml.safe_load(f)

    gcp = config["gcp"]
    ar  = gcp["artifact_registry"]

    aiplatform.init(
        project=gcp["project_id"],
        location=gcp["region"],
    )

    model_resource_name = ""

    for entity_type in config["data"]["entity_types"]:
        display_name     = f"er-{entity_type}-deberta"
        entity_image_uri = (
            f"{ar['location']}-docker.pkg.dev/"
            f"{gcp['project_id']}/{ar['repository']}/"
            f"{ar['image_name'].format(entity_type=entity_type)}:latest"
        )

        print(f"[register_model_op] Registering {display_name} ← {entity_image_uri}")

        model = aiplatform.Model.upload(
            display_name=display_name,
            serving_container_image_uri=entity_image_uri,
            serving_container_predict_route="/predict",
            serving_container_health_route="/health",
            serving_container_ports=[8080],
            # Pass calibrated threshold so the serving container matches evaluate.py
            serving_container_environment_variables={
                "CONFIG_PATH":              "/app/config/training_config.yaml",
                "CLASSIFICATION_THRESHOLD": str(
                    config["validation"]["classification_threshold"]
                ),
            },
            labels={
                "entity_type": entity_type,
                "pipeline":    "er-model-pipeline",
                "framework":   "deberta-lora",
            },
        )

        print(
            f"[register_model_op] Registered: {model.resource_name} "
            f"(version {model.version_id})"
        )
        model_resource_name = model.resource_name

    return model_resource_name  # → deploy_to_endpoint_op


@component(base_image=TRAINER_IMAGE)
def deploy_to_endpoint_op(
    model_resource_name: str,
    gcs_bucket: str,
    machine_type: str = "n1-standard-4",
    min_replica_count: int = 1,
    max_replica_count: int = 3,
) -> str:
    """
    Deploys the registered model to a Vertex AI Endpoint.

    Endpoint management:
        Looks for an existing endpoint named 'er-model-endpoint' and reuses it,
        so repeated pipeline runs update the same endpoint rather than creating
        new ones. Creates it on first run.

    Traffic split:
        Deploys with traffic_split={"0": 100} — 100% to new version immediately.
        For canary releases, change to {"0": 10} and manage the previous
        deployed_model_id separately.

    Autoscaling:
        min_replica_count=1 keeps the endpoint warm (no cold starts for the
        monitoring pipeline and Streamlit frontend).
        max_replica_count=3 handles burst traffic.

    Saves endpoint_info.json to GCS so the monitoring pipeline and Streamlit
    frontend can discover the predict URL without hardcoding it.

    Returns the endpoint resource name.
    """
    import json, yaml
    from google.cloud import aiplatform, storage

    with open("/app/config/training_config.yaml") as f:
        config = yaml.safe_load(f)

    gcp = config["gcp"]
    aiplatform.init(
        project=gcp["project_id"],
        location=gcp["region"],
    )

    endpoint_display_name = "er-model-endpoint"

    # Reuse existing endpoint to avoid accumulating stale endpoints
    existing = aiplatform.Endpoint.list(
        filter=f'display_name="{endpoint_display_name}"',
        order_by="create_time desc",
    )

    if existing:
        endpoint = existing[0]
        print(f"[deploy_to_endpoint_op] Reusing endpoint: {endpoint.resource_name}")
    else:
        endpoint = aiplatform.Endpoint.create(
            display_name=endpoint_display_name,
            labels={
                "project":  "entity-resolution",
                "pipeline": "er-model-pipeline",
            },
        )
        print(f"[deploy_to_endpoint_op] Created endpoint: {endpoint.resource_name}")

    model = aiplatform.Model(model_name=model_resource_name)
    print(
        f"[deploy_to_endpoint_op] Deploying {model.display_name} "
        f"on {machine_type} (replicas {min_replica_count}–{max_replica_count})…"
    )

    model.deploy(
        endpoint=endpoint,
        deployed_model_display_name=model.display_name,
        machine_type=machine_type,
        min_replica_count=min_replica_count,
        max_replica_count=max_replica_count,
        traffic_split={"0": 100},
        # sync=True is safe here — we're inside a Vertex AI Pipeline component,
        # not inside Airflow. No heartbeat timeout risk.
        sync=True,
    )

    predict_url = (
        f"https://{gcp['region']}-aiplatform.googleapis.com/v1/"
        f"{endpoint.resource_name}:predict"
    )
    print(
        f"[deploy_to_endpoint_op] Deployed.\n"
        f"  Endpoint : {endpoint.resource_name}\n"
        f"  Predict  : {predict_url}"
    )

    # Persist endpoint info for the monitoring pipeline and Streamlit frontend
    endpoint_info = json.dumps({
        "endpoint_resource_name": endpoint.resource_name,
        "predict_url":            predict_url,
        "project_id":             gcp["project_id"],
        "region":                 gcp["region"],
    })
    storage.Client().bucket(gcs_bucket).blob(
        "pipeline-results/endpoint_info.json"
    ).upload_from_string(endpoint_info, content_type="application/json")
    print(
        f"[deploy_to_endpoint_op] Endpoint info → "
        f"gs://{gcs_bucket}/pipeline-results/endpoint_info.json"
    )

    return endpoint.resource_name


@component(base_image=TRAINER_IMAGE)
def alert_op(reason: str, mlflow_tracking_uri: str) -> None:
    import json, urllib.request, yaml

    with open("/app/config/training_config.yaml") as f:
        config = yaml.safe_load(f)

    msg = (
        f"[ER Pipeline] {reason}\n"
        f"MLflow:    {mlflow_tracking_uri}\n"
        f"Vertex AI: https://console.cloud.google.com/vertex-ai/pipelines"
        f"?project={config['gcp']['project_id']}"
    )
    print(msg)

    notif = config.get("notifications", {})
    if notif.get("enabled") and notif.get("slack_webhook_url"):
        payload = json.dumps({"text": msg}).encode()
        req = urllib.request.Request(
            notif["slack_webhook_url"],
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req)
        print("[alert_op] Slack notification sent")


# =============================================================================
# Pipeline
# =============================================================================

@dsl.pipeline(
    name="er-model-pipeline",
    description=(
        "Entity Resolution: train → evaluate → bias/sensitivity → "
        "rollback_check → quality_gate → push → register → deploy"
    ),
    pipeline_root=PIPELINE_ROOT,
)
def er_model_pipeline(
    mlflow_tracking_uri: str   = "",
    gcs_bucket:          str   = GCS_BUCKET,
    rollback_threshold:  float = 0.02,
    deploy_machine_type: str   = "n1-standard-4",
    min_replicas:        int   = 1,
    max_replicas:        int   = 3,
):
    """
    Parameters
    ----------
    mlflow_tracking_uri  External IP of airflow-vm (port 5000).
                         Update when the VM restarts until both pipelines
                         are consolidated on a single VM.
    gcs_bucket           GCS bucket for data + pipeline artifacts.
    rollback_threshold   Max tolerated absolute F1 drop vs best GO run.
    deploy_machine_type  Vertex AI endpoint machine type.
    min_replicas         Min endpoint replicas (1 = always warm).
    max_replicas         Max endpoint replicas for autoscaling.
    """

    # ── 1. Train (GPU) ────────────────────────────────────────────────────────
    train_task = (
        train_op(mlflow_tracking_uri=mlflow_tracking_uri)
        .set_display_name("train")
        .set_accelerator_type("NVIDIA_TESLA_T4")
        .set_accelerator_limit(1)
        .set_cpu_limit("8")
        .set_memory_limit("30G")
        .set_retry(num_retries=1, backoff_duration="60s")
    )

    # ── 2. Evaluate (CPU) ─────────────────────────────────────────────────────
    evaluate_task = (
        evaluate_op(
            mlflow_tracking_uri=mlflow_tracking_uri,
            gcs_bucket=gcs_bucket,
        )
        .set_display_name("evaluate")
        .set_cpu_limit("4")
        .set_memory_limit("16G")
        .after(train_task)
    )

    # ── 3a & 3b. Bias + Sensitivity (parallel, CPU) ───────────────────────────
    bias_task = (
        bias_detection_op(
            mlflow_tracking_uri=mlflow_tracking_uri,
            gcs_bucket=gcs_bucket,
        )
        .set_display_name("bias_detection")
        .set_cpu_limit("4")
        .set_memory_limit("16G")
        .after(evaluate_task)
    )

    sensitivity_task = (
        sensitivity_analysis_op(
            mlflow_tracking_uri=mlflow_tracking_uri,
            gcs_bucket=gcs_bucket,
        )
        .set_display_name("sensitivity_analysis")
        .set_cpu_limit("4")
        .set_memory_limit("16G")
        .after(evaluate_task)
    )

    # ── 4. Rollback + quality gate (waits for both parallel tasks) ────────────
    combined_task = (
        rollback_and_gate_op(
            mlflow_tracking_uri=mlflow_tracking_uri,
            gcs_bucket=gcs_bucket,
            rollback_threshold=rollback_threshold,
        )
        .set_display_name("rollback_and_quality_gate")
        .after(bias_task, sensitivity_task)
    )

    # ── 5. Flat branches — no nesting ─────────────────────────────────────────
    with dsl.If(combined_task.output == "ROLLBACK", name="if-rollback"):
        rollback_task = rollback_op(
            mlflow_tracking_uri=mlflow_tracking_uri,
        ).set_display_name("rollback")
        alert_op(
            reason="Rollback triggered — F1 degradation exceeded threshold.",
            mlflow_tracking_uri=mlflow_tracking_uri,
        ).set_display_name("alert_rollback").after(rollback_task)

    with dsl.If(combined_task.output == "NO-GO", name="if-no-go"):
        alert_op(
            reason="Quality gate NO-GO — one or more metrics below threshold.",
            mlflow_tracking_uri=mlflow_tracking_uri,
        ).set_display_name("alert_quality_gate")

    with dsl.If(combined_task.output == "GO", name="if-push"):
        push_task = push_to_registry_op(
            mlflow_tracking_uri=mlflow_tracking_uri,
            gcs_bucket=gcs_bucket,
        ).set_display_name("push_to_registry")

        register_task = register_model_op(
            image_uri=push_task.output,
            gcs_bucket=gcs_bucket,
        ).set_display_name("register_model")

        deploy_to_endpoint_op(
            model_resource_name=register_task.output,
            gcs_bucket=gcs_bucket,
            machine_type=deploy_machine_type,
            min_replica_count=min_replicas,
            max_replica_count=max_replicas,
        ).set_display_name("deploy_to_endpoint")


# =============================================================================
# Compile / upload / run helpers
# =============================================================================

def compile_pipeline(output_path: str = PIPELINE_YAML) -> None:
    compiler.Compiler().compile(
        pipeline_func=er_model_pipeline,
        package_path=output_path,
    )
    print(f"[compile] → {output_path}")


def upload_to_gcs(
    local_path: str = PIPELINE_YAML,
    gcs_path:   str = GCS_PIPELINE_YAML,
) -> None:
    from google.cloud import storage as gcs
    bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
    gcs.Client().bucket(bucket_name).blob(blob_name).upload_from_filename(local_path)
    print(f"[upload] → {gcs_path}")
    print(f"[upload] RunPipelineJobOperator template_path='{gcs_path}'")


def run_pipeline(pipeline_yaml: str = PIPELINE_YAML,) -> None:
    mlflow_uri = get_mlflow_uri(GCS_BUCKET)
    print(f"[run] MLflow URI for bucket {GCS_BUCKET}: {mlflow_uri}")

    aiplatform.init(
        project=PROJECT_ID,
        location=REGION,
        staging_bucket=STAGING_BUCKET,
    )

    job = aiplatform.PipelineJob(
        display_name=f"er-model-pipeline-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        template_path=pipeline_yaml,
        pipeline_root=PIPELINE_ROOT,
        parameter_values={
            "mlflow_tracking_uri": mlflow_uri,
            "gcs_bucket":          GCS_BUCKET,
            "rollback_threshold":  0.02,
            "deploy_machine_type": "n1-standard-4",
            "min_replicas":        1,
            "max_replicas":        3,
        },
        enable_caching=False,
    )

    job.run(sync=False)
    print(
        f"[run] Monitor: https://console.cloud.google.com/vertex-ai/pipelines"
        f"?project={PROJECT_ID}"
    )


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --compile
  python pipeline.py --compile --upload
  python pipeline.py --run
        """,
    )
    parser.add_argument("--compile", action="store_true", help="Compile to pipeline.yaml")
    parser.add_argument("--upload",  action="store_true", help="Upload pipeline.yaml to GCS")
    parser.add_argument("--run",     action="store_true", help="Compile + upload + submit to Vertex AI")
    parser.add_argument("--output", default=PIPELINE_YAML)
    args = parser.parse_args()

    if not any([args.compile, args.upload, args.run]):
        parser.print_help()
    else:
        if args.compile or args.run:
            compile_pipeline(args.output)
        if args.upload or args.run:
            upload_to_gcs(args.output)
        if args.run:
            run_pipeline(pipeline_yaml=args.output)