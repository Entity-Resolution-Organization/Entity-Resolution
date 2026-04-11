"""
Vertex AI Pipeline (KFP v2) — Entity Resolution Model Monitoring
================================================================

Runs daily via Cloud Scheduler. Checks model health and triggers
retraining if needed.

Pipeline flow:
    monitor_op (CPU)
      → check decision
          ├─[HEALTHY] → alert("All checks passed")
          └─[RETRAIN] → trigger_retrain → alert("Retraining triggered")

Usage:
    python monitoring_pipeline.py --compile
    python monitoring_pipeline.py --run
"""

import argparse
import os
from datetime import datetime

from google.cloud import aiplatform
from kfp import compiler, dsl
from kfp.dsl import component
from utils import get_mlflow_uri

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "entity-resolution-487121")
REGION = os.environ.get("GCP_REGION", "us-central1")
GCS_BUCKET = "entity-resolution-bucket-1"
STAGING_BUCKET = "gs://entity-resolution-staging-bucket"
PIPELINE_ROOT = f"gs://{GCS_BUCKET}/pipeline-root"
MONITORING_YAML = "monitoring_pipeline.yaml"
GCS_MONITORING_YAML = f"gs://{GCS_BUCKET}/pipelines/monitoring_pipeline.yaml"

TRAINER_IMAGE = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/ml-models/er-trainer:latest"


@component(base_image=TRAINER_IMAGE)
def monitor_op(
    gcs_bucket: str,
    project_id: str,
    region: str,
) -> str:
    """
    Runs full monitoring: evaluate holdout, detect drift, check thresholds,
    log to BigQuery. Returns 'HEALTHY' or 'RETRAIN'.
    """
    import json
    import os
    import subprocess
    import sys

    # Install monitoring dependencies
    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "evidently>=0.5.0", "scipy", "requests",
        "--quiet", "--no-warn-script-location"
    ])

    os.environ["CONFIG_PATH"] = "/app/config/training_config.yaml"

    from scripts.monitor import ModelMonitor, load_config

    config = load_config(os.environ["CONFIG_PATH"])

    results = {}
    for entity_type in config["data"]["entity_types"]:
        monitor = ModelMonitor(config=config, entity_type=entity_type)
        result = monitor.run()
        results[entity_type] = result


    # Save detailed summary to GCS for alert_op
    import json as _json
    from google.cloud import storage as _storage
    from datetime import datetime as _dt

    summary = {"timestamp": _dt.utcnow().isoformat(), "entity_results": {}}
    overall_decision = "HEALTHY"

    for et, r in results.items():
        et_decision = r.get("thresholds", {}).get("decision", "ERROR")
        perf = r.get("performance", {})
        drift = r.get("drift", {})
        checks = r.get("thresholds", {}).get("checks", {})

        summary["entity_results"][et] = {
            "decision": et_decision,
            "f1": perf.get("test_f1"),
            "precision": perf.get("test_precision"),
            "recall": perf.get("test_recall"),
            "auc": perf.get("test_auc"),
            "drift_ratio": drift.get("drift_ratio"),
            "drifted_features": drift.get("drifted_features", 0),
            "checks": checks,
        }

        if et_decision == "RETRAIN_DATA":
            overall_decision = "RETRAIN_DATA"
        elif et_decision == "RETRAIN_MODEL" and overall_decision != "RETRAIN_DATA":
            overall_decision = "RETRAIN_MODEL"

    summary["overall_decision"] = overall_decision

    try:
        client = _storage.Client()
        bucket = client.bucket(gcs_bucket)
        bucket.blob("monitoring/latest_summary.json").upload_from_string(
            _json.dumps(summary, indent=2), content_type="application/json"
        )
        print(f"[monitor_op] Summary saved to gs://{gcs_bucket}/monitoring/latest_summary.json")
    except Exception as e:
        print(f"[monitor_op] Failed to save summary: {e}")

    return overall_decision


@component(base_image=TRAINER_IMAGE)
def trigger_retrain_op(
    gcs_bucket: str,
    project_id: str,
    region: str,
    mlflow_tracking_uri: str,
) -> str:
    """Triggers the model training pipeline."""
    import os

    from google.cloud import aiplatform, storage

    aiplatform.init(
        project=project_id,
        location=region,
        staging_bucket=f"gs://{gcs_bucket.replace('gs://', '')}",
    )

    # Read pipeline YAML from GCS
    pipeline_yaml_gcs = f"gs://{gcs_bucket}/pipelines/pipeline.yaml"
    local_yaml = "/tmp/pipeline.yaml"

    client = storage.Client()
    client.bucket(gcs_bucket).blob("pipelines/pipeline.yaml").download_to_filename(
        local_yaml
    )

    job = aiplatform.PipelineJob(
        display_name=f"er-retrain-triggered-by-monitoring-{os.environ.get('PIPELINE_RUN_ID', 'manual')}",
        template_path=local_yaml,
        pipeline_root=f"gs://{gcs_bucket}/pipeline-root",
        parameter_values={
            "mlflow_tracking_uri": mlflow_tracking_uri,
            "gcs_bucket": gcs_bucket,
            "rollback_threshold": 0.02,
            "deploy_machine_type": "n1-standard-4",
            "min_replicas": 1,
            "max_replicas": 1,
        },
        enable_caching=False,
    )

    job.run(sync=False)
    print(f"[trigger_retrain_op] Retraining pipeline submitted")
    return "submitted"


@component(base_image=TRAINER_IMAGE)

@component(base_image=TRAINER_IMAGE)
def trigger_data_pipeline_op(
    airflow_vm_ip: str,
    gcs_bucket: str,
) -> str:
    """Triggers Airflow data pipeline via REST API, waits, then triggers ML pipeline."""
    import json
    import time
    import urllib.request

    # Trigger the data pipeline DAG
    url = f"http://{airflow_vm_ip}:8080/api/v1/dags/er_data_pipeline/dagRuns"
    payload = json.dumps({"conf": {"triggered_by": "monitoring_pipeline"}}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic " + __import__("base64").b64encode(b"airflow:airflow").decode(),
        },
    )

    try:
        resp = urllib.request.urlopen(req)
        result = json.loads(resp.read().decode())
        dag_run_id = result.get("dag_run_id", "unknown")
        print(f"[trigger_data_pipeline_op] Data pipeline triggered: {dag_run_id}")
    except Exception as e:
        print(f"[trigger_data_pipeline_op] Failed to trigger data pipeline: {e}")
        return "failed"

    # Poll for completion (max 2 hours)
    status_url = f"http://{airflow_vm_ip}:8080/api/v1/dags/er_data_pipeline/dagRuns/{dag_run_id}"
    for i in range(240):  # 240 * 30s = 2 hours
        time.sleep(30)
        try:
            status_req = urllib.request.Request(
                status_url,
                headers={
                    "Authorization": "Basic " + __import__("base64").b64encode(b"airflow:airflow").decode(),
                },
            )
            resp = urllib.request.urlopen(status_req)
            state = json.loads(resp.read().decode()).get("state", "unknown")
            print(f"[trigger_data_pipeline_op] Poll {i+1}: {state}")
            if state == "success":
                return "success"
            elif state == "failed":
                return "failed"
        except Exception as e:
            print(f"[trigger_data_pipeline_op] Poll error: {e}")

    return "timeout"

@component(base_image=TRAINER_IMAGE)
def alert_op(reason: str, decision: str, gcs_bucket: str) -> None:
    """Send rich Slack notification with metric details."""
    import json
    import urllib.request

    import yaml
    from google.cloud import storage

    with open("/app/config/training_config.yaml") as f:
        config = yaml.safe_load(f)

    # Read detailed summary from GCS
    summary = None
    try:
        client = storage.Client()
        blob = client.bucket(gcs_bucket).blob("monitoring/latest_summary.json")
        summary = json.loads(blob.download_as_text())
    except Exception:
        pass

    # Build rich message based on decision
    if decision == "RETRAIN_DATA" and summary:
        lines = ["⚠️ *[ER Monitoring] Data Drift Detected — Retraining Triggered*\n"]
        for et, d in summary.get("entity_results", {}).items():
            lines.append(f"*Entity: {et}*")
            if d.get("f1") is not None:
                lines.append(f"  F1: {d['f1']:.4f}")
            if d.get("precision") is not None:
                lines.append(f"  Precision: {d['precision']:.4f}")
            if d.get("recall") is not None:
                lines.append(f"  Recall: {d['recall']:.4f}")
            if d.get("drift_ratio") is not None:
                lines.append(f"  Drift ratio: {d['drift_ratio']:.2%}")
            lines.append(f"  Drifted features: {d.get('drifted_features', 0)}")
        lines.append("\nAction: Data pipeline → ML pipeline triggered")

    elif decision == "RETRAIN_MODEL" and summary:
        lines = ["⚠️ *[ER Monitoring] Model Drift — Retraining Triggered*\n"]
        for et, d in summary.get("entity_results", {}).items():
            lines.append(f"*Entity: {et}*")
            if d.get("f1") is not None:
                lines.append(f"  F1: {d['f1']:.4f}")
            if d.get("precision") is not None:
                lines.append(f"  Precision: {d['precision']:.4f}")
            if d.get("recall") is not None:
                lines.append(f"  Recall: {d['recall']:.4f}")
        lines.append("\nAction: ML pipeline triggered (data looks fine)")

    elif decision == "HEALTHY" and summary:
        lines = ["✅ *[ER Monitoring] All Checks Passed*\n"]
        for et, d in summary.get("entity_results", {}).items():
            f1 = d.get("f1")
            lines.append(f"  {et}: F1={f1:.4f}" if f1 else f"  {et}: OK")

    else:
        lines = [f"[ER Monitoring] Decision: {decision}", f"Reason: {reason}"]

    lines.append(f"\nTimestamp: {summary.get('timestamp', 'N/A')}" if summary else "")
    lines.append(
        f"Vertex AI: https://console.cloud.google.com/vertex-ai/pipelines"
        f"?project={config['gcp']['project_id']}"
    )

    msg = "\n".join(lines)
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
        print("[alert_op] Slack notification sent")


@dsl.pipeline(
    name="er-monitoring-pipeline",
    description="Daily model monitoring: evaluate, detect drift, trigger retrain if needed",
    pipeline_root=PIPELINE_ROOT,
)
def er_monitoring_pipeline(
    gcs_bucket: str = GCS_BUCKET,
    project_id: str = PROJECT_ID,
    region: str = REGION,
    mlflow_tracking_uri: str = "",
):
    # Step 1: Run monitoring
    monitor_task = (
        monitor_op(
            gcs_bucket=gcs_bucket,
            project_id=project_id,
            region=region,
        )
        .set_display_name("monitor_model")
        .set_cpu_limit("4")
        .set_memory_limit("16G")
    )
    # Step 2: If DATA DRIFT, trigger data pipeline first, then ML pipeline
    with dsl.If(monitor_task.output == "RETRAIN_DATA", name="if-retrain-data"):
        data_task = trigger_data_pipeline_op(
            airflow_vm_ip="34.31.109.57",
            gcs_bucket=gcs_bucket,
        ).set_display_name("trigger_data_pipeline")

        trigger_task = trigger_retrain_op(
            gcs_bucket=gcs_bucket,
            project_id=project_id,
            region=region,
            mlflow_tracking_uri=mlflow_tracking_uri,
        ).set_display_name("trigger_retraining_after_data").after(data_task)

        alert_op(
            reason="Data drift detected. Data pipeline + retraining triggered.",
            decision="RETRAIN_DATA",
            gcs_bucket=gcs_bucket,
        ).set_display_name("alert_retrain_data").after(trigger_task)

    # Step 3: If MODEL DRIFT, trigger ML pipeline only
    with dsl.If(monitor_task.output == "RETRAIN_MODEL", name="if-retrain-model"):
        trigger_task_model = trigger_retrain_op(
            gcs_bucket=gcs_bucket,
            project_id=project_id,
            region=region,
            mlflow_tracking_uri=mlflow_tracking_uri,
        ).set_display_name("trigger_retraining_model_only")

        alert_op(
            reason="Model performance degraded. Retraining triggered.",
            decision="RETRAIN_MODEL",
            gcs_bucket=gcs_bucket,
        ).set_display_name("alert_retrain_model").after(trigger_task_model)

    # Step 4: If HEALTHY, just notify
    with dsl.If(monitor_task.output == "HEALTHY", name="if-healthy"):
        alert_op(
            reason="All metrics within thresholds. No action needed.",
            decision="HEALTHY",
            gcs_bucket=gcs_bucket,
        ).set_display_name("alert_healthy")


def compile_pipeline(output_path: str = MONITORING_YAML) -> None:
    compiler.Compiler().compile(
        pipeline_func=er_monitoring_pipeline,
        package_path=output_path,
    )
    print(f"[compile] -> {output_path}")


def upload_to_gcs(
    local_path: str = MONITORING_YAML,
    gcs_path: str = GCS_MONITORING_YAML,
) -> None:
    from google.cloud import storage as gcs

    bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
    gcs.Client().bucket(bucket_name).blob(blob_name).upload_from_filename(local_path)
    print(f"[upload] -> {gcs_path}")


def run_pipeline(pipeline_yaml: str = MONITORING_YAML) -> None:
    mlflow_uri = get_mlflow_uri(GCS_BUCKET)
    print(f"[run] MLflow URI: {mlflow_uri}")

    aiplatform.init(
        project=PROJECT_ID,
        location=REGION,
        staging_bucket=STAGING_BUCKET,
    )

    job = aiplatform.PipelineJob(
        display_name=f"er-monitoring-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        template_path=pipeline_yaml,
        pipeline_root=PIPELINE_ROOT,
        parameter_values={
            "gcs_bucket": GCS_BUCKET,
            "project_id": PROJECT_ID,
            "region": REGION,
            "mlflow_tracking_uri": mlflow_uri,
        },
        enable_caching=False,
    )

    job.run(sync=False)
    print(
        f"[run] Monitor: https://console.cloud.google.com/vertex-ai/pipelines"
        f"?project={PROJECT_ID}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--compile", action="store_true")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--output", default=MONITORING_YAML)
    args = parser.parse_args()

    if not any([args.compile, args.upload, args.run]):
        parser.print_help()
    else:
        if args.compile or args.run:
            compile_pipeline(args.output)
        if args.upload or args.run:
            upload_to_gcs(args.output)
        if args.run:
            run_pipeline(args.output)
