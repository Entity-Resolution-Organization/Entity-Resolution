"""
Entity Resolution — Pipeline Runner

Orchestrates build_graph -> write_clusters -> score_network as a single
in-process pipeline. Designed to be called from the FastAPI backend as a
BackgroundTask, or from CLI for testing.

Tracks job status in a shared dict so the API can expose progress.

Usage (CLI):
    CONFIG_PATH=../Model-Pipeline/config/training_config.yaml \
    MODEL_RESOURCE_NAME=projects/.../models/... \
    python scripts/pipeline_runner.py --gcs-path gs://bucket/to-process/demo.csv

Usage (from app.py):
    from scripts.pipeline_runner import run_unify_pipeline, job_store
    background_tasks.add_task(run_unify_pipeline, job_id, gcs_path, config)
"""

import logging
import os
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("pipeline_runner")

# In-memory job store — shared with app.py via import
# Each entry: {status, stage, error, unified_gcs_path, stats, created_at, job_suffix}
job_store: dict[str, dict] = {}


def _new_job(job_id: str, gcs_path: str, job_suffix: str) -> dict:
    return {
        "job_id": job_id,
        "status": "queued",
        "stage": "",
        "error": None,
        "gcs_path": gcs_path,
        "job_suffix": job_suffix,
        "unified_gcs_path": None,
        "stats": {},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
    }


def run_unify_pipeline(
    job_id: str,
    records_gcs_path: str,
    config: dict,
    model_resource_name: str,
    entity_type: Optional[str] = None,
    run_scoring: bool = False,
    flagged_cluster_ids: Optional[list[str]] = None,
):
    """
    Run the full unify pipeline: build_graph -> write_clusters -> (optional) score_network.

    Updates job_store[job_id] at each stage for status polling.

    Args:
        job_id: unique job identifier
        records_gcs_path: GCS path to input records CSV
        config: loaded training_config.yaml dict
        model_resource_name: Vertex AI model resource name
        entity_type: defaults to config.data.entity_types[0]
        run_scoring: whether to run score_network after clustering
        flagged_cluster_ids: cluster IDs to flag for bad_neighbour scoring
    """
    # Import pipeline scripts here to avoid circular imports at module level
    from scripts.build_graph import (
        build_graph,
        derive_job_suffix,
        get_field_rules,
    )
    from scripts.write_clusters import (
        load_graph_data,
        build_clusters,
        compute_cluster_metrics,
        write_to_gcs,
        write_to_bigquery,
        build_unified_csv,
    )

    entity_type = entity_type or config["data"]["entity_types"][0]
    job_suffix = derive_job_suffix(records_gcs_path)
    bucket = config["data"]["gcs_bucket"]
    threshold = config["graph"]["cluster_edge_threshold"]

    job = _new_job(job_id, records_gcs_path, job_suffix)
    job_store[job_id] = job

    try:
        # ---- Stage 1: Build Graph ----
        job["status"] = "running"
        job["stage"] = "building_graph"
        log.info(f"[Job {job_id}] Stage 1: build_graph  suffix={job_suffix}")

        output_prefix = f"gs://{bucket}/graph"
        edges_path, nodes_path = build_graph(
            records_path=records_gcs_path,
            output_prefix=output_prefix,
            model_resource_name=model_resource_name,
            config=config,
            entity_type=entity_type,
            job_suffix=job_suffix,
        )

        if not edges_path:
            job["status"] = "failed"
            job["error"] = "No candidate pairs or edges above threshold"
            return

        # ---- Stage 2: Write Clusters ----
        job["stage"] = "clustering"
        log.info(f"[Job {job_id}] Stage 2: write_clusters")

        edges_df, nodes_df = load_graph_data(bucket, entity_type, job_suffix)

        entity_clusters_df, cluster_edges_df = build_clusters(
            edges_df=edges_df,
            nodes_df=nodes_df,
            threshold=threshold,
            entity_type=entity_type,
            job_suffix=job_suffix,
        )

        metrics = compute_cluster_metrics(entity_clusters_df, cluster_edges_df)
        job["stats"] = metrics

        # Write to GCS
        write_to_gcs(
            entity_clusters_df, cluster_edges_df,
            bucket, entity_type, job_suffix,
        )

        # Write to BigQuery (best-effort for demo)
        try:
            write_to_bigquery(entity_clusters_df, cluster_edges_df, config)
        except Exception as e:
            log.warning(f"[Job {job_id}] BigQuery write failed (non-fatal): {e}")

        # Build unified CSV
        unified_path = build_unified_csv(
            nodes_df, entity_clusters_df, job_suffix, bucket
        )
        job["unified_gcs_path"] = unified_path

        # ---- Stage 3: Score Network (optional) ----
        if run_scoring:
            job["stage"] = "scoring"
            log.info(f"[Job {job_id}] Stage 3: score_network")

            from scripts.score_network import (
                score_cluster_risk,
                score_bad_neighbour,
                score_shared_fields,
                score_network_centrality,
                build_composite_score,
                write_scores_to_bigquery,
            )

            risk_df = score_cluster_risk(entity_clusters_df)
            bad_neighbour_df = score_bad_neighbour(
                entity_clusters_df, cluster_edges_df,
                flagged_cluster_ids or [],
            )
            shared_field_df = score_shared_fields(
                entity_clusters_df, cluster_edges_df,
            )
            centrality_df = score_network_centrality(
                entity_clusters_df, cluster_edges_df,
            )

            cfg_scoring = config.get("network_scoring", {})
            scores_df = build_composite_score(
                entity_clusters_df, risk_df, bad_neighbour_df,
                shared_field_df, centrality_df, cfg_scoring,
            )

            try:
                write_scores_to_bigquery(scores_df, config, job_suffix)
            except Exception as e:
                log.warning(f"[Job {job_id}] Network scores BQ write failed: {e}")

            job["stats"]["mean_context_score"] = round(
                float(scores_df["composite_context_score"].mean()), 4
            )

        # ---- Done ----
        job["status"] = "complete"
        job["stage"] = "done"
        job["completed_at"] = datetime.now(timezone.utc).isoformat()
        log.info(f"[Job {job_id}] Complete  clusters={metrics.get('n_clusters')}")

    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        log.error(f"[Job {job_id}] Failed: {e}\n{traceback.format_exc()}")


def create_job_id() -> str:
    """Generate a short, filesystem-safe job ID."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"{ts}-{short}"


# =============================================================================
# CLI entrypoint for testing
# =============================================================================

def main():
    import argparse
    import yaml

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Run unify pipeline")
    parser.add_argument("--gcs-path", required=True, help="GCS path to records CSV")
    parser.add_argument("--config", default=os.environ.get("CONFIG_PATH", "config/training_config.yaml"))
    parser.add_argument("--model", default=os.environ.get("MODEL_RESOURCE_NAME", ""))
    parser.add_argument("--scoring", action="store_true", help="Run score_network")
    parser.add_argument("--flagged", type=str, default="", help="Comma-separated flagged cluster IDs")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    if not args.model:
        from scripts.build_graph import resolve_model_resource_name
        model = resolve_model_resource_name(config)
    else:
        model = args.model

    job_id = create_job_id()
    flagged = [c.strip() for c in args.flagged.split(",") if c.strip()]

    run_unify_pipeline(
        job_id=job_id,
        records_gcs_path=args.gcs_path,
        config=config,
        model_resource_name=model,
        run_scoring=args.scoring,
        flagged_cluster_ids=flagged,
    )

    job = job_store[job_id]
    print(f"\nJob {job_id}: {job['status']}")
    if job["error"]:
        print(f"Error: {job['error']}")
    if job["unified_gcs_path"]:
        print(f"Unified CSV: {job['unified_gcs_path']}")
    if job["stats"]:
        for k, v in job["stats"].items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
