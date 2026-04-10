"""
Entity Resolution — Cluster Writer

Reads enriched_edges.csv produced by build_graph.py, applies veto rules,
runs connected components to form entity clusters, writes results to GCS
and BigQuery, produces unified output CSV, and deletes source file.

Pipeline position:
    build_graph.py → write_clusters.py → score_network.py

Input:
    gs://{bucket}/graph/{entity_type}/{job_suffix}/enriched_edges.csv
    gs://{bucket}/graph/{entity_type}/{job_suffix}/node_features.csv (for record IDs)

Output:
    GCS:
      graph/clusters/{entity_type}/{job_suffix}/entity_clusters.csv
      graph/clusters/{entity_type}/{job_suffix}/cluster_edges.csv
    BigQuery:
      {dataset}.entity_clusters   (partitioned by date_partition)
      {dataset}.cluster_edges     (partitioned by date_partition)
    Unified CSV (upload flow only):
      processed/{job_suffix}_unified.csv

Cluster IDs:
    SHA256 of sorted record IDs — stable for same membership.
    Changes when membership changes (correct behaviour).

Threshold:
    Uses deberta_score directly (GNN shelved).
    Veto edges (vetoed=True) excluded before connected components.

Env vars:
    CONFIG_PATH
    JOB_SUFFIX        — must match build_graph job_suffix
    ENTITY_TYPE
    SOURCE_GCS_PATH   — original uploaded file to delete on success (optional)
                        only set for upload flow, not training runs
    MLFLOW_TRACKING_URI
"""

import hashlib
import logging
import os
from datetime import datetime
from io import StringIO

import mlflow
import networkx as nx
import numpy as np
import pandas as pd
import yaml
from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("write_clusters")

DEFAULT_CONFIG_PATH = os.environ.get("CONFIG_PATH", "config/training_config.yaml")
JOB_SUFFIX          = os.environ.get("JOB_SUFFIX", "train")
SOURCE_GCS_PATH     = os.environ.get("SOURCE_GCS_PATH", "")  # original upload — deleted on success


# =============================================================================
# Config
# =============================================================================

def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


# =============================================================================
# GCS I/O
# =============================================================================

def _gcs_download_csv(bucket_name: str, blob_name: str) -> pd.DataFrame:
    content = storage.Client().bucket(bucket_name).blob(blob_name).download_as_text()
    return pd.read_csv(StringIO(content))


def _gcs_upload_csv(df: pd.DataFrame, bucket_name: str, blob_name: str):
    storage.Client().bucket(bucket_name).blob(blob_name).upload_from_string(
        df.to_csv(index=False), content_type="text/csv"
    )
    log.info(f"[GCS] Uploaded {len(df)} rows → gs://{bucket_name}/{blob_name}")


def _gcs_delete(gcs_path: str):
    bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
    storage.Client().bucket(bucket_name).blob(blob_name).delete()
    log.info(f"[GCS] Deleted: {gcs_path}")


def load_graph_data(
    bucket: str,
    entity_type: str,
    job_suffix: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    graph_prefix = f"graph/{entity_type}/{job_suffix}"
    log.info(f"[Data] Loading from gs://{bucket}/{graph_prefix}/")

    edges_df = _gcs_download_csv(bucket, f"{graph_prefix}/enriched_edges.csv")
    nodes_df = _gcs_download_csv(bucket, f"{graph_prefix}/node_features.csv")

    log.info(f"[Data] {len(edges_df)} edges, {len(nodes_df)} nodes")
    return edges_df, nodes_df


# =============================================================================
# Cluster ID
# =============================================================================

def make_cluster_id(record_ids: list[str]) -> str:
    """
    Deterministic cluster ID — SHA256 of sorted, comma-joined record IDs.
    Stable for same membership. Changes correctly when membership changes.
    16-char hex is unique at realistic cluster counts.
    """
    key = ",".join(sorted(str(r) for r in record_ids))
    return hashlib.sha256(key.encode()).hexdigest()[:16]


# =============================================================================
# Connected components — clusters
# =============================================================================

def build_clusters(
    edges_df: pd.DataFrame,
    nodes_df: pd.DataFrame,
    threshold: float,
    entity_type: str,
    job_suffix: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    1. Exclude vetoed edges (hard rules fired in build_graph)
    2. Apply deberta_score threshold → binary edges
    3. Run connected components
    4. Build entity_clusters + cluster_edges tables

    Returns:
        entity_clusters_df — one row per record
        cluster_edges_df   — one row per binary edge within a cluster
    """
    date = datetime.utcnow().strftime("%Y-%m-%d")

    # —— 1. Veto exclusion ————————————————————————————————————————————————————
    if "vetoed" in edges_df.columns:
        n_vetoed = int(edges_df["vetoed"].astype(bool).sum())
        if n_vetoed:
            log.info(f"[Clusters] Excluding {n_vetoed} vetoed edges")
        edges_df = edges_df[~edges_df["vetoed"].astype(bool)].copy()

    # —— 2. Binary threshold ——————————————————————————————————————————————————
    binary = edges_df[edges_df["deberta_score"] >= threshold].copy()
    log.info(
        f"[Clusters] {len(binary)}/{len(edges_df)} edges above "
        f"threshold {threshold}"
    )

    # —— 3. Build graph ———————————————————————————————————————————————————————
    G = nx.Graph()

    # All records as nodes — singletons included
    all_records = set(
        nodes_df["record_id"].astype(str).tolist()
    ) | set(
        edges_df["id1"].astype(str)
    ) | set(
        edges_df["id2"].astype(str)
    )
    G.add_nodes_from(all_records)

    for _, row in binary.iterrows():
        G.add_edge(str(row["id1"]), str(row["id2"]))

    components = list(nx.connected_components(G))
    n_singleton = sum(1 for c in components if len(c) == 1)
    log.info(
        f"[Clusters] {len(components)} clusters "
        f"({n_singleton} singletons, "
        f"{len(components) - n_singleton} multi-record)"
    )

    # —— 4. Build score lookup from edges —————————————————————————————————————
    score_lookup: dict[tuple, dict] = {}
    for _, row in edges_df.iterrows():
        r1, r2 = str(row["id1"]), str(row["id2"])
        val = {"deberta_score": float(row.get("deberta_score", 0.0))}
        # Carry rule-based scores for audit trail
        for col in edges_df.columns:
            if col.endswith("_score") and col != "deberta_score":
                val[col] = float(row.get(col, 0.0))
        score_lookup[(r1, r2)] = val
        score_lookup[(r2, r1)] = val

    # —— 5. entity_clusters table —————————————————————————————————————————————
    cluster_rows = []
    for component in components:
        members      = list(component)
        cluster_id   = make_cluster_id(members)
        cluster_size = len(members)
        is_singleton = cluster_size == 1

        intra_scores = [
            score_lookup[(r1, r2)]["deberta_score"]
            for i, r1 in enumerate(members)
            for r2 in members[i + 1:]
            if (r1, r2) in score_lookup
        ]

        avg_score = float(np.mean(intra_scores)) if intra_scores else 1.0
        max_score = float(np.max(intra_scores))  if intra_scores else 1.0
        min_score = float(np.min(intra_scores))  if intra_scores else 1.0

        # Consistency check — flag clusters with internal field conflicts
        # (same pair-wise match but conflicting definitive fields across cluster)
        requires_review = (
            cluster_size > 10 and avg_score < 0.6
        )

        for record_id in members:
            cluster_rows.append({
                "record_id":       record_id,
                "cluster_id":      cluster_id,
                "cluster_size":    cluster_size,
                "is_singleton":    is_singleton,
                "avg_edge_score":  avg_score,
                "max_edge_score":  max_score,
                "min_edge_score":  min_score,
                "requires_review": requires_review,
                "entity_type":     entity_type,
                "job_suffix":      job_suffix,
                "date_partition":  date,
            })

    entity_clusters_df = pd.DataFrame(cluster_rows)

    # Build record → cluster lookup
    record_to_cluster = {
        row["record_id"]: row["cluster_id"]
        for _, row in entity_clusters_df.iterrows()
    }

    # —— 6. cluster_edges table ———————————————————————————————————————————————
    edge_rows = []
    for _, row in binary.iterrows():
        r1, r2 = str(row["id1"]), str(row["id2"])
        cid = record_to_cluster.get(r1)
        if cid != record_to_cluster.get(r2):
            continue  # shouldn't happen — guard

        edge_row = {
            "record_id_1":    r1,
            "record_id_2":    r2,
            "deberta_score":  float(row.get("deberta_score", 0.0)),
            "cluster_id":     cid,
            "entity_type":    entity_type,
            "job_suffix":     job_suffix,
            "date_partition": date,
            "vetoed":         False,
            "veto_reason":    "",
        }
        # Rule-based scores — audit trail
        for col in binary.columns:
            if col.endswith("_score") and col != "deberta_score":
                edge_row[col] = float(row.get(col, 0.0))
            if col.startswith("has_"):
                edge_row[col] = int(row.get(col, 0))

        edge_rows.append(edge_row)

    # Also record vetoed edges for audit (excluded from clustering but stored)
    if "vetoed" in edges_df.columns:
        vetoed_df = edges_df[edges_df["vetoed"].astype(bool)]
        for _, row in vetoed_df.iterrows():
            r1, r2 = str(row["id1"]), str(row["id2"])
            edge_rows.append({
                "record_id_1":    r1,
                "record_id_2":    r2,
                "deberta_score":  float(row.get("deberta_score", 0.0)),
                "cluster_id":     None,
                "entity_type":    entity_type,
                "job_suffix":     job_suffix,
                "date_partition": date,
                "vetoed":         True,
                "veto_reason":    str(row.get("veto_reason", "")),
            })

    cluster_edges_df = pd.DataFrame(edge_rows)

    log.info(
        f"[Clusters] entity_clusters: {len(entity_clusters_df)} rows, "
        f"cluster_edges: {len(cluster_edges_df)} rows"
    )
    return entity_clusters_df, cluster_edges_df


# =============================================================================
# Unified output CSV — joins original records with cluster assignments
# =============================================================================

def build_unified_csv(
    nodes_df: pd.DataFrame,
    entity_clusters_df: pd.DataFrame,
    job_suffix: str,
    bucket: str,
) -> str:
    """
    Join cluster assignments back onto original records.
    Output: processed/{job_suffix}_unified.csv

    Columns: all original record columns + cluster_id + cluster_size +
             avg_edge_score + requires_review
    """
    cluster_cols = entity_clusters_df[[
        "record_id", "cluster_id", "cluster_size",
        "avg_edge_score", "requires_review",
    ]].copy()

    # node_features.csv has record_id + emb_* columns
    # We only want record_id for the join — drop embeddings for output
    emb_cols = [c for c in nodes_df.columns if c.startswith("emb_")]
    records   = nodes_df.drop(columns=emb_cols)

    unified = records.merge(
        cluster_cols,
        on="record_id",
        how="left",
    )

    blob_name = f"processed/{job_suffix}_unified.csv"
    _gcs_upload_csv(unified, bucket, blob_name)
    log.info(
        f"[Unified] {len(unified)} records → "
        f"gs://{bucket}/{blob_name}"
    )
    return f"gs://{bucket}/{blob_name}"


# =============================================================================
# Cluster quality metrics
# =============================================================================

def compute_cluster_metrics(
    entity_clusters_df: pd.DataFrame,
    cluster_edges_df: pd.DataFrame,
) -> dict:
    sizes      = entity_clusters_df.groupby("cluster_id")["record_id"].count()
    singletons = int(entity_clusters_df["is_singleton"].sum())

    metrics = {
        "n_clusters":          int(entity_clusters_df["cluster_id"].nunique()),
        "n_records":           int(len(entity_clusters_df)),
        "n_singleton_records": singletons,
        "singleton_rate":      round(singletons / max(len(entity_clusters_df), 1), 4),
        "cluster_size_mean":   round(float(sizes.mean()), 2),
        "cluster_size_max":    int(sizes.max()),
        "cluster_size_p95":    round(float(sizes.quantile(0.95)), 2),
        "n_cluster_edges":     int(len(cluster_edges_df[~cluster_edges_df["vetoed"]])),
        "n_vetoed_edges":      int(cluster_edges_df["vetoed"].sum()),
        "avg_deberta_score":   round(float(
            cluster_edges_df.loc[~cluster_edges_df["vetoed"], "deberta_score"].mean()
        ), 4) if len(cluster_edges_df) else 0.0,
        "n_requires_review":   int(entity_clusters_df["requires_review"].sum()),
    }

    log.info("[Metrics] Cluster quality:")
    for k, v in metrics.items():
        log.info(f"  {k}: {v}")

    return metrics


# =============================================================================
# BigQuery write
# =============================================================================

BQ_CLUSTERS_SCHEMA = [
    bigquery.SchemaField("record_id",       "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("cluster_id",      "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("cluster_size",    "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("is_singleton",    "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("avg_edge_score",  "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("max_edge_score",  "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("min_edge_score",  "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("requires_review", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("entity_type",     "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("job_suffix",      "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("date_partition",  "DATE",    mode="REQUIRED"),
]

BQ_EDGES_SCHEMA = [
    bigquery.SchemaField("record_id_1",    "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("record_id_2",    "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("deberta_score",  "FLOAT",   mode="REQUIRED"),
    bigquery.SchemaField("cluster_id",     "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("entity_type",    "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("job_suffix",     "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("date_partition", "DATE",    mode="REQUIRED"),
    bigquery.SchemaField("vetoed",         "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("veto_reason",    "STRING",  mode="NULLABLE"),
]


def _ensure_bq_table(
    client: bigquery.Client,
    table_id: str,
    schema: list,
    partition_field: str = "date_partition",
):
    try:
        client.get_table(table_id)
    except Exception:
        tbl = bigquery.Table(table_id, schema=schema)
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        client.create_table(tbl)
        log.info(f"[BQ] Created table {table_id}")


def write_to_bigquery(
    entity_clusters_df: pd.DataFrame,
    cluster_edges_df: pd.DataFrame,
    cfg: dict,
):
    cfg_bq   = cfg["bigquery"]
    project  = cfg["gcp"]["project_id"]
    dataset  = cfg_bq["dataset"]
    client   = bigquery.Client(project=project)

    clusters_table_id = f"{project}.{dataset}.{cfg_bq['clusters_table']}"
    edges_table_id    = f"{project}.{dataset}.{cfg_bq['edges_table']}"

    _ensure_bq_table(client, clusters_table_id, BQ_CLUSTERS_SCHEMA)
    _ensure_bq_table(client, edges_table_id,    BQ_EDGES_SCHEMA)

    job_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date_partition",
        ),
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    def _upload(df: pd.DataFrame, table_id: str):
        df_copy = df.copy()
        df_copy["date_partition"] = pd.to_datetime(
            df_copy["date_partition"]
        ).dt.date
        job = client.load_table_from_dataframe(df_copy, table_id, job_config=job_cfg)
        job.result()
        log.info(f"[BQ] {len(df)} rows → {table_id}")

    _upload(entity_clusters_df, clusters_table_id)
    _upload(cluster_edges_df,   edges_table_id)


# =============================================================================
# GCS write
# =============================================================================

def write_to_gcs(
    entity_clusters_df: pd.DataFrame,
    cluster_edges_df: pd.DataFrame,
    bucket: str,
    entity_type: str,
    job_suffix: str,
):
    prefix = f"graph/clusters/{entity_type}/{job_suffix}"
    _gcs_upload_csv(entity_clusters_df, bucket, f"{prefix}/entity_clusters.csv")
    _gcs_upload_csv(cluster_edges_df,   bucket, f"{prefix}/cluster_edges.csv")
    return f"gs://{bucket}/{prefix}/"


# =============================================================================
# Main
# =============================================================================

def main():
    config_path = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    cfg         = load_config(config_path)
    bucket      = cfg["data"]["gcs_bucket"]
    entity_type = os.environ.get("ENTITY_TYPE", cfg["data"]["entity_types"][0])
    job_suffix  = JOB_SUFFIX
    threshold   = cfg["graph"]["cluster_edge_threshold"]

    mlflow.set_tracking_uri(
        os.environ.get("MLFLOW_TRACKING_URI") or cfg["mlflow"]["tracking_uri"]
    )
    mlflow.set_experiment(
        cfg.get("graph", {}).get("experiment_name", "entity-resolution-clusters")
    )

    log.info(
        f"[Main] entity={entity_type}  suffix={job_suffix}  "
        f"threshold={threshold}"
    )

    # 1. Load graph data
    edges_df, nodes_df = load_graph_data(bucket, entity_type, job_suffix)

    # 2. Build clusters — veto + threshold + connected components
    entity_clusters_df, cluster_edges_df = build_clusters(
        edges_df    = edges_df,
        nodes_df    = nodes_df,
        threshold   = threshold,
        entity_type = entity_type,
        job_suffix  = job_suffix,
    )

    # 3. Compute metrics
    metrics = compute_cluster_metrics(entity_clusters_df, cluster_edges_df)

    with mlflow.start_run(
        run_name=f"write_clusters_{entity_type}_{job_suffix}",
        tags={
            "entity_type": entity_type,
            "stage":       "write_clusters",
            "job_suffix":  job_suffix,
        },
    ):
        mlflow.log_metrics(metrics)
        mlflow.log_params({
            "threshold":   threshold,
            "entity_type": entity_type,
            "job_suffix":  job_suffix,
        })

        # 4. Write to GCS
        gcs_path = write_to_gcs(
            entity_clusters_df, cluster_edges_df,
            bucket, entity_type, job_suffix,
        )
        log.info(f"[Main] GCS clusters → {gcs_path}")

        # 5. Write to BigQuery
        write_to_bigquery(entity_clusters_df, cluster_edges_df, cfg)

        # 6. Build unified output CSV
        unified_path = build_unified_csv(
            nodes_df, entity_clusters_df, job_suffix, bucket
        )
        mlflow.log_param("unified_output", unified_path)

        # 7. Delete original source file if this was an upload flow run
        if SOURCE_GCS_PATH:
            try:
                _gcs_delete(SOURCE_GCS_PATH)
                log.info(f"[Main] Source file deleted: {SOURCE_GCS_PATH}")
            except Exception as e:
                log.warning(f"[Main] Could not delete source file: {e}")

    log.info("[Main] Done")


if __name__ == "__main__":
    main()
