"""
Entity Resolution — Network Scorer (Contextual Scoring Engine)

Reads resolved clusters from BigQuery, computes network-position-aware
contextual scores, and writes back a network_scores table.

This is the Quantexa-style layer — an entity's score is influenced by
who it is connected to, not just its own attributes.

Scores computed:
    1. cluster_risk_score      — based on cluster size + avg edge confidence
    2. bad_neighbour_score     — 2-hop connection to flagged/suspicious entities
    3. shared_field_score      — same email/phone/DOB across distinct names in cluster
    4. network_centrality      — how connected the entity is across clusters
    5. composite_context_score — weighted combination of all above

Pipeline position:
    build_graph.py → write_clusters.py → score_network.py

Input (BigQuery):
    {dataset}.entity_clusters
    {dataset}.cluster_edges

Output (BigQuery):
    {dataset}.network_scores   — one row per record_id

Env vars:
    CONFIG_PATH
    JOB_SUFFIX         — must match write_clusters job_suffix
    ENTITY_TYPE
    MLFLOW_TRACKING_URI
    FLAGGED_CLUSTER_IDS — comma-separated cluster_ids flagged as bad actors
                          (optional — for bad_neighbour scoring)
                          e.g. "abc123def456,xyz789..."
"""

import logging
import os
from datetime import datetime

import mlflow
import pandas as pd
import yaml
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("score_network")

DEFAULT_CONFIG_PATH = os.environ.get("CONFIG_PATH", "config/training_config.yaml")
JOB_SUFFIX          = os.environ.get("JOB_SUFFIX", "train")
FLAGGED_CLUSTER_IDS = [
    c.strip()
    for c in os.environ.get("FLAGGED_CLUSTER_IDS", "").split(",")
    if c.strip()
]


# =============================================================================
# Config
# =============================================================================

def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


# =============================================================================
# BigQuery read helpers
# =============================================================================

def bq_query(client: bigquery.Client, sql: str) -> pd.DataFrame:
    log.info(f"[BQ] Running query:\n{sql[:200]}…")
    return client.query(sql).to_dataframe()


def load_clusters(
    client: bigquery.Client,
    project: str,
    dataset: str,
    clusters_table: str,
    edges_table: str,
    entity_type: str,
    job_suffix: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load entity_clusters and cluster_edges for this job_suffix."""

    clusters_sql = f"""
        SELECT
            record_id,
            cluster_id,
            cluster_size,
            is_singleton,
            avg_edge_score,
            requires_review,
            entity_type,
            job_suffix
        FROM `{project}.{dataset}.{clusters_table}`
        WHERE entity_type = '{entity_type}'
          AND job_suffix  = '{job_suffix}'
    """

    edges_sql = f"""
        SELECT
            record_id_1,
            record_id_2,
            deberta_score,
            cluster_id,
            vetoed,
            veto_reason
        FROM `{project}.{dataset}.{edges_table}`
        WHERE entity_type = '{entity_type}'
          AND job_suffix  = '{job_suffix}'
          AND vetoed      = FALSE
    """

    clusters_df = bq_query(client, clusters_sql)
    edges_df    = bq_query(client, edges_sql)

    log.info(
        f"[Data] {len(clusters_df)} cluster rows, {len(edges_df)} edges loaded"
    )
    return clusters_df, edges_df


# =============================================================================
# Score 1 — Cluster risk score
# =============================================================================

def score_cluster_risk(clusters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Risk based on cluster properties:
        - Large cluster + low avg confidence = suspicious (over-merge risk)
        - Small cluster + high confidence = clean
        - requires_review flag = elevated risk

    Score range: 0.0 (clean) → 1.0 (high risk)
    """
    df = clusters_df.copy()

    # Normalise cluster size — larger = more suspicious up to a cap of 20
    df["size_risk"] = (df["cluster_size"].clip(upper=20) / 20.0).round(4)

    # Low confidence = higher risk
    df["confidence_risk"] = (1.0 - df["avg_edge_score"].clip(0, 1)).round(4)

    # Review flag boost
    df["review_boost"] = df["requires_review"].astype(float) * 0.3

    df["cluster_risk_score"] = (
        0.4 * df["size_risk"]
        + 0.4 * df["confidence_risk"]
        + 0.2 * df["review_boost"]
    ).clip(0, 1).round(4)

    return df[["record_id", "cluster_id", "cluster_risk_score"]]


# =============================================================================
# Score 2 — Bad neighbour score (2-hop)
# =============================================================================

def score_bad_neighbour(
    clusters_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    flagged_cluster_ids: list[str],
) -> pd.DataFrame:
    """
    2-hop bad neighbour scoring — Quantexa's core contextual signal.

    For each record:
        hop-1: is the record itself in a flagged cluster?
        hop-2: is any record in the same cluster connected (via cluster_edges)
               to a flagged cluster?

    Score: 1.0 if directly in flagged cluster
           0.5 if 2-hop neighbour of flagged cluster
           0.0 otherwise

    In production: flagged_cluster_ids comes from a risk watchlist,
    OFAC SDN matches, or manual analyst flags.
    """
    record_to_cluster = dict(
        zip(clusters_df["record_id"], clusters_df["cluster_id"])
    )

    # All clusters that have a direct edge to a flagged cluster
    flagged_set = set(flagged_cluster_ids)
    two_hop_clusters: set = set()

    if flagged_set and len(edges_df):
        for _, row in edges_df.iterrows():
            c1 = record_to_cluster.get(str(row["record_id_1"]))
            c2 = record_to_cluster.get(str(row["record_id_2"]))
            if c1 in flagged_set:
                two_hop_clusters.add(c2)
            if c2 in flagged_set:
                two_hop_clusters.add(c1)
        two_hop_clusters -= flagged_set  # exclude direct hits

    rows = []
    for _, row in clusters_df.iterrows():
        cid   = row["cluster_id"]
        score = 0.0
        if cid in flagged_set:
            score = 1.0
        elif cid in two_hop_clusters:
            score = 0.5
        rows.append({
            "record_id":          row["record_id"],
            "bad_neighbour_score": round(score, 4),
            "is_flagged_direct":  cid in flagged_set,
            "is_flagged_2hop":    cid in two_hop_clusters,
        })

    return pd.DataFrame(rows)


# =============================================================================
# Score 3 — Shared field anomaly score
# =============================================================================

def score_shared_fields(
    clusters_df: pd.DataFrame,
    edges_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Detect clusters where the same linking field (email, phone, DOB)
    appears on records with clearly different names — fraud ring signal.

    Uses edge-level rule scores from cluster_edges:
        email_score = 1.0 but deberta_score low — same email, different identity
        dob_score   = 1.0 but deberta_score low — same DOB, different name

    Score: 0.0 = no anomaly, 1.0 = strong shared-field anomaly
    """
    record_to_cluster = dict(
        zip(clusters_df["record_id"], clusters_df["cluster_id"])
    )

    # Find rule-score columns available
    rule_score_cols = [
        c for c in edges_df.columns
        if c.endswith("_score") and c != "deberta_score"
    ]

    cluster_anomaly: dict[str, float] = {}

    for cid, grp in edges_df.groupby(
        edges_df["record_id_1"].map(record_to_cluster)
    ):
        if cid is None:
            continue
        anomaly = 0.0
        for col in rule_score_cols:
            if col not in grp.columns:
                continue
            # High rule score + low deberta = shared field across different identities
            suspicious = grp[
                (grp[col] >= 0.9) & (grp["deberta_score"] < 0.55)
            ]
            if len(suspicious):
                anomaly = min(1.0, anomaly + 0.3 * len(suspicious) / max(len(grp), 1))
        cluster_anomaly[cid] = round(min(1.0, anomaly), 4)

    rows = []
    for _, row in clusters_df.iterrows():
        rows.append({
            "record_id":          row["record_id"],
            "shared_field_score": cluster_anomaly.get(row["cluster_id"], 0.0),
        })
    return pd.DataFrame(rows)


# =============================================================================
# Score 4 — Network centrality
# =============================================================================

def score_network_centrality(
    clusters_df: pd.DataFrame,
    edges_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Simple degree centrality — how many distinct clusters does this
    record's cluster connect to via cluster_edges?

    High centrality = hub entity = higher scrutiny warranted.
    Normalised to [0, 1] by dividing by max degree in graph.
    """
    record_to_cluster = dict(
        zip(clusters_df["record_id"], clusters_df["cluster_id"])
    )

    cluster_connections: dict[str, set] = {}
    for _, row in edges_df.iterrows():
        c1 = record_to_cluster.get(str(row["record_id_1"]))
        c2 = record_to_cluster.get(str(row["record_id_2"]))
        if c1 and c2 and c1 != c2:
            cluster_connections.setdefault(c1, set()).add(c2)
            cluster_connections.setdefault(c2, set()).add(c1)

    max_degree = max(
        (len(v) for v in cluster_connections.values()), default=1
    )

    rows = []
    for _, row in clusters_df.iterrows():
        cid    = row["cluster_id"]
        degree = len(cluster_connections.get(cid, set()))
        rows.append({
            "record_id":           row["record_id"],
            "network_centrality":  round(degree / max_degree, 4),
        })
    return pd.DataFrame(rows)


# =============================================================================
# Composite score
# =============================================================================

def build_composite_score(
    clusters_df: pd.DataFrame,
    risk_df: pd.DataFrame,
    bad_neighbour_df: pd.DataFrame,
    shared_field_df: pd.DataFrame,
    centrality_df: pd.DataFrame,
    cfg_scoring: dict,
) -> pd.DataFrame:
    """
    Weighted composite of all four scores.

    Weights configurable via config.network_scoring.weights.
    Default: cluster_risk=0.3, bad_neighbour=0.4, shared_field=0.2, centrality=0.1
    """
    weights = cfg_scoring.get("weights", {})
    w_risk        = weights.get("cluster_risk",    0.30)
    w_bad_nbr     = weights.get("bad_neighbour",   0.40)
    w_shared      = weights.get("shared_field",    0.20)
    w_centrality  = weights.get("centrality",      0.10)

    # Merge all scores on record_id
    df = clusters_df[["record_id", "cluster_id", "cluster_size", "is_singleton"]].copy()
    df = df.merge(risk_df,          on="record_id", how="left")
    df = df.merge(bad_neighbour_df, on="record_id", how="left")
    df = df.merge(shared_field_df,  on="record_id", how="left")
    df = df.merge(centrality_df,    on="record_id", how="left")

    df = df.fillna(0.0)

    df["composite_context_score"] = (
        w_risk       * df["cluster_risk_score"]
        + w_bad_nbr  * df["bad_neighbour_score"]
        + w_shared   * df["shared_field_score"]
        + w_centrality * df["network_centrality"]
    ).clip(0, 1).round(4)

    df["scored_at"] = datetime.utcnow().strftime("%Y-%m-%d")

    return df


# =============================================================================
# BigQuery write
# =============================================================================

BQ_NETWORK_SCORES_SCHEMA = [
    bigquery.SchemaField("record_id",               "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("cluster_id",              "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("cluster_size",            "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("is_singleton",            "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("cluster_risk_score",      "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("bad_neighbour_score",     "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("is_flagged_direct",       "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("is_flagged_2hop",         "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("shared_field_score",      "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("network_centrality",      "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("composite_context_score", "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("scored_at",               "DATE",    mode="NULLABLE"),
]


def write_scores_to_bigquery(
    scores_df: pd.DataFrame,
    cfg: dict,
    job_suffix: str,
):
    cfg_bq   = cfg["bigquery"]
    project  = cfg["gcp"]["project_id"]
    dataset  = cfg_bq["dataset"]
    table    = cfg_bq.get("network_scores_table", "network_scores")
    table_id = f"{project}.{dataset}.{table}"
    client   = bigquery.Client(project=project)

    # Delete existing rows if table exists
    try:
        client.get_table(table_id)
        client.query(
            f"DELETE FROM `{table_id}` WHERE cluster_id IN "
            f"(SELECT cluster_id FROM `{project}.{dataset}.{cfg_bq['clusters_table']}` "
            f"WHERE job_suffix = '{job_suffix}')"
        ).result()
    except Exception:
        pass  # table doesn't exist yet — will be created on load

    job_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )
    df_copy = scores_df.copy()
    df_copy["scored_at"] = pd.to_datetime(df_copy["scored_at"]).dt.date
    job = client.load_table_from_dataframe(df_copy, table_id, job_config=job_cfg)
    job.result()
    log.info(f"[BQ] {len(scores_df)} network scores → {table_id}")


# =============================================================================
# Main
# =============================================================================

def main():
    config_path = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    cfg         = load_config(config_path)
    entity_type = os.environ.get("ENTITY_TYPE", cfg["data"]["entity_types"][0])
    job_suffix  = JOB_SUFFIX
    cfg_bq      = cfg["bigquery"]
    project     = cfg["gcp"]["project_id"]

    mlflow.set_tracking_uri(
        os.environ.get("MLFLOW_TRACKING_URI") or cfg["mlflow"]["tracking_uri"]
    )
    mlflow.set_experiment(
        cfg.get("network_scoring", {}).get(
            "experiment_name", "entity-resolution-network"
        )
    )

    log.info(
        f"[Main] entity={entity_type}  suffix={job_suffix}  "
        f"flagged_clusters={len(FLAGGED_CLUSTER_IDS)}"
    )

    client = bigquery.Client(project=project)

    # 1. Load clusters + edges from BigQuery
    clusters_df, edges_df = load_clusters(
        client        = client,
        project       = project,
        dataset       = cfg_bq["dataset"],
        clusters_table= cfg_bq["clusters_table"],
        edges_table   = cfg_bq["edges_table"],
        entity_type   = entity_type,
        job_suffix     = job_suffix,
    )

    if clusters_df.empty:
        log.warning("[Main] No clusters found — run write_clusters.py first")
        return

    # 2. Compute individual scores
    log.info("[Scores] Computing cluster risk scores…")
    risk_df = score_cluster_risk(clusters_df)

    log.info("[Scores] Computing bad-neighbour scores…")
    bad_neighbour_df = score_bad_neighbour(
        clusters_df, edges_df, FLAGGED_CLUSTER_IDS
    )

    log.info("[Scores] Computing shared-field anomaly scores…")
    shared_field_df = score_shared_fields(clusters_df, edges_df)

    log.info("[Scores] Computing network centrality…")
    centrality_df = score_network_centrality(clusters_df, edges_df)

    # 3. Composite score
    cfg_scoring = cfg.get("network_scoring", {})
    scores_df   = build_composite_score(
        clusters_df, risk_df, bad_neighbour_df,
        shared_field_df, centrality_df, cfg_scoring,
    )

    log.info(
        f"[Scores] composite_context_score — "
        f"mean={scores_df['composite_context_score'].mean():.4f}  "
        f"max={scores_df['composite_context_score'].max():.4f}"
    )

    # 4. Write to BigQuery
    with mlflow.start_run(
        run_name=f"score_network_{entity_type}_{job_suffix}",
        tags={
            "entity_type": entity_type,
            "stage":       "score_network",
            "job_suffix":  job_suffix,
        },
    ):
        mlflow.log_params({
            "entity_type":         entity_type,
            "job_suffix":          job_suffix,
            "n_flagged_clusters":  len(FLAGGED_CLUSTER_IDS),
            "scoring_weights":     str(cfg_scoring.get("weights", {})),
        })
        mlflow.log_metrics({
            "n_records_scored":     len(scores_df),
            "mean_context_score":   float(scores_df["composite_context_score"].mean()),
            "max_context_score":    float(scores_df["composite_context_score"].max()),
            "n_flagged_direct":     int(scores_df["is_flagged_direct"].sum()),
            "n_flagged_2hop":       int(scores_df["is_flagged_2hop"].sum()),
            "n_high_risk":          int((scores_df["composite_context_score"] >= 0.7).sum()),
        })

        write_scores_to_bigquery(scores_df, cfg, job_suffix)

    log.info("[Main] Done")


if __name__ == "__main__":
    main()
