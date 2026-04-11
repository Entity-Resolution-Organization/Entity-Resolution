"""
Entity Resolution — Graph Builder

Generates candidate edges from a record population by:
    1. Load      — from GCS CSV (default) or BigQuery 48h incremental (stub)
    2. Blocking  — reduce N² to manageable candidate pairs
    3. Scoring   — Vertex AI Batch Prediction (DeBERTa)
    4. Enriching — rule-based signals from config field_rules (weighted)
    5. Veto      — hard rules applied post-enrichment (e.g. DOB mismatch)
    6. Output    — enriched_edges.csv + node_features.csv → GCS

Input modes (INPUT_MODE env var):
    csv      — load from GCS. Source file resolved in order:
                 1. RECORDS_GCS_PATH env var (explicit path)
                 2. Scan config.graph.process_csv_bucket/to-process/ for CSVs
                 3. Extract records from train.csv + val.csv (training fallback)
               Default mode.
    bigquery — load from BigQuery last-48h window (stub, not yet implemented)

Output (namespaced by job_suffix, no date in path):
    graph/{entity_type}/{job_suffix}/enriched_edges.csv
    graph/{entity_type}/{job_suffix}/node_features.csv

    job_suffix derived from source filename — prevents collision when multiple
    files are processed. Training run always uses suffix="train".

Lifecycle (CSV upload flow):
    to-process/{filename}.csv
        → build_graph writes enriched_edges + node_features
        → source file NOT deleted here — deleted by write_clusters.py
          after full pipeline completes successfully

Model resource — resolved in order:
    1. MODEL_RESOURCE_NAME env var
    2. pipeline-results/endpoint_info.json in GCS (key: "model")
    3. RuntimeError — no hardcoded fallback

Field rules — defined in config under field_rules[]:
    field            — column name in source data
    scorer           — email | phone | date_exact | jaro_winkler | exact
    weight           — float, multiplied into edge feature score
    veto_on_mismatch — bool, hard veto when field present + mismatched
    has_flag         — bool, include has_{field} presence flag
"""

import json
import logging
import os
import re
import time
import unicodedata
from datetime import datetime
from io import StringIO
from typing import Any, Optional

import pandas as pd
import yaml
from google.cloud import storage
from google.cloud import aiplatform

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("build_graph")

DEFAULT_CONFIG_PATH = os.environ.get("CONFIG_PATH", "config/training_config.yaml")
MAX_CANDIDATES      = int(os.environ.get("MAX_CANDIDATES", 25))
RECORDS_GCS_PATH    = os.environ.get("RECORDS_GCS_PATH", "")
DEBERTA_THRESHOLD   = 0.30
GCP_PROJECT         = os.environ.get("GCP_PROJECT", "entity-resolution-487121")
GCP_REGION          = os.environ.get("GCP_REGION", "us-central1")
INPUT_MODE          = os.environ.get("INPUT_MODE", "csv")   # csv | bigquery


# =============================================================================
# Config
# =============================================================================

def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_field_rules(config: dict) -> list[dict]:
    """
    Return field_rules from config with defaults applied.
    Each rule: {field, scorer, weight, veto_on_mismatch, has_flag}
    """
    rules = config.get("field_rules", [])
    for r in rules:
        r.setdefault("weight",           1.0)
        r.setdefault("veto_on_mismatch", False)
        r.setdefault("has_flag",         True)
        r.setdefault("scorer",           "jaro_winkler")
    return rules


def resolve_model_resource_name(config: dict) -> str:
    """
    Resolve MODEL_RESOURCE_NAME:
      1. MODEL_RESOURCE_NAME env var
      2. pipeline-results/endpoint_info.json in GCS (key: "model")
      3. RuntimeError — no hardcoded fallback
    """
    from_env = os.environ.get("MODEL_RESOURCE_NAME", "").strip()
    if from_env:
        log.info(f"[Model] Resource from env: {from_env}")
        return from_env

    try:
        bucket_name = config["data"]["gcs_bucket"]
        info = json.loads(
            storage.Client()
            .bucket(bucket_name)
            .blob("pipeline-results/endpoint_info.json")
            .download_as_text()
        )
        name = info.get("model") or info.get("model_resource_name")
        if name:
            log.info(f"[Model] Resource from GCS endpoint_info: {name}")
            return name
    except Exception as e:
        log.warning(f"[Model] Could not read endpoint_info.json: {e}")

    raise RuntimeError(
        "MODEL_RESOURCE_NAME not set and endpoint_info.json not found or "
        "missing 'model' key. Set MODEL_RESOURCE_NAME env var."
    )


# =============================================================================
# Helpers
# =============================================================================

def _safe_str(val) -> str:
    """Null safety only — returns '' for missing/NaN. Use before any comparison."""
    if pd.isna(val) or str(val).strip() == "":
        return ""
    return str(val).strip().lower()


def _normalise(s: str) -> str:
    """Strip accents, punctuation, lowercase. Use for fuzzy text comparison."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9\s]", "", s.lower())
    return s.strip()


def _jaro_winkler(s1: str, s2: str) -> float:
    if not s1 or not s2:
        return 0.0
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    match_dist  = max(len1, len2) // 2 - 1
    s1_matches  = [False] * len1
    s2_matches  = [False] * len2
    matches = transpositions = 0
    for i in range(len1):
        lo = max(0, i - match_dist)
        hi = min(i + match_dist + 1, len2)
        for j in range(lo, hi):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = s2_matches[j] = True
            matches += 1
            break
    if not matches:
        return 0.0
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    jaro = (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3
    prefix = 0
    for i in range(min(4, len1, len2)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * 0.1 * (1 - jaro)


# =============================================================================
# Field scorers
# =============================================================================

def _score_email(v1: str, v2: str) -> float:
    v1, v2 = _safe_str(v1), _safe_str(v2)
    if not v1 or not v2:
        return 0.0
    if v1 == v2:
        return 1.0
    u1 = v1.split("@")[0] if "@" in v1 else v1
    u2 = v2.split("@")[0] if "@" in v2 else v2
    if u1 == u2:
        return 0.6
    return _jaro_winkler(v1, v2)


def _score_phone(v1: str, v2: str) -> float:
    v1 = re.sub(r"\D", "", _safe_str(v1))
    v2 = re.sub(r"\D", "", _safe_str(v2))
    if not v1 or not v2:
        return 0.0
    if v1[-9:] == v2[-9:]:
        return 1.0
    return _jaro_winkler(v1, v2)


def _score_date_exact(v1: str, v2: str) -> float:
    """
    Returns 1.0 (match), 0.0 (mismatch), -1.0 (missing — sentinel).
    Caller checks for -1.0 and treats as absent.
    """
    v1, v2 = _safe_str(v1), _safe_str(v2)
    if not v1 or not v2:
        return -1.0
    v1 = re.sub(r"[-/\\.]", "", v1)
    v2 = re.sub(r"[-/\\.]", "", v2)
    return 1.0 if v1 == v2 else 0.0


def _score_exact(v1: str, v2: str) -> float:
    v1, v2 = _normalise(_safe_str(v1)), _normalise(_safe_str(v2))
    if not v1 or not v2:
        return 0.0
    return 1.0 if v1 == v2 else 0.0


def _score_jaro_winkler(v1: str, v2: str) -> float:
    v1, v2 = _normalise(_safe_str(v1)), _normalise(_safe_str(v2))
    if not v1 or not v2:
        return 0.0
    return _jaro_winkler(v1, v2)


SCORER_MAP = {
    "email":        _score_email,
    "phone":        _score_phone,
    "date_exact":   _score_date_exact,
    "exact":        _score_exact,
    "jaro_winkler": _score_jaro_winkler,
}


# =============================================================================
# Blocking
# =============================================================================

def generate_blocking_keys(row: pd.Series, cols: dict) -> list[str]:
    """
    Supports both pair-style columns (name1, address1) and
    record-style columns (name, address) — tries config key first,
    falls back to bare field name so extracted records.csv works
    without a separate column rename step.
    """
    keys = []

    # Name — try name1 (pairs format) then name (records format)
    name_val = (
        row.get(cols.get("name1", "name1"), "")
        or row.get("name", "")
    )
    name = _normalise(_safe_str(name_val))
    if name:
        keys.append(f"name4:{name[:4]}")
        parts = name.split()
        if parts:
            keys.append(f"surname:{parts[-1][:5]}")

    # DOB year — bare "dob" column (same in both formats)
    dob = _safe_str(row.get("dob", ""))
    if dob:
        dob_norm = re.sub(r"\D", "", dob)
        if len(dob_norm) >= 4:
            keys.append(f"dobyear:{dob_norm[-4:]}")

    # Email username prefix
    email = _safe_str(row.get("email", ""))
    if email and "@" in email:
        keys.append(f"emailuser:{email.split('@')[0][:6]}")

    # Phone last 6 digits
    phone = re.sub(r"\D", "", _safe_str(row.get("phone", "")))
    if phone:
        keys.append(f"phone:{phone[-6:]}")

    # Address — try address1 (pairs format) then address (records format)
    addr_val = (
        row.get(cols.get("address1", "address1"), "")
        or row.get("address", "")
    )
    address = _safe_str(addr_val)
    if address:
        street_num = re.match(r"(\d+)", address.strip())
        if street_num:
            keys.append(f"streetnum:{street_num.group()}")
        tokens = address.split()
        if tokens and len(tokens[-1]) >= 3:
            keys.append(f"post:{tokens[-1].upper()[:3]}")

    return keys if keys else []


def get_candidate_pairs(df: pd.DataFrame, cols: dict) -> list[tuple]:
    from collections import defaultdict
    log.info(f"[Blocking] {len(df)} records — generating candidates…")

    index: dict[str, list] = {}
    for idx, row in df.iterrows():
        for key in generate_blocking_keys(row, cols):
            index.setdefault(key, []).append(idx)

    # Apply cap inline — won't build full pair set in memory
    count: dict     = defaultdict(int)
    seen_pairs: set = set()
    filtered: list  = []

    for key, idxs in index.items():
        for i in range(len(idxs)):
            for j in range(i + 1, len(idxs)):
                a, b = min(idxs[i], idxs[j]), max(idxs[i], idxs[j])
                if (a, b) in seen_pairs:
                    continue
                if count[a] < MAX_CANDIDATES and count[b] < MAX_CANDIDATES:
                    seen_pairs.add((a, b))
                    filtered.append((a, b))
                    count[a] += 1
                    count[b] += 1

    log.info(f"[Blocking] {len(filtered)} candidate pairs (cap={MAX_CANDIDATES})")
    return filtered


# =============================================================================
# DeBERTa scoring — Vertex AI Batch Prediction
# =============================================================================

def _write_pairs_jsonl(
    pairs: list[tuple],
    df: pd.DataFrame,
    cols: dict,
    gcs_path: str,
) -> None:
    # Resolve name/address columns — support both pair-style and record-style
    name_col    = cols.get("name1", "name1")
    address_col = cols.get("address1", "address1")
    if name_col not in df.columns:
        name_col = "name"
    if address_col not in df.columns:
        address_col = "address"

    lines = []
    for a, b in pairs:
        ra, rb = df.loc[a], df.loc[b]
        lines.append(json.dumps({
            "name1":    str(ra.get(name_col, "")),
            "address1": str(ra.get(address_col, "")),
            "name2":    str(rb.get(name_col, "")),
            "address2": str(rb.get(address_col, "")),
            "pair_key": f"{a}__{b}",
        }))
    bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
    storage.Client().bucket(bucket_name).blob(blob_name).upload_from_string(
        "\n".join(lines), content_type="application/jsonl"
    )
    log.info(f"[BatchPred] Wrote {len(lines)} instances → {gcs_path}")


def _read_batch_predictions(gcs_output_prefix: str) -> dict[tuple, float]:
    client = storage.Client()
    bucket_name, prefix = gcs_output_prefix.replace("gs://", "").split("/", 1)
    bucket     = client.bucket(bucket_name)
    blobs      = list(bucket.list_blobs(prefix=prefix))
    pred_blobs = [b for b in blobs if "prediction.results" in b.name]

    if not pred_blobs:
        raise RuntimeError(
            f"No prediction result files found at {gcs_output_prefix}."
        )

    scores = {}
    for blob in pred_blobs:
        for line in blob.download_as_text().strip().split("\n"):
            if not line:
                continue
            record     = json.loads(line)
            instance   = record["instance"]
            prediction = record["prediction"]
            pair_key   = instance.get("pair_key", "")
            if not pair_key or "__" not in pair_key:
                log.warning(f"[BatchPred] Malformed pair_key: {pair_key}")
                continue
            a_str, b_str = pair_key.split("__", 1)
            scores[(int(a_str), int(b_str))] = float(prediction["probability"])

    log.info(f"[BatchPred] Read {len(scores)} predictions from {gcs_output_prefix}")
    return scores


ONLINE_SCORING_THRESHOLD = int(os.environ.get("ONLINE_SCORING_THRESHOLD", 1000))


def _score_pairs_online(
    pairs: list[tuple],
    df: pd.DataFrame,
    cols: dict,
) -> dict[tuple, float]:
    """
    Score pairs via the online endpoint (model_client).
    Fast for small jobs — no container startup overhead.
    """
    from scripts.model_client import get_client

    name_col    = cols.get("name1", "name1")
    address_col = cols.get("address1", "address1")
    if name_col not in df.columns:
        name_col = "name"
    if address_col not in df.columns:
        address_col = "address"

    client = get_client()
    scores = {}
    batch_size = 64

    for i in range(0, len(pairs), batch_size):
        batch = pairs[i:i + batch_size]
        input_pairs = []
        for a, b in batch:
            ra, rb = df.loc[a], df.loc[b]
            input_pairs.append({
                "name1":    str(ra.get(name_col, "")),
                "address1": str(ra.get(address_col, "")),
                "name2":    str(rb.get(name_col, "")),
                "address2": str(rb.get(address_col, "")),
            })
        results = client.predict(input_pairs)
        for (a, b), result in zip(batch, results):
            scores[(a, b)] = result.probability
        log.info(f"[Online] Scored {min(i + batch_size, len(pairs))}/{len(pairs)}")

    return scores


def score_pairs_deberta(
    pairs: list[tuple],
    df: pd.DataFrame,
    cols: dict,
    model_resource_name: str,
    gcs_staging_prefix: str,
    job_suffix: str,
) -> dict[tuple, float]:
    """
    Score candidate pairs via DeBERTa model.
    Uses online endpoint for small jobs (< ONLINE_SCORING_THRESHOLD pairs),
    Vertex AI Batch Prediction for large jobs.
    """
    # Small job — use online endpoint directly (no container startup)
    if len(pairs) <= ONLINE_SCORING_THRESHOLD:
        log.info(
            f"[Scoring] {len(pairs)} pairs <= {ONLINE_SCORING_THRESHOLD} "
            f"— using online endpoint"
        )
        return _score_pairs_online(pairs, df, cols)

    # Large job — batch prediction
    log.info(f"[BatchPred] Scoring {len(pairs)} pairs  suffix={job_suffix}")

    input_gcs  = f"{gcs_staging_prefix}/{job_suffix}/batch_input/pairs.jsonl"
    output_gcs = f"{gcs_staging_prefix}/{job_suffix}/batch_output"

    # Check if predictions already exist — safe to resume after crash
    bucket_name, prefix = output_gcs.replace("gs://", "").split("/", 1)
    existing = list(
        storage.Client().bucket(bucket_name).list_blobs(prefix=prefix + "/")
    )

    if existing:
        log.info("[BatchPred] Existing predictions found — skipping job submission")
    else:
        _write_pairs_jsonl(pairs, df, cols, input_gcs)

        aiplatform.init(project=GCP_PROJECT, location=GCP_REGION)
        job = aiplatform.BatchPredictionJob.create(
            job_display_name       = f"er-graph-{job_suffix}",
            model_name             = model_resource_name,
            instances_format       = "jsonl",
            predictions_format     = "jsonl",
            gcs_source             = input_gcs,
            gcs_destination_prefix = output_gcs,
            machine_type           = "n1-standard-4",
            accelerator_type       = "NVIDIA_TESLA_T4",
            accelerator_count      = 1,
            starting_replica_count = 1,
            max_replica_count      = 5,
            sync                   = False,
        )

        time.sleep(5)
        log.info(f"[BatchPred] Job submitted: {job.resource_name}")

        resource_name = job.resource_name
        poll_interval = 30
        while True:
            job   = aiplatform.BatchPredictionJob(resource_name)
            state = job.state.name
            log.info(f"[BatchPred] Job state: {state}")
            if state == "JOB_STATE_SUCCEEDED":
                break
            elif state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED"):
                raise RuntimeError(
                    f"Batch prediction job failed: {state}. "
                    f"Console: {resource_name}"
                )
            time.sleep(poll_interval)

    # Read predictions — Vertex writes under {output_gcs}/prediction-*/
    bucket_name_out, out_prefix = output_gcs.replace("gs://", "").split("/", 1)
    bucket  = storage.Client().bucket(bucket_name_out)
    subdirs = set()
    for blob in bucket.list_blobs(prefix=out_prefix):
        parts = blob.name[len(out_prefix):].lstrip("/").split("/")
        if parts and parts[0]:
            subdirs.add(parts[0])

    scores = {}
    for subdir in subdirs:
        full_prefix = f"gs://{bucket_name_out}/{out_prefix}/{subdir}"
        scores.update(_read_batch_predictions(full_prefix))

    log.info(f"[BatchPred] Done — {len(scores)} scores returned")
    return scores


# =============================================================================
# Edge enrichment — config-driven field rules with weights and veto logic
# =============================================================================

def enrich_edge(
    ra: pd.Series,
    rb: pd.Series,
    deberta_score: float,
    field_rules: list[dict],
) -> dict:
    """
    Build enriched edge feature dict driven entirely by field_rules config.

    Each rule produces:
        {field}_score  — scorer output × weight (0.0 if field absent)
        has_{field}    — presence flag (if rule has_flag=True)

    Veto logic:
        veto_on_mismatch=True + both fields present + score=0.0
        → vetoed=True, veto_reason records which field fired

    date_exact returns -1.0 sentinel for missing — treated as absent,
    never appears in output.
    """
    edge: dict[str, Any] = {
        "deberta_score": deberta_score,
        "vetoed":        False,
        "veto_reason":   "",
    }

    for rule in field_rules:
        field   = rule["field"]
        scorer  = SCORER_MAP.get(rule["scorer"], _score_jaro_winkler)
        weight  = float(rule.get("weight", 1.0))
        veto    = bool(rule.get("veto_on_mismatch", False))
        has_flg = bool(rule.get("has_flag", True))

        v1 = ra.get(field, "")
        v2 = rb.get(field, "")
        both_present = bool(_safe_str(v1)) and bool(_safe_str(v2))

        if both_present:
            raw_score = scorer(str(v1), str(v2))
            if raw_score == -1.0:       # date_exact missing sentinel
                both_present = False
                raw_score    = 0.0
        else:
            raw_score = 0.0

        weighted_score = round(raw_score * weight, 6) if both_present else 0.0
        edge[f"{field}_score"] = weighted_score

        if has_flg:
            edge[f"has_{field}"] = int(both_present)

        # Veto: present on both sides, definitively mismatched (score=0)
        if veto and both_present and raw_score == 0.0:
            edge["vetoed"]      = True
            edge["veto_reason"] = (
                (edge["veto_reason"] + f"; {field} mismatch").lstrip("; ")
            )

    return edge


# =============================================================================
# Node features — record ID manifest (embeddings shelved with GNN)
# =============================================================================

def build_node_features(df: pd.DataFrame, cols: dict) -> pd.DataFrame:
    """
    Build a minimal node_features.csv with record_id.
    Embeddings shelved until GNN is reintroduced — write_clusters.py
    only needs the record_id column from this file.
    """
    id_col = cols.get("id1", "id1")
    if id_col not in df.columns:
        id_col = "id"
    node_df = pd.DataFrame({"record_id": df[id_col].astype(str).values})
    log.info(f"[Nodes] {len(node_df)} record IDs extracted")
    return node_df


# =============================================================================
# Record loaders
# =============================================================================

def load_records_csv(gcs_path: str) -> pd.DataFrame:
    """Load records from a GCS CSV path."""
    client = storage.Client()
    bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
    content = client.bucket(bucket_name).blob(blob_name).download_as_text()
    df = pd.read_csv(StringIO(content))
    log.info(f"[Load] {len(df)} records from {gcs_path}")
    return df


def extract_records_from_pairs(
    pairs_gcs_paths: list[str],
    config: dict,
) -> str:
    """
    Extract unique individual records from one or more pairs CSVs
    (e.g. train.csv + val.csv).

    Both sides of each pair are deduplicated into a flat records DataFrame.
    Output columns follow config column names so blocking keys resolve
    correctly without any rename step:
        id, name1, address1, dob, email, phone, company, ...

    Args:
        pairs_gcs_paths: list of GCS paths to pairs CSVs
        config:          loaded training config

    Returns:
        GCS path to uploaded records CSV
    """
    cols        = config["data"]["columns"]
    bucket      = config["data"]["gcs_bucket"]
    field_rules = get_field_rules(config)

    id1_col   = cols.get("id1",      "id1")
    id2_col   = cols.get("id2",      "id2")
    name1_col = cols.get("name1",    "name1")
    name2_col = cols.get("name2",    "name2")
    addr1_col = cols.get("address1", "address1")
    addr2_col = cols.get("address2", "address2")

    seen: set  = set()
    rows: list = []

    for gcs_path in pairs_gcs_paths:
        df = load_records_csv(gcs_path)

        for _, row in df.iterrows():
            for id_col, name_col, addr_col, side_suffix in [
                (id1_col, name1_col, addr1_col, "1"),
                (id2_col, name2_col, addr2_col, "2"),
            ]:
                rid = str(row.get(id_col, "")).strip()
                if not rid or rid in seen:
                    continue
                seen.add(rid)

                # Output uses config column names so blocking resolves correctly
                record = {
                    "id":      rid,
                    name1_col: row.get(name_col, ""),    # → "name1"
                    addr1_col: row.get(addr_col, ""),    # → "address1"
                }

                # Extra fields from field_rules — pairs CSV has dob1/dob2 etc.
                # Records CSV stores as bare field name (dob, email, phone...)
                for rule in field_rules:
                    field    = rule["field"]
                    pair_col = f"{field}{side_suffix}"   # e.g. dob1, email2
                    if pair_col in row.index:
                        record[field] = row[pair_col]
                    elif field in row.index:
                        record[field] = row[field]

                rows.append(record)

    records_df  = pd.DataFrame(rows)
    output_path = f"gs://{bucket}/graph/person/train/records.csv"
    upload_csv_to_gcs(records_df, output_path)
    log.info(
        f"[Records] Extracted {len(records_df)} unique records "
        f"from {len(pairs_gcs_paths)} file(s) → {output_path}"
    )
    return output_path


def scan_process_bucket(config: dict) -> list[str]:
    """
    Scan to-process/ folder in process_csv_bucket.
    Returns list of GCS paths to unprocessed CSV files.
    """
    bucket_name = config["graph"].get(
        "process_csv_bucket", config["data"]["gcs_bucket"]
    )
    client = storage.Client()
    blobs  = client.list_blobs(bucket_name, prefix="to-process/")
    paths  = [
        f"gs://{bucket_name}/{b.name}"
        for b in blobs
        if b.name.endswith(".csv") and not b.name.endswith("/")
    ]
    log.info(f"[Scan] Found {len(paths)} CSV(s) in to-process/: {paths}")
    return paths


def load_records_bigquery(config: dict) -> pd.DataFrame:
    """
    Load records added in the last 48 hours from BigQuery.
    TODO: implement incremental BigQuery load.
        - Query: SELECT * FROM source_records
                 WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
        - Merge with existing blocking index for cross-corpus candidate generation
        - Return DataFrame in same schema as load_records_csv
    """
    raise NotImplementedError(
        "BigQuery 48h incremental load not yet implemented. "
        "Set INPUT_MODE=csv or implement this function."
    )


# =============================================================================
# GCS helpers
# =============================================================================

def upload_csv_to_gcs(df: pd.DataFrame, gcs_path: str):
    client = storage.Client()
    bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
    client.bucket(bucket_name).blob(blob_name).upload_from_string(
        df.to_csv(index=False), content_type="text/csv"
    )
    log.info(f"[GCS] Uploaded {len(df)} rows → {gcs_path}")


def delete_gcs_file(gcs_path: str):
    """Delete source file after successful pipeline completion."""
    client = storage.Client()
    bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
    client.bucket(bucket_name).blob(blob_name).delete()
    log.info(f"[GCS] Deleted source file: {gcs_path}")


def derive_job_suffix(gcs_path: str) -> str:
    """
    Derive a stable, filesystem-safe job suffix from the source filename.
    gs://.../to-process/orders_april.csv → orders_april
    """
    filename = gcs_path.rstrip("/").split("/")[-1]
    stem     = filename.rsplit(".", 1)[0]
    safe     = re.sub(r"[^a-zA-Z0-9_\-]", "_", stem)
    return safe[:64]


# =============================================================================
# Core build_graph
# =============================================================================

def build_graph(
    records_path: str,
    output_prefix: str,
    model_resource_name: str,
    config: dict,
    entity_type: Optional[str] = None,
    date_partition: Optional[str] = None,
    job_suffix: Optional[str] = None,
) -> tuple[str, str]:
    """
    Full graph build pipeline for a single records file.

    Returns:
        (edges_gcs_path, nodes_gcs_path)
    """
    date_partition = date_partition or datetime.utcnow().strftime("%Y-%m-%d")
    entity_type    = entity_type    or config["data"]["entity_types"][0]
    job_suffix     = job_suffix     or derive_job_suffix(records_path)
    cols           = config["data"]["columns"]
    field_rules    = get_field_rules(config)
    gcs_bucket     = config["data"]["gcs_bucket"]
    graph_prefix   = config.get("graph", {}).get("graph_prefix", "graph")
    gcs_staging    = f"gs://{gcs_bucket}/{graph_prefix}/batch_staging"

    log.info(
        f"[Build] entity={entity_type}  suffix={job_suffix}  "
        f"source={records_path}"
    )
    log.info(
        f"[Build] Field rules ({len(field_rules)}): "
        f"{[r['field'] for r in field_rules]}"
    )

    # 1. Load records
    df = load_records_csv(records_path)

    # 2. Blocking
    pairs = get_candidate_pairs(df, cols)
    if not pairs:
        log.warning("[Build] No candidate pairs — check blocking keys and input data")
        return "", ""

    # 3. DeBERTa scoring via Batch Prediction
    scores = score_pairs_deberta(
        pairs=pairs,
        df=df,
        cols=cols,
        model_resource_name=model_resource_name,
        gcs_staging_prefix=gcs_staging,
        job_suffix=job_suffix,
    )

    # 4. Filter at DEBERTA_THRESHOLD
    candidates = [(a, b) for (a, b), s in scores.items() if s >= DEBERTA_THRESHOLD]
    log.info(f"[Build] {len(candidates)} edges above threshold {DEBERTA_THRESHOLD}")

    if not candidates:
        log.warning("[Build] No edges above threshold — check model and data quality")
        return "", ""

    # 5. Enrich edges
    id_col = cols.get("id1", "id1")
    if id_col not in df.columns:
        id_col = "id"
    rows   = []
    vetoed = 0
    for a, b in candidates:
        ra, rb = df.loc[a], df.loc[b]
        edge = enrich_edge(ra, rb, scores[(a, b)], field_rules)
        edge["id1"]         = str(ra.get(id_col, a))
        edge["id2"]         = str(rb.get(id_col, b))
        edge["date"]        = date_partition
        edge["source_file"] = job_suffix
        if edge["vetoed"]:
            vetoed += 1
        rows.append(edge)

    log.info(f"[Build] {vetoed}/{len(rows)} edges vetoed by hard rules")

    edges_df = pd.DataFrame(rows)
    front    = ["id1", "id2", "date", "source_file", "deberta_score",
                "vetoed", "veto_reason"]
    rest     = [c for c in edges_df.columns if c not in front]
    edges_df = edges_df[front + rest]

    # 6. Node ID manifest (embeddings shelved with GNN)
    node_df = build_node_features(df, cols)

    # 7. Upload — namespaced by entity_type/job_suffix (no date in path)
    edges_path = f"{output_prefix}/{entity_type}/{job_suffix}/enriched_edges.csv"
    nodes_path = f"{output_prefix}/{entity_type}/{job_suffix}/node_features.csv"
    upload_csv_to_gcs(edges_df, edges_path)
    upload_csv_to_gcs(node_df,  nodes_path)

    log.info(
        f"[Build] Done — {len(edges_df)} edges ({vetoed} vetoed), "
        f"{len(node_df)} nodes"
    )
    return edges_path, nodes_path


# =============================================================================
# Main — three-tier dispatch: explicit path → to-process scan → training fallback
# =============================================================================

def main():
    config        = load_config(DEFAULT_CONFIG_PATH)
    entity_type   = os.environ.get("ENTITY_TYPE", config["data"]["entity_types"][0])
    date          = os.environ.get("DATE_PARTITION")
    output_prefix = f"gs://{config['data']['gcs_bucket']}/graph"

    model_resource_name = resolve_model_resource_name(config)

    # — BigQuery incremental mode ————————————————————————————————————————————
    if INPUT_MODE == "bigquery":
        load_records_bigquery(config)   # raises NotImplementedError until implemented
        return

    # — CSV mode — build (records_path, job_suffix) job list ——————————————————
    if RECORDS_GCS_PATH:
        # Tier 1: explicit path — customer file or manual run
        jobs = [(RECORDS_GCS_PATH, derive_job_suffix(RECORDS_GCS_PATH))]
    else:
        # Tier 2: scan to-process/ — upload flow
        scanned = scan_process_bucket(config)
        if scanned:
            jobs = [(p, derive_job_suffix(p)) for p in scanned]
        else:
            # Tier 3: training fallback — extract records from train + val pairs
            gcs_base    = (
                f"gs://{config['data']['gcs_bucket']}/"
                f"{config['data']['gcs_path']}"
            )
            pairs_paths = [f"{gcs_base}/val.csv",]
            records_path = extract_records_from_pairs(pairs_paths, config)
            jobs         = [(records_path, "train")]

    # — Process each job ——————————————————————————————————————————————————————
    for records_path, suffix in jobs:
        log.info(f"[Main] Processing: {records_path}  (suffix={suffix})")

        edges_path, nodes_path = build_graph(
            records_path        = records_path,
            output_prefix       = output_prefix,
            model_resource_name = model_resource_name,
            config              = config,
            entity_type         = entity_type,
            date_partition      = date,
            job_suffix          = suffix,
        )

        if edges_path:
            log.info(f"[Main] Edges → {edges_path}")
            log.info(f"[Main] Nodes → {nodes_path}")
            # Source file NOT deleted here.
            # write_clusters.py deletes to-process/ file after full pipeline succeeds.
        else:
            log.warning(f"[Main] No output for {records_path} — skipping")


if __name__ == "__main__":
    main()
