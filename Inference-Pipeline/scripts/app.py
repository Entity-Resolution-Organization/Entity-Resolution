# -*- coding: utf-8 -*-
"""
app.py
======
FastAPI backend for the Entity Resolution Inference Pipeline.

Endpoints:
  POST /resolve          - Single pair resolution
  POST /resolve/batch    - Batch resolution (up to 1000 pairs)
  POST /search           - Entity search against analytics dataset
  POST /unify/upload     - Upload CSV for full graph pipeline (cluster assignment)
  GET  /unify/status/{id} - Poll job status
  GET  /unify/download/{id} - Download unified CSV with cluster_ids
  GET  /health           - API + endpoint connectivity status
  GET  /metrics/pipeline - Pipeline quality gate + bias + model metrics
  GET  /metrics/inference - Runtime inference stats

Runs on port 8000.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import List

import yaml
from fastapi import BackgroundTasks, FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from scripts.model_client import PredictionResult, get_client
from scripts.pipeline_runner import create_job_id, job_store, run_unify_pipeline
from scripts.preprocess import InferencePreprocessor

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).parent.parent / "config" / "inference_config.yaml"


def _load_config() -> dict:
    with open(_CONFIG_PATH) as f:
        raw = f.read()
    for key, val in os.environ.items():
        raw = raw.replace(f"${{{key}}}", val)
    return yaml.safe_load(raw)


CFG = _load_config()

# ------------------------------------------------------------------
# App init
# ------------------------------------------------------------------
_is_production = os.environ.get("PRODUCTION", "").lower() in ("1", "true", "yes")

app = FastAPI(
    title="Entity Resolution API",
    description="deBERTa + LoRA model serving for PERSON entity resolution",
    version="1.0.0",
    docs_url=None if _is_production else "/docs",
    redoc_url=None if _is_production else "/redoc",
    openapi_url=None if _is_production else "/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CFG["api"]["cors_origins"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------
# Shared state (loaded once at startup)
# ------------------------------------------------------------------
_client = None
_preprocessor = InferencePreprocessor()
_inference_stats = defaultdict(float)
_inference_stats["request_count"] = 0
_inference_stats["total_latency_ms"] = 0.0
_inference_stats["match_count"] = 0
_inference_stats["no_match_count"] = 0
_inference_stats["review_count"] = 0


@app.on_event("startup")
async def startup():
    global _client
    logger.info("Loading model client...")
    _client = get_client()
    logger.info("Model client ready.")

    # Pre-cache demo data for interactive pages (360, KYC, Fraud, Clusters)
    try:
        from scripts.demo_cache import load_demo_data
        bucket = CFG.get("gcp", {}).get("bucket_name", "")
        if bucket:
            load_demo_data(bucket)
    except Exception as e:
        logger.warning(f"Demo cache load failed (non-fatal): {e}")


# ------------------------------------------------------------------
# Pydantic schemas
# ------------------------------------------------------------------
class EntityPair(BaseModel):
    name1: str = Field(..., example="Robert Smith", max_length=500)
    address1: str = Field(..., example="123 Main Street", max_length=500)
    name2: str = Field(..., example="Bob Smith", max_length=500)
    address2: str = Field(..., example="126 Main St", max_length=500)
    dob1: str = Field("", example="1985-03-15", max_length=20)
    dob2: str = Field("", example="1985-03-15", max_length=20)
    email1: str = Field("", example="rsmith@gmail.com", max_length=254)
    email2: str = Field("", example="bob.smith@gmail.com", max_length=254)
    phone1: str = Field("", example="617-555-0142", max_length=30)
    phone2: str = Field("", example="617-555-0142", max_length=30)


class BatchRequest(BaseModel):
    pairs: List[EntityPair] = Field(..., max_length=25)


class SearchRequest(BaseModel):
    name: str = Field(..., example="Robert Smith")
    address: str = Field("", example="123 Main Street")
    top_k: int = Field(10, ge=1, le=50)


class ResolutionResponse(BaseModel):
    probability: float
    decision: str
    confidence_level: str
    field_similarities: dict
    latency_ms: float
    warnings: List[str] = []
    flag: str = ""
    deberta_raw: float = 0.0
    adjustments: List[str] = []


class BatchResponse(BaseModel):
    results: List[ResolutionResponse]
    summary: dict


class SearchResult(BaseModel):
    rank: int
    name: str
    address: str
    source_dataset: str
    match_score: float
    decision: str


class HealthResponse(BaseModel):
    status: str
    model_client: str
    endpoint_reachable: bool
    version: str = "1.0.0"


# ------------------------------------------------------------------
# Field-level booster/veto logic
# ------------------------------------------------------------------
def _apply_field_rules(deberta_score: float, pair: EntityPair) -> tuple:
    """
    Apply field-level boosters and vetoes on top of the DeBERTa score.

    Returns: (adjusted_score, flag, adjustments_list)
    """
    import re

    score = deberta_score
    flag = ""
    adjustments = []

    def _norm(s: str) -> str:
        return re.sub(r"[-/\\.\s]", "", s.strip().lower()) if s else ""

    def _is_plausible_dob(s: str) -> bool:
        """Return True only if s looks like an actual date (YYYY-MM-DD, MM/DD/YYYY, etc.)."""
        s = s.strip()
        if not s:
            return False
        # Must contain at least some digits to be a date
        if not re.search(r"\d{4}", s):
            return False
        # Quick format check: contains digits + separators only
        return bool(re.match(r"^[\d\-/. ]+$", s))

    # --- DOB ---
    dob1_raw, dob2_raw = pair.dob1.strip(), pair.dob2.strip()
    dob1_valid = _is_plausible_dob(dob1_raw)
    dob2_valid = _is_plausible_dob(dob2_raw)
    if dob1_valid and dob2_valid:
        dob1_norm, dob2_norm = _norm(dob1_raw), _norm(dob2_raw)
        if dob1_norm == dob2_norm:
            score = min(1.0, score + 0.15)
            adjustments.append("dob match: +0.15")
        else:
            score = min(score, 0.10)
            adjustments.append("dob mismatch: capped at 0.10")

    # --- Email ---
    email1, email2 = pair.email1.strip().lower(), pair.email2.strip().lower()
    has_email = bool(email1) and bool(email2)
    if has_email:
        if email1 == email2:
            if deberta_score < 0.45:
                flag = "ambiguous"
                adjustments.append("same email + low DeBERTa: flagged ambiguous")
            else:
                score = min(1.0, score + 0.20)
                adjustments.append("email exact match: +0.20")
        else:
            # Check username match (different domain)
            u1 = email1.split("@")[0] if "@" in email1 else email1
            u2 = email2.split("@")[0] if "@" in email2 else email2
            if u1 == u2:
                score = min(1.0, score + 0.10)
                adjustments.append("email username match: +0.10")

    # --- Phone ---
    phone1 = re.sub(r"\D", "", pair.phone1)
    phone2 = re.sub(r"\D", "", pair.phone2)
    has_phone = bool(phone1) and bool(phone2)
    if has_phone and phone1[-9:] == phone2[-9:]:
        score = min(1.0, score + 0.10)
        adjustments.append("phone match: +0.10")

    # Recompute decision based on adjusted score
    if flag == "ambiguous":
        decision = "REVIEW"
    elif score >= 0.45:
        decision = "MATCH"
    elif score <= 0.20:
        decision = "NO-MATCH"
    else:
        decision = "REVIEW"

    # Confidence level
    if score >= 0.80:
        confidence = "HIGH"
    elif score >= 0.50:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return score, decision, confidence, flag, adjustments


# ------------------------------------------------------------------
# POST /resolve  — single pair
# ------------------------------------------------------------------
@app.post("/resolve", response_model=ResolutionResponse)
async def resolve(pair: EntityPair):
    """
    Resolve a single entity pair.
    Returns match probability, decision (MATCH/NO-MATCH/REVIEW),
    confidence level, per-field similarity breakdown, and field rule adjustments.
    """
    t0 = time.monotonic()

    # Reject completely empty inputs — model returns high match on blanks
    side1_empty = not pair.name1.strip() and not pair.address1.strip()
    side2_empty = not pair.name2.strip() and not pair.address2.strip()
    if side1_empty or side2_empty:
        raise HTTPException(
            status_code=422,
            detail="Both name and address are empty on one or both sides. Provide at least a name or address for each entity.",
        )

    # Validate input quality
    warnings = _preprocessor.validate_pair(pair.name1, pair.address1, pair.name2, pair.address2)

    # Preprocess
    processed = _preprocessor.prepare_pair(pair.name1, pair.address1, pair.name2, pair.address2)

    # Predict via DeBERTa
    try:
        results: List[PredictionResult] = _client.predict([processed])
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=503, detail="Model is temporarily unavailable. Please try again in a moment.")

    result = results[0]
    deberta_raw = result.probability

    # Apply field-level boosters and vetoes
    adj_score, decision, confidence, flag, adjustments = _apply_field_rules(deberta_raw, pair)

    # Update stats
    _update_stats(result, time.monotonic() - t0)

    return ResolutionResponse(
        probability=round(adj_score, 6),
        decision=decision,
        confidence_level=confidence,
        field_similarities={k: round(v, 4) for k, v in result.field_similarities.items()},
        latency_ms=round(result.latency_ms, 1),
        warnings=warnings,
        flag=flag,
        deberta_raw=round(deberta_raw, 6),
        adjustments=adjustments,
    )


# ------------------------------------------------------------------
# POST /resolve/batch  — batch resolution
# ------------------------------------------------------------------
@app.post("/resolve/batch", response_model=BatchResponse)
async def resolve_batch(request: BatchRequest):
    """
    Resolve a batch of entity pairs (up to 1000).
    Returns results for each pair plus summary statistics.
    """
    if not request.pairs:
        raise HTTPException(status_code=400, detail="No pairs provided")

    t0 = time.monotonic()

    # Preprocess all pairs
    processed = _preprocessor.prepare_batch([p.dict() for p in request.pairs])

    # Predict
    try:
        predictions: List[PredictionResult] = _client.predict(processed)
    except Exception as e:
        logger.error(f"Batch prediction failed: {e}")
        raise HTTPException(status_code=503, detail="Model is temporarily unavailable. Please try again in a moment.")

    total_ms = (time.monotonic() - t0) * 1000

    # Build response + summary
    responses = []
    decision_counts = defaultdict(int)
    confidence_counts = defaultdict(int)

    for pair, pred in zip(request.pairs, predictions):
        warnings = _preprocessor.validate_pair(pair.name1, pair.address1, pair.name2, pair.address2)
        deberta_raw = pred.probability
        adj_score, decision, confidence, flag, adjustments = _apply_field_rules(deberta_raw, pair)
        responses.append(
            ResolutionResponse(
                probability=round(adj_score, 6),
                decision=decision,
                confidence_level=confidence,
                field_similarities={k: round(v, 4) for k, v in pred.field_similarities.items()},
                latency_ms=round(pred.latency_ms, 1),
                warnings=warnings,
                flag=flag,
                deberta_raw=round(deberta_raw, 6),
                adjustments=adjustments,
            )
        )
        decision_counts[decision] += 1
        confidence_counts[confidence] += 1
        _update_stats(pred, 0)

    total = len(predictions)
    summary = {
        "total_pairs": total,
        "match_count": decision_counts["MATCH"],
        "no_match_count": decision_counts["NO-MATCH"],
        "review_count": decision_counts["REVIEW"],
        "match_rate": round(decision_counts["MATCH"] / total, 4) if total else 0,
        "confidence_distribution": dict(confidence_counts),
        "total_latency_ms": round(total_ms, 2),
        "avg_latency_ms": round(total_ms / total, 2) if total else 0,
    }

    return BatchResponse(results=responses, summary=summary)


# ------------------------------------------------------------------
# POST /search  — entity search
# ------------------------------------------------------------------
@app.post("/search")
async def search(request: SearchRequest):
    """
    Search for matching entities in the analytics dataset.
    Scores query record against all records and returns top-K matches.
    """
    # Build pairs: query vs each candidate
    query_name = request.name
    query_address = request.address

    # Load a sample of the analytics dataset for search
    # In production this would use a vector index / BigQuery
    candidates = _get_search_candidates(query_name, query_address, top_k=request.top_k * 3)

    if not candidates:
        return {"results": [], "message": "Analytics dataset not available"}

    # Score all candidates
    pairs = [
        {
            "name1": query_name,
            "address1": query_address,
            "name2": c["name"],
            "address2": c.get("address", ""),
        }
        for c in candidates
    ]

    processed = _preprocessor.prepare_batch(pairs)

    try:
        predictions = _client.predict(processed)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

    # Rank by probability
    scored = sorted(
        zip(candidates, predictions),
        key=lambda x: x[1].probability,
        reverse=True,
    )

    results = []
    for rank, (candidate, pred) in enumerate(scored[: request.top_k], start=1):
        results.append(
            SearchResult(
                rank=rank,
                name=candidate["name"],
                address=candidate.get("address", ""),
                source_dataset=candidate.get("source", "unknown"),
                match_score=round(pred.probability, 4),
                decision=pred.decision,
            ).dict()
        )

    return {"results": results, "query": {"name": query_name, "address": query_address}}


# ------------------------------------------------------------------
# GET /health
# ------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse)
async def health():
    """
    Returns API health status and model endpoint connectivity.
    """
    client_type = type(_client).__name__ if _client else "not initialized"
    endpoint_ok = _client is not None

    return HealthResponse(
        status="ok" if endpoint_ok else "degraded",
        model_client=client_type,
        endpoint_reachable=endpoint_ok,
    )


# ------------------------------------------------------------------
# GET /metrics/pipeline
# ------------------------------------------------------------------
@app.get("/metrics/pipeline")
async def metrics_pipeline():
    """
    Returns quality gate results, bias report, and model metrics from GCS.
    Powers the Pipeline Dashboard tab in the Streamlit UI.
    """
    try:
        from google.cloud import storage as gcs

        bucket_name = CFG["gcp"]["bucket_name"]
        project_id = CFG["gcp"]["project_id"]
        metrics_dir = CFG["gcs_paths"]["metrics_dir"]

        client = gcs.Client(project=project_id)
        bucket = client.bucket(bucket_name)

        def _read_json(path):
            try:
                return json.loads(bucket.blob(path).download_as_text())
            except Exception:
                return {}

        quality_gate = _read_json(f"{metrics_dir}quality_gate.json")
        bias_report = _read_json(f"{metrics_dir}bias_report.json")
        model_metrics = _read_json("pipeline-results/models/person/results/test_metrics.json")

        return {
            "quality_gate": quality_gate,
            "bias_report": bias_report,
            "model_metrics": model_metrics,
        }

    except Exception as e:
        logger.warning(f"Could not load pipeline metrics from GCS: {e}")
        return {
            "quality_gate": {"status": "unavailable"},
            "bias_report": {"status": "unavailable"},
            "model_metrics": {"status": "unavailable"},
        }


# ------------------------------------------------------------------
# GET /metrics/inference
# ------------------------------------------------------------------
@app.get("/metrics/inference")
async def metrics_inference():
    """
    Returns runtime inference statistics collected since startup.
    """
    count = _inference_stats["request_count"]
    return {
        "request_count": int(count),
        "avg_latency_ms": round(_inference_stats["total_latency_ms"] / count if count else 0, 2),
        "match_rate": round(_inference_stats["match_count"] / count if count else 0, 4),
        "decision_distribution": {
            "MATCH": int(_inference_stats["match_count"]),
            "NO-MATCH": int(_inference_stats["no_match_count"]),
            "REVIEW": int(_inference_stats["review_count"]),
        },
    }


# ------------------------------------------------------------------
# POST /unify/upload  — upload CSV for graph pipeline
# ------------------------------------------------------------------
_TRAINING_CONFIG_PATH = Path(__file__).parent.parent.parent / "Model-Pipeline" / "config" / "training_config.yaml"


def _load_training_config() -> dict:
    path = os.environ.get("CONFIG_PATH", str(_TRAINING_CONFIG_PATH))
    with open(path) as f:
        return yaml.safe_load(f)


_MAX_CSV_SIZE = 10 * 1024 * 1024  # 10 MB
_MAX_CSV_ROWS = 5000
_REQUIRED_COLUMNS = {"id", "name", "address"}


@app.post("/unify/upload")
async def unify_upload(file: UploadFile, background_tasks: BackgroundTasks):
    """
    Upload a records CSV. Triggers the full pipeline as a background task:
    build_graph -> write_clusters -> (optional) score_network.
    Returns a job_id to poll for status.
    """
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files accepted")

    # Read file content
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    if len(content) > _MAX_CSV_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"File too large ({len(content) / 1024 / 1024:.1f} MB). Maximum is {_MAX_CSV_SIZE // 1024 // 1024} MB.",
        )

    # Validate CSV structure before uploading to GCS
    import csv
    from io import StringIO

    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            try:
                text = content.decode("latin-1")
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail="Could not read file. Ensure it is a UTF-8 encoded CSV.",
                )

    try:
        reader = csv.reader(StringIO(text))
        rows = list(reader)
    except csv.Error:
        raise HTTPException(status_code=400, detail="File is not valid CSV.")

    if len(rows) < 2:
        raise HTTPException(
            status_code=400,
            detail="CSV must contain a header row and at least one data row.",
        )

    headers = {h.strip().lower() for h in rows[0]}
    missing = _REQUIRED_COLUMNS - headers
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"CSV is missing required columns: {', '.join(sorted(missing))}. Expected: id, name, address.",
        )

    data_rows = len(rows) - 1
    if data_rows < 2:
        raise HTTPException(
            status_code=400,
            detail=f"CSV has only {data_rows} data row(s). At least 2 records are needed to form candidate pairs.",
        )

    if data_rows > _MAX_CSV_ROWS:
        raise HTTPException(
            status_code=400,
            detail=f"CSV has {data_rows:,} rows. Maximum is {_MAX_CSV_ROWS:,} for this demo.",
        )

    # Re-encode as UTF-8 in case we decoded from latin-1
    content = text.encode("utf-8")

    # Upload to GCS to-process/
    try:
        from google.cloud import storage as gcs

        training_cfg = _load_training_config()
        bucket_name = training_cfg["data"]["gcs_bucket"]
        blob_name = f"to-process/{file.filename}"
        gcs_path = f"gs://{bucket_name}/{blob_name}"

        client = gcs.Client()
        client.bucket(bucket_name).blob(blob_name).upload_from_string(
            content, content_type="text/csv"
        )
        logger.info(f"[Unify] Uploaded {len(content)} bytes -> {gcs_path}")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to upload file. Please try again.")

    # Resolve model resource name
    from scripts.build_graph import resolve_model_resource_name
    model_resource_name = resolve_model_resource_name(training_cfg)

    # Create job and launch background pipeline
    job_id = create_job_id()
    background_tasks.add_task(
        run_unify_pipeline,
        job_id=job_id,
        records_gcs_path=gcs_path,
        config=training_cfg,
        model_resource_name=model_resource_name,
        run_scoring=True,
    )

    return {"job_id": job_id, "status": "queued", "records": data_rows}


# ------------------------------------------------------------------
# GET /unify/status/{job_id}  — poll job progress
# ------------------------------------------------------------------
@app.get("/unify/status/{job_id}")
async def unify_status(job_id: str):
    """Return current status of a unify pipeline job."""
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")
    job = job_store[job_id]

    # Sanitize — strip internal paths, clean error messages
    safe = {
        "job_id": job["job_id"],
        "status": job["status"],
        "stage": job["stage"],
        "error": _sanitize_error(job.get("error")),
        "job_suffix": job.get("job_suffix", ""),
        "stats": job.get("stats", {}),
        "created_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
    }
    return safe


def _sanitize_error(err: str | None) -> str | None:
    """Strip internal details from pipeline errors."""
    if not err:
        return None
    lower = err.lower()
    if "codec" in lower or "decode" in lower:
        return "File encoding error. Please save as UTF-8 CSV."
    if "503" in err or "service unavailable" in lower:
        return "Model is temporarily unavailable. Please try again."
    if "timeout" in lower:
        return "Pipeline timed out. Try a smaller CSV."
    if "no candidate pairs" in lower:
        return "No matching candidate pairs found. Check that records have name and address fields."
    return err


# ------------------------------------------------------------------
# GET /unify/download/{job_id}  — download unified CSV
# ------------------------------------------------------------------
@app.get("/unify/download/{job_id}")
async def unify_download(job_id: str):
    """Download the unified CSV with cluster assignments."""
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")

    job = job_store[job_id]
    if job["status"] != "complete":
        raise HTTPException(status_code=409, detail=f"Job not complete: {job['status']}")

    unified_path = job.get("unified_gcs_path")
    if not unified_path:
        raise HTTPException(status_code=404, detail="No unified CSV available")

    try:
        from io import BytesIO

        from google.cloud import storage as gcs

        bucket_name, blob_name = unified_path.replace("gs://", "").split("/", 1)
        content = gcs.Client().bucket(bucket_name).blob(blob_name).download_as_bytes()

        return StreamingResponse(
            BytesIO(content),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={job['job_suffix']}_unified.csv"
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {e}")


# ------------------------------------------------------------------
# GET /unify/jobs  — list all jobs
# ------------------------------------------------------------------
@app.get("/unify/jobs")
async def unify_jobs():
    """List all pipeline jobs with their status."""
    return {
        "jobs": [
            {
                "job_id": j["job_id"],
                "status": j["status"],
                "stage": j["stage"],
                "job_suffix": j["job_suffix"],
                "created_at": j["created_at"],
                "completed_at": j.get("completed_at"),
                "stats": j.get("stats", {}),
            }
            for j in job_store.values()
        ]
    }


# ------------------------------------------------------------------
# Customer 360 endpoints
# ------------------------------------------------------------------
@app.get("/360/search")
async def search_360(name: str = "", limit: int = 10):
    """Search entities by name, returns matching clusters."""
    from scripts.demo_cache import is_loaded, search_entities
    if not is_loaded():
        raise HTTPException(status_code=503, detail="Demo data not loaded yet")
    results = search_entities(name, limit)
    return {"results": results, "query": name}


@app.get("/360/cluster/{cluster_id}")
async def cluster_profile(cluster_id: str):
    """Full cluster profile: golden record + source records + edges."""
    from scripts.demo_cache import is_loaded, get_cluster_profile
    if not is_loaded():
        raise HTTPException(status_code=503, detail="Demo data not loaded yet")
    profile = get_cluster_profile(cluster_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return profile


# ------------------------------------------------------------------
# KYC endpoints
# ------------------------------------------------------------------
@app.get("/kyc/alerts")
async def kyc_alerts():
    """List KYC risk alerts (OFAC linkages)."""
    from scripts.demo_cache import is_loaded, get_kyc_alerts
    if not is_loaded():
        raise HTTPException(status_code=503, detail="Demo data not loaded yet")
    return {"alerts": get_kyc_alerts()}


@app.get("/kyc/investigate/{record_id}")
async def kyc_investigate(record_id: str):
    """2-hop investigation graph for a flagged entity."""
    from scripts.demo_cache import is_loaded, get_kyc_investigation
    if not is_loaded():
        raise HTTPException(status_code=503, detail="Demo data not loaded yet")
    result = get_kyc_investigation(record_id)
    if not result:
        raise HTTPException(status_code=404, detail="No KYC alert for this record")
    return result


# ------------------------------------------------------------------
# Fraud detection endpoints
# ------------------------------------------------------------------
@app.get("/fraud/rings")
async def fraud_rings():
    """Detected fraud rings (cross-cluster shared fields)."""
    from scripts.demo_cache import is_loaded, get_fraud_rings
    if not is_loaded():
        raise HTTPException(status_code=503, detail="Demo data not loaded yet")
    return {"rings": get_fraud_rings()}


# ------------------------------------------------------------------
# Clusters endpoints
# ------------------------------------------------------------------
@app.get("/clusters")
async def list_clusters():
    """List all non-singleton clusters."""
    from scripts.demo_cache import is_loaded, get_all_clusters
    if not is_loaded():
        raise HTTPException(status_code=503, detail="Demo data not loaded yet")
    return {"clusters": get_all_clusters()}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _update_stats(result: PredictionResult, elapsed: float):
    _inference_stats["request_count"] += 1
    _inference_stats["total_latency_ms"] += result.latency_ms
    if result.decision == "MATCH":
        _inference_stats["match_count"] += 1
    elif result.decision == "NO-MATCH":
        _inference_stats["no_match_count"] += 1
    else:
        _inference_stats["review_count"] += 1


def _get_search_candidates(name: str, address: str, top_k: int = 30) -> list:
    """
    Load candidate records from analytics dataset for entity search.
    Returns a sample of records to score against.
    """
    try:
        import csv
        import io

        from google.cloud import storage as gcs

        bucket_name = CFG["gcp"]["bucket_name"]
        project_id = CFG["gcp"]["project_id"]
        analytics_path = CFG["gcs_paths"]["analytics_data"]

        client = gcs.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        content = bucket.blob(analytics_path).download_as_text()

        reader = csv.DictReader(io.StringIO(content))
        records = []
        for i, row in enumerate(reader):
            if i >= top_k * 3:
                break
            record_name = row.get("name", row.get("full_name", ""))
            record_addr = row.get("address", row.get("street_address", ""))
            source = row.get("source", row.get("dataset", "unknown"))
            if record_name:
                records.append(
                    {
                        "name": record_name,
                        "address": record_addr,
                        "source": source,
                    }
                )
        return records

    except Exception as e:
        logger.warning(f"Could not load analytics dataset: {e}")
        return []


# ------------------------------------------------------------------
# Serve React frontend (production builds only)
# ------------------------------------------------------------------
_FRONTEND_DIR = Path(__file__).parent.parent / "frontend" / "dist"
if _FRONTEND_DIR.exists():
    from starlette.staticfiles import StaticFiles
    from starlette.responses import FileResponse

    app.mount("/assets", StaticFiles(directory=str(_FRONTEND_DIR / "assets")), name="assets")

    @app.get("/{path:path}")
    async def serve_spa(path: str):
        """Serve React SPA — all non-API routes fall through to index.html."""
        file_path = _FRONTEND_DIR / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_FRONTEND_DIR / "index.html"))


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "scripts.app:app",
        host=CFG["api"]["host"],
        port=CFG["api"]["port"],
        reload=True,
    )
