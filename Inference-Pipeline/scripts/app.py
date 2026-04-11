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
app = FastAPI(
    title="Entity Resolution API",
    description="deBERTa + LoRA model serving for PERSON entity resolution",
    version="1.0.0",
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


# ------------------------------------------------------------------
# Pydantic schemas
# ------------------------------------------------------------------
class EntityPair(BaseModel):
    name1: str = Field(..., example="Robert Smith")
    address1: str = Field(..., example="123 Main Street")
    name2: str = Field(..., example="Bob Smith")
    address2: str = Field(..., example="126 Main St")


class BatchRequest(BaseModel):
    pairs: List[EntityPair] = Field(..., max_items=1000)


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
# POST /resolve  — single pair
# ------------------------------------------------------------------
@app.post("/resolve", response_model=ResolutionResponse)
async def resolve(pair: EntityPair):
    """
    Resolve a single entity pair.
    Returns match probability, decision (MATCH/NO-MATCH/REVIEW),
    confidence level, and per-field similarity breakdown.
    """
    t0 = time.monotonic()

    # Validate input quality
    warnings = _preprocessor.validate_pair(pair.name1, pair.address1, pair.name2, pair.address2)

    # Preprocess
    processed = _preprocessor.prepare_pair(pair.name1, pair.address1, pair.name2, pair.address2)

    # Predict
    try:
        results: List[PredictionResult] = _client.predict([processed])
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=503, detail=f"Model inference failed: {str(e)}")

    result = results[0]

    # Update stats
    _update_stats(result, time.monotonic() - t0)

    return ResolutionResponse(
        probability=result.probability,
        decision=result.decision,
        confidence_level=result.confidence_level,
        field_similarities=result.field_similarities,
        latency_ms=result.latency_ms,
        warnings=warnings,
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
        raise HTTPException(status_code=503, detail=f"Model inference failed: {str(e)}")

    total_ms = (time.monotonic() - t0) * 1000

    # Build response + summary
    responses = []
    decision_counts = defaultdict(int)
    confidence_counts = defaultdict(int)

    for pair, pred in zip(request.pairs, predictions):
        warnings = _preprocessor.validate_pair(pair.name1, pair.address1, pair.name2, pair.address2)
        responses.append(
            ResolutionResponse(
                probability=pred.probability,
                decision=pred.decision,
                confidence_level=pred.confidence_level,
                field_similarities=pred.field_similarities,
                latency_ms=pred.latency_ms,
                warnings=warnings,
            )
        )
        decision_counts[pred.decision] += 1
        confidence_counts[pred.confidence_level] += 1
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
            "error": str(e),
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GCS upload failed: {e}")

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

    return {"job_id": job_id, "status": "queued", "gcs_path": gcs_path}


# ------------------------------------------------------------------
# GET /unify/status/{job_id}  — poll job progress
# ------------------------------------------------------------------
@app.get("/unify/status/{job_id}")
async def unify_status(job_id: str):
    """Return current status of a unify pipeline job."""
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_store[job_id]


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
