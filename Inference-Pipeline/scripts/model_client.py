# -*- coding: utf-8 -*-
"""
model_client.py
===============
Abstraction layer between the inference API and the model backend.

Two clients share the same interface:
  - VertexAIClient  : calls the live Vertex AI endpoint
  - MockClient      : Jaro-Winkler + token overlap (dev / demo fallback)

Usage:
    client = get_client()
    results = client.predict(pairs)   # List[dict] -> List[PredictionResult]

Factory function respects (in priority order):
  1. USE_MOCK_CLIENT env var  ("true" / "false")
  2. inference_config.yaml    mock.use_mock field
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import yaml

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Config loader (cached at module level)
# ------------------------------------------------------------------
_CONFIG: Optional[dict] = None
_CONFIG_PATH = Path(__file__).parent.parent / "config" / "inference_config.yaml"


def _load_config() -> dict:
    global _CONFIG
    if _CONFIG is None:
        with open(_CONFIG_PATH) as f:
            raw = f.read()
        for key, val in os.environ.items():
            raw = raw.replace(f"${{{key}}}", val)
        _CONFIG = yaml.safe_load(raw)
    return _CONFIG


# ------------------------------------------------------------------
# Shared result dataclass
# ------------------------------------------------------------------
@dataclass
class PredictionResult:
    """Unified result returned by both clients."""
    probability: float        # match probability 0.0 - 1.0
    decision: str             # "MATCH" | "NO-MATCH" | "REVIEW"
    confidence_level: str     # "HIGH" | "MEDIUM" | "LOW"
    field_similarities: dict  # per-field breakdown for explainability
    latency_ms: float         # round-trip time

    def to_dict(self) -> dict:
        return {
            "probability": round(self.probability, 4),
            "decision": self.decision,
            "confidence_level": self.confidence_level,
            "field_similarities": self.field_similarities,
            "latency_ms": round(self.latency_ms, 2),
        }


def _make_decision(probability: float, cfg: dict) -> tuple:
    """Convert raw probability into (decision, confidence_level) using config thresholds."""
    t = cfg["thresholds"]
    match_threshold = t["match_threshold"]    # 0.45
    no_match_ceiling = t["no_match_ceiling"]  # 0.20
    high_min = t["confidence"]["high_min"]    # 0.80
    medium_min = t["confidence"]["medium_min"] # 0.50

    if probability >= match_threshold:
        decision = "MATCH"
    elif probability <= no_match_ceiling:
        decision = "NO-MATCH"
    else:
        decision = "REVIEW"

    if probability >= high_min or probability <= (1 - high_min):
        confidence = "HIGH"
    elif probability >= medium_min or probability <= (1 - medium_min):
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return decision, confidence


# ------------------------------------------------------------------
# VertexAIClient
# ------------------------------------------------------------------
class VertexAIClient:
    """
    Calls the live Vertex AI endpoint.

    Reads endpoint URL from:
        gs://{bucket}/pipeline-results/endpoint_info.json
    Written by deploy_to_endpoint_op in the Model-Pipeline.
    """

    def __init__(self, cfg: dict):
        self._cfg = cfg
        self._predict_url: Optional[str] = None
        self._credentials = None
        self._load_endpoint_info()

    def _load_endpoint_info(self) -> None:
        """Read endpoint_info.json from GCS and cache the predict URL."""
        from google.cloud import storage
        bucket_name = self._cfg["gcp"]["bucket_name"]
        gcs_path = "pipeline-results/endpoint_info.json"

        logger.info(f"Loading endpoint info from gs://{bucket_name}/{gcs_path}")
        try:
            gcs_client = storage.Client(project=self._cfg["gcp"]["project_id"])
            bucket = gcs_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            info = json.loads(blob.download_as_text())
            self._predict_url = info["predict_url"]
            logger.info(f"Vertex AI endpoint ready: {self._predict_url}")
        except Exception as e:
            logger.error(f"Failed to load endpoint info: {e}")
            raise RuntimeError(
                f"Could not load endpoint_info.json from GCS: {e}\n"
                "Set USE_MOCK_CLIENT=true to use the mock client instead."
            )

    def _get_token(self) -> str:
        """Get a fresh Google auth token."""
        from google.auth import default as google_default
        from google.auth.transport.requests import Request
        if self._credentials is None:
            self._credentials, _ = google_default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
        self._credentials.refresh(Request())
        return self._credentials.token

    def predict(self, pairs: List[dict]) -> List[PredictionResult]:
        """
        Call Vertex AI endpoint with a list of entity pairs.

        Args:
            pairs: List of dicts with keys: name1, address1, name2, address2

        Returns:
            List of PredictionResult (one per input pair)
        """
        import urllib.request

        cfg = self._cfg
        max_batch = cfg["vertex_ai"]["max_batch_size"]
        timeout = cfg["vertex_ai"]["prediction_timeout_seconds"]
        results = []

        for i in range(0, len(pairs), max_batch):
            batch = pairs[i: i + max_batch]
            payload = json.dumps({"instances": batch}).encode("utf-8")
            token = self._get_token()

            req = urllib.request.Request(
                self._predict_url,
                data=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )

            t0 = time.monotonic()
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    response_body = json.loads(resp.read())
                latency_ms = (time.monotonic() - t0) * 1000

                predictions = response_body.get("predictions", [])
                for pair, pred in zip(batch, predictions):
                    if isinstance(pred, dict):
                        probability = float(pred.get("probability", pred.get("score", 0.5)))
                    else:
                        probability = float(pred)

                    decision, confidence = _make_decision(probability, cfg)
                    field_sims = _compute_field_similarities(pair)
                    results.append(PredictionResult(
                        probability=probability,
                        decision=decision,
                        confidence_level=confidence,
                        field_similarities=field_sims,
                        latency_ms=latency_ms / len(batch),
                    ))

            except Exception as e:
                logger.error(f"Vertex AI prediction failed for batch {i}: {e}")
                raise

        return results


# ------------------------------------------------------------------
# MockClient
# ------------------------------------------------------------------
class MockClient:
    """
    Development / demo fallback. No GCP credentials required.
    Uses Jaro-Winkler similarity on names + token overlap on addresses.
    """

    def __init__(self, cfg: dict):
        self._cfg = cfg
        self._latency_ms = cfg["mock"]["mock_latency_ms"]
        logger.warning(
            "MockClient active - using Jaro-Winkler similarity. "
            "Set USE_MOCK_CLIENT=false to use the live Vertex AI endpoint."
        )

    def predict(self, pairs: List[dict]) -> List[PredictionResult]:
        results = []
        for pair in pairs:
            t0 = time.monotonic()
            time.sleep(self._latency_ms / 1000)
            probability = self._score_pair(pair)
            decision, confidence = _make_decision(probability, self._cfg)
            field_sims = _compute_field_similarities(pair)
            results.append(PredictionResult(
                probability=probability,
                decision=decision,
                confidence_level=confidence,
                field_similarities=field_sims,
                latency_ms=(time.monotonic() - t0) * 1000,
            ))
        return results

    def _score_pair(self, pair: dict) -> float:
        """70% name similarity + 30% address similarity."""
        import jellyfish
        name1 = str(pair.get("name1", "")).lower().strip()
        name2 = str(pair.get("name2", "")).lower().strip()
        addr1 = str(pair.get("address1", "")).lower().strip()
        addr2 = str(pair.get("address2", "")).lower().strip()

        name_sim = jellyfish.jaro_winkler_similarity(name1, name2) if name1 and name2 else 0.0
        addr_sim = _token_overlap(addr1, addr2)
        return round(0.70 * name_sim + 0.30 * addr_sim, 4)


# ------------------------------------------------------------------
# Shared helpers
# ------------------------------------------------------------------
def _token_overlap(a: str, b: str) -> float:
    """Jaccard similarity on whitespace-split tokens."""
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if not tokens_a and not tokens_b:
        return 1.0
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def _compute_field_similarities(pair: dict) -> dict:
    """Per-field similarity breakdown for explainability."""
    import jellyfish
    name1 = str(pair.get("name1", "")).lower().strip()
    name2 = str(pair.get("name2", "")).lower().strip()
    addr1 = str(pair.get("address1", "")).lower().strip()
    addr2 = str(pair.get("address2", "")).lower().strip()

    return {
        "name_jaro_winkler": round(
            jellyfish.jaro_winkler_similarity(name1, name2) if name1 and name2 else 0.0, 4
        ),
        "name_levenshtein_ratio": round(
            1 - jellyfish.levenshtein_distance(name1, name2) / max(len(name1), len(name2), 1), 4
        ),
        "address_token_overlap": round(_token_overlap(addr1, addr2), 4),
        "address_jaro_winkler": round(
            jellyfish.jaro_winkler_similarity(addr1, addr2) if addr1 and addr2 else 0.0, 4
        ),
    }


# ------------------------------------------------------------------
# Factory function  (public API)
# ------------------------------------------------------------------
def get_client():
    """
    Return the appropriate client based on config + environment.

    Priority:
      1. USE_MOCK_CLIENT env var  ("true" overrides everything)
      2. inference_config.yaml    mock.use_mock field
    """
    cfg = _load_config()
    env_override = os.environ.get("USE_MOCK_CLIENT", "").lower()

    if env_override == "true":
        use_mock = True
    elif env_override == "false":
        use_mock = False
    else:
        use_mock = cfg["mock"]["use_mock"]

    return MockClient(cfg) if use_mock else VertexAIClient(cfg)
