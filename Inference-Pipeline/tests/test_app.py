# -*- coding: utf-8 -*-
"""test_app.py"""
import os
import sys
from unittest.mock import MagicMock, patch

os.environ["USE_MOCK_CLIENT"] = "true"
os.environ["GCP_PROJECT_ID"] = "entity-resolution-487121"
os.environ["GCP_BUCKET_NAME"] = "entity-resolution-bucket-1"
os.environ["API_BASE_URL"] = "http://localhost:8000"
os.environ["MLFLOW_TRACKING_URI"] = ""

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

mock_storage = MagicMock()
mock_auth = MagicMock()
mock_auth.default.return_value = (MagicMock(token="fake", refresh=MagicMock()), "proj")
mock_requests_mod = MagicMock()
mock_requests_mod.Request = object

with patch.dict("sys.modules", {
    "google.cloud.storage": mock_storage,
    "google.auth": mock_auth,
    "google.auth.transport.requests": mock_requests_mod,
}):
    from fastapi.testclient import TestClient
    import scripts.app as app_module
    from scripts.app import app
    from scripts.model_client import MockClient, _load_config

# Directly inject mock client so startup event is not needed
CFG = _load_config()
app_module._client = MockClient(CFG)

client = TestClient(app)


def test_health_returns_200():
    assert client.get("/health").status_code == 200

def test_health_has_required_fields():
    data = client.get("/health").json()
    assert all(k in data for k in ["status", "model_client", "endpoint_reachable"])

def test_health_status_ok():
    assert client.get("/health").json()["status"] == "ok"

def test_health_client_is_mock():
    assert "Mock" in client.get("/health").json()["model_client"]

def test_resolve_returns_200():
    r = client.post("/resolve", json={
        "name1": "Robert Smith", "address1": "123 Main St",
        "name2": "Bob Smith", "address2": "125 Main St",
    })
    assert r.status_code == 200

def test_resolve_has_all_fields():
    data = client.post("/resolve", json={
        "name1": "Alice", "address1": "1 Elm St",
        "name2": "Alice", "address2": "1 Elm St",
    }).json()
    assert all(k in data for k in ["probability", "decision", "confidence_level",
                                    "field_similarities", "latency_ms", "warnings"])

def test_resolve_probability_in_range():
    prob = client.post("/resolve", json={
        "name1": "Alice", "address1": "1 St",
        "name2": "Alice", "address2": "1 St",
    }).json()["probability"]
    assert 0.0 <= prob <= 1.0

def test_resolve_decision_valid():
    decision = client.post("/resolve", json={
        "name1": "Alice", "address1": "1 St",
        "name2": "Bob", "address2": "9 Ave",
    }).json()["decision"]
    assert decision in ("MATCH", "NO-MATCH", "REVIEW")

def test_resolve_exact_match():
    decision = client.post("/resolve", json={
        "name1": "Alice Johnson", "address1": "10 Elm Street",
        "name2": "Alice Johnson", "address2": "10 Elm Street",
    }).json()["decision"]
    assert decision == "MATCH"

def test_resolve_field_similarities_keys():
    sims = client.post("/resolve", json={
        "name1": "Tom", "address1": "1 St",
        "name2": "Tim", "address2": "2 St",
    }).json()["field_similarities"]
    assert "name_jaro_winkler" in sims
    assert "address_token_overlap" in sims

def test_resolve_missing_body_returns_422():
    assert client.post("/resolve", json={}).status_code == 422

def test_batch_returns_200():
    r = client.post("/resolve/batch", json={"pairs": [
        {"name1": "Alice", "address1": "1 St", "name2": "Alice", "address2": "1 St"},
        {"name1": "Bob", "address1": "2 Ave", "name2": "Rob", "address2": "2 Ave"},
    ]})
    assert r.status_code == 200

def test_batch_result_count():
    r = client.post("/resolve/batch", json={"pairs": [
        {"name1": "A", "address1": "X", "name2": "A", "address2": "X"},
        {"name1": "B", "address1": "Y", "name2": "C", "address2": "Z"},
        {"name1": "D", "address1": "W", "name2": "D", "address2": "W"},
    ]})
    assert len(r.json()["results"]) == 3

def test_batch_summary_has_fields():
    r = client.post("/resolve/batch", json={"pairs": [
        {"name1": "Alice", "address1": "1 St", "name2": "Alice", "address2": "1 St"},
    ]})
    summary = r.json()["summary"]
    assert all(k in summary for k in ["total_pairs", "match_count", "match_rate", "total_latency_ms"])

def test_batch_empty_returns_400():
    assert client.post("/resolve/batch", json={"pairs": []}).status_code == 400

def test_inference_metrics_returns_200():
    assert client.get("/metrics/inference").status_code == 200

def test_inference_metrics_has_fields():
    data = client.get("/metrics/inference").json()
    assert all(k in data for k in ["request_count", "avg_latency_ms",
                                    "match_rate", "decision_distribution"])

def test_inference_metrics_increments():
    before = client.get("/metrics/inference").json()["request_count"]
    client.post("/resolve", json={
        "name1": "Alice", "address1": "1 St",
        "name2": "Alice", "address2": "1 St",
    })
    after = client.get("/metrics/inference").json()["request_count"]
    assert after > before

def test_pipeline_metrics_returns_200():
    assert client.get("/metrics/pipeline").status_code == 200

def test_pipeline_metrics_has_fields():
    data = client.get("/metrics/pipeline").json()
    assert all(k in data for k in ["quality_gate", "bias_report", "model_metrics"])
