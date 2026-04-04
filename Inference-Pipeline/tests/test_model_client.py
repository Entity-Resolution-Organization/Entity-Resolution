"""
test_model_client.py
====================
Tests for MockClient and shared helpers.
VertexAIClient is tested via integration tests (requires GCP creds).
"""
import os
import sys
import pytest

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
os.environ["USE_MOCK_CLIENT"] = "true"
os.environ["GCP_PROJECT_ID"] = "entity-resolution-487121"
os.environ["GCP_BUCKET_NAME"] = "entity-resolution-bucket-1"

from scripts.model_client import (
    MockClient,
    PredictionResult,
    _compute_field_similarities,
    _make_decision,
    _token_overlap,
    get_client,
)

# Load config once
from scripts.model_client import _load_config
CFG = _load_config()

# ------------------------------------------------------------------
# _token_overlap
# ------------------------------------------------------------------
def test_token_overlap_identical():
    assert _token_overlap("123 main street", "123 main street") == 1.0

def test_token_overlap_empty():
    assert _token_overlap("", "") == 1.0

def test_token_overlap_no_match():
    assert _token_overlap("alpha beta", "gamma delta") == 0.0

def test_token_overlap_partial():
    score = _token_overlap("123 main street", "123 main avenue")
    assert 0.0 < score < 1.0

# ------------------------------------------------------------------
# _make_decision
# ------------------------------------------------------------------
def test_decision_match():
    decision, _ = _make_decision(0.9, CFG)
    assert decision == "MATCH"

def test_decision_no_match():
    decision, _ = _make_decision(0.1, CFG)
    assert decision == "NO-MATCH"

def test_decision_review():
    decision, _ = _make_decision(0.30, CFG)
    assert decision == "REVIEW"

def test_confidence_high():
    _, confidence = _make_decision(0.95, CFG)
    assert confidence == "HIGH"

def test_confidence_low():
    _, confidence = _make_decision(0.55, CFG)
    assert confidence == "LOW"

# ------------------------------------------------------------------
# _compute_field_similarities
# ------------------------------------------------------------------
def test_field_similarities_keys():
    pair = {"name1": "John Smith", "address1": "123 Main St",
            "name2": "Jon Smith",  "address2": "123 Main Street"}
    sims = _compute_field_similarities(pair)
    assert "name_jaro_winkler" in sims
    assert "name_levenshtein_ratio" in sims
    assert "address_token_overlap" in sims
    assert "address_jaro_winkler" in sims

def test_field_similarities_identical_names():
    pair = {"name1": "Jane Doe", "address1": "", "name2": "Jane Doe", "address2": ""}
    sims = _compute_field_similarities(pair)
    assert sims["name_jaro_winkler"] == 1.0

def test_field_similarities_missing_fields():
    # Should not raise even with empty input
    pair = {}
    sims = _compute_field_similarities(pair)
    assert isinstance(sims, dict)

# ------------------------------------------------------------------
# MockClient
# ------------------------------------------------------------------
def test_mock_client_returns_results():
    client = MockClient(CFG)
    pairs = [{"name1": "Robert Smith", "address1": "123 Main St",
               "name2": "Bob Smith",   "address2": "125 Main St"}]
    results = client.predict(pairs)
    assert len(results) == 1
    assert isinstance(results[0], PredictionResult)

def test_mock_client_exact_match():
    client = MockClient(CFG)
    pairs = [{"name1": "Alice Johnson", "address1": "10 Elm Street",
               "name2": "Alice Johnson", "address2": "10 Elm Street"}]
    results = client.predict(pairs)
    assert results[0].probability > 0.8
    assert results[0].decision == "MATCH"

def test_mock_client_clear_no_match():
    client = MockClient(CFG)
    pairs = [{"name1": "Zara Smith",  "address1": "1 Alpha Road",
               "name2": "John Brown", "address2": "99 Omega Lane"}]
    results = client.predict(pairs)
    assert results[0].decision in ("NO-MATCH", "REVIEW")

def test_mock_client_batch():
    client = MockClient(CFG)
    pairs = [
        {"name1": "A", "address1": "X", "name2": "A", "address2": "X"},
        {"name1": "B", "address1": "Y", "name2": "C", "address2": "Z"},
    ]
    results = client.predict(pairs)
    assert len(results) == 2

def test_mock_client_result_has_field_similarities():
    client = MockClient(CFG)
    pairs = [{"name1": "Tom", "address1": "1 St", "name2": "Tim", "address2": "2 St"}]
    result = client.predict(pairs)[0]
    assert "name_jaro_winkler" in result.field_similarities

def test_mock_client_to_dict():
    client = MockClient(CFG)
    pairs = [{"name1": "Sam", "address1": "5 Rd", "name2": "Sam", "address2": "5 Rd"}]
    result = client.predict(pairs)[0]
    d = result.to_dict()
    assert all(k in d for k in ["probability", "decision", "confidence_level",
                                  "field_similarities", "latency_ms"])

# ------------------------------------------------------------------
# get_client factory
# ------------------------------------------------------------------
def test_get_client_returns_mock_when_env_set():
    os.environ["USE_MOCK_CLIENT"] = "true"
    client = get_client()
    assert isinstance(client, MockClient)

def test_get_client_mock_overrides_config():
    os.environ["USE_MOCK_CLIENT"] = "true"
    client = get_client()
    assert isinstance(client, MockClient)
