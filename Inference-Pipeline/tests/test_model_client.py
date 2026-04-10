# -*- coding: utf-8 -*-
"""test_model_client.py"""
import os, sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
os.environ["USE_MOCK_CLIENT"] = "true"
os.environ["GCP_PROJECT_ID"] = "entity-resolution-487121"
os.environ["GCP_BUCKET_NAME"] = "entity-resolution-bucket-1"
os.environ["MLFLOW_TRACKING_URI"] = ""

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
    from scripts.model_client import (
        MockClient, PredictionResult, _compute_field_similarities,
        _make_decision, _token_overlap, get_client, _load_config,
    )

CFG = _load_config()

def test_token_overlap_identical():
    assert _token_overlap("123 main street", "123 main street") == 1.0

def test_token_overlap_empty():
    assert _token_overlap("", "") == 1.0

def test_token_overlap_no_match():
    assert _token_overlap("alpha beta", "gamma delta") == 0.0

def test_token_overlap_partial():
    assert 0.0 < _token_overlap("123 main street", "123 main avenue") < 1.0

def test_decision_match():
    assert _make_decision(0.9, CFG)[0] == "MATCH"

def test_decision_no_match():
    assert _make_decision(0.1, CFG)[0] == "NO-MATCH"

def test_decision_review():
    assert _make_decision(0.30, CFG)[0] == "REVIEW"

def test_confidence_high():
    assert _make_decision(0.95, CFG)[1] == "HIGH"

def test_field_similarities_keys():
    sims = _compute_field_similarities({"name1": "John", "address1": "1 St", "name2": "Jon", "address2": "2 Ave"})
    assert all(k in sims for k in ["name_jaro_winkler", "name_levenshtein_ratio", "address_token_overlap", "address_jaro_winkler"])

def test_field_similarities_missing_fields():
    assert isinstance(_compute_field_similarities({}), dict)

def test_mock_client_returns_results():
    client = MockClient(CFG)
    results = client.predict([{"name1": "Robert Smith", "address1": "123 Main St", "name2": "Bob Smith", "address2": "125 Main St"}])
    assert len(results) == 1
    assert isinstance(results[0], PredictionResult)

def test_mock_client_exact_match():
    client = MockClient(CFG)
    results = client.predict([{"name1": "Alice Johnson", "address1": "10 Elm Street", "name2": "Alice Johnson", "address2": "10 Elm Street"}])
    assert results[0].decision == "MATCH"

def test_mock_client_batch():
    client = MockClient(CFG)
    results = client.predict([{"name1": "A", "address1": "X", "name2": "A", "address2": "X"}, {"name1": "B", "address1": "Y", "name2": "C", "address2": "Z"}])
    assert len(results) == 2

def test_mock_client_to_dict():
    client = MockClient(CFG)
    d = client.predict([{"name1": "Sam", "address1": "5 Rd", "name2": "Sam", "address2": "5 Rd"}])[0].to_dict()
    assert all(k in d for k in ["probability", "decision", "confidence_level", "field_similarities", "latency_ms"])

def test_get_client_returns_mock():
    os.environ["USE_MOCK_CLIENT"] = "true"
    assert isinstance(get_client(), MockClient)