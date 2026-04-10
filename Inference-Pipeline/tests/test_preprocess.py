# -*- coding: utf-8 -*-
"""test_preprocess.py"""
import sys, os
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
os.environ["GCP_PROJECT_ID"] = "entity-resolution-487121"
os.environ["GCP_BUCKET_NAME"] = "entity-resolution-bucket-1"

from scripts.preprocess import MISSING_SENTINEL, InferencePreprocessor, normalize_address, normalize_name

def test_name_lowercase():
    assert normalize_name("ROBERT SMITH") == "robert smith"

def test_name_extra_whitespace():
    assert normalize_name("Robert  Smith") == "robert smith"

def test_name_strips_prefix_dr():
    assert normalize_name("Dr. Jane Doe") == "jane doe"

def test_name_strips_prefix_mr():
    assert normalize_name("Mr John Adams") == "john adams"

def test_name_strips_suffix_jr():
    assert normalize_name("John Smith Jr") == "john smith"

def test_name_none_returns_sentinel():
    assert normalize_name(None) == MISSING_SENTINEL

def test_name_empty_returns_sentinel():
    assert normalize_name("") == MISSING_SENTINEL

def test_address_lowercase():
    assert normalize_address("123 MAIN ST") == "123 main st"

def test_address_street_expansion():
    assert normalize_address("123 Main Street") == "123 main st"

def test_address_avenue_expansion():
    assert normalize_address("456 Elm Avenue") == "456 elm ave"

def test_address_removes_comma():
    assert "," not in normalize_address("123 Main St, Apt 4B")

def test_address_apartment_normalized():
    assert "apt" in normalize_address("123 Main St Apartment 4B")

def test_address_none_returns_sentinel():
    assert normalize_address(None) == MISSING_SENTINEL

def test_address_empty_returns_sentinel():
    assert normalize_address("") == MISSING_SENTINEL

def test_prepare_pair_returns_all_keys():
    p = InferencePreprocessor()
    result = p.prepare_pair("Robert Smith", "123 Main St", "Bob Smith", "123 Main Street")
    assert all(k in result for k in ["name1", "address1", "name2", "address2"])

def test_prepare_pair_normalizes_name():
    p = InferencePreprocessor()
    result = p.prepare_pair("ROBERT SMITH", "123 Main St", "bob smith", "123 Main St")
    assert result["name1"] == "robert smith"

def test_prepare_pair_normalizes_address():
    p = InferencePreprocessor()
    result = p.prepare_pair("A", "123 Main Street", "B", "456 Elm Avenue")
    assert result["address1"] == "123 main st"

def test_prepare_pair_none_fields():
    p = InferencePreprocessor()
    assert all(v == MISSING_SENTINEL for v in p.prepare_pair(None, None, None, None).values())

def test_prepare_batch_length():
    p = InferencePreprocessor()
    assert len(p.prepare_batch([{"name1": "A", "address1": "1", "name2": "A", "address2": "1"}, {"name1": "B", "address1": "2", "name2": "B", "address2": "2"}])) == 2

def test_prepare_batch_missing_keys():
    p = InferencePreprocessor()
    assert p.prepare_batch([{"name1": "Alice", "name2": "Alice"}])[0]["address1"] == MISSING_SENTINEL

def test_validate_pair_no_warnings():
    assert InferencePreprocessor().validate_pair("Alice", "1 Main St", "Bob", "2 Ave") == []

def test_validate_pair_warns_empty_name():
    assert any("name1" in w for w in InferencePreprocessor().validate_pair("", "1 St", "Bob", "2 Ave"))

def test_validate_pair_warns_empty_address():
    assert any("address1" in w for w in InferencePreprocessor().validate_pair("Alice", "", "Bob", "2 Ave"))

def test_output_lowercase():
    p = InferencePreprocessor()
    result = p.prepare_pair("DR. ROBERT SMITH JR", "123 Main Street", "Bob Smith", "123 MAIN ST")
    assert all(v == v.lower() for v in result.values())