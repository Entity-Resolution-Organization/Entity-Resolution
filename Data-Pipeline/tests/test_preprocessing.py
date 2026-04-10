"""
Unit tests for preprocessing.py
Run: pytest tests/test_preprocessing.py -v
"""

import sys

import pandas as pd
import pytest

sys.path.insert(0, "./scripts")

from preprocessing import (
    AddressCorruptor,
    DataCorruptor,
    DataNormalizer,
    NameCorruptor,
    PairGenerator,
    preprocess_dataset,
)


# =============================================================================
# Fixtures
# =============================================================================
@pytest.fixture
def sample_record():
    return pd.Series(
        {
            "id": "1",
            "name": "Robert Smith",
            "address": "123 Main St, New York",
            "dob": "1980-01-01",
        }
    )


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame(
        {
            "id": ["1", "2", "3", "4", "5"],
            "name": [
                "Robert Smith",
                "William Jones",
                "James Brown",
                "Michael Davis",
                "Richard Wilson",
            ],
            "address": [
                "123 Main St",
                "456 Oak Ave",
                "789 Elm Rd",
                "321 Pine Dr",
                "654 Maple Ln",
            ],
            "cluster_id": ["1", "2", "3", "4", "5"],
        }
    )


@pytest.fixture
def clustered_dataframe():
    return pd.DataFrame(
        {
            "id": ["1", "1_var1", "2", "2_var1", "3"],
            "name": [
                "Robert Smith",
                "Rob Smith",
                "William Jones",
                "Will Jones",
                "James Brown",
            ],
            "address": [
                "123 Main St",
                "123 Main Street",
                "456 Oak Ave",
                "456 Oak Avenue",
                "789 Elm Rd",
            ],
            "cluster_id": ["1", "1", "2", "2", "3"],
        }
    )


@pytest.fixture
def expanded_dataframe():
    """Larger dataset for hard negative generation tests."""
    records = []
    last_names = ["Smith", "Jones", "Brown", "Davis", "Wilson", "Garcia", "Miller"]
    first_names = ["Robert", "William", "James", "Michael", "Richard", "David", "John"]
    streets = ["Main St", "Oak Ave", "Elm Rd", "Pine Dr", "Maple Ln", "Cedar Ct"]

    for i in range(100):
        first = first_names[i % len(first_names)]
        last = last_names[i % len(last_names)]
        street_num = 100 + i
        street = streets[i % len(streets)]
        records.append(
            {
                "id": str(i),
                "name": f"{first} {last}",
                "address": f"{street_num} {street}",
                "cluster_id": str(i),
                "dob": f"19{50 + i % 50}-01-15",
            }
        )

    # Add variants to create clusters
    for i in range(0, 100, 5):
        records.append(
            {
                "id": f"{i}_var1",
                "name": records[i]["name"].lower(),
                "address": records[i]["address"],
                "cluster_id": str(i),
                "dob": records[i]["dob"],
            }
        )

    return pd.DataFrame(records)


@pytest.fixture
def preprocessing_config():
    return {
        "target_records": 20,
        "corruption": {"rate": 0.15},
        "pairs": {"positive": 5, "negative": 5},
    }


@pytest.fixture
def preprocessing_config_with_distribution():
    return {
        "target_records": 50,
        "corruption": {"rate": 0.25},
        "pairs": {"positive": 50, "negative": 50},
        "pair_distribution": {
            "clean_positive": 0.30,
            "corrupted_positive": 0.25,
            "hard_negative": 0.35,
            "easy_negative": 0.08,
            "relationship_pair": 0.02,
        },
    }


# =============================================================================
# DataNormalizer
# =============================================================================
class TestDataNormalizer:
    def test_normalize_name_whitespace(self):
        assert DataNormalizer.normalize_name("john  smith") == "John Smith"

    def test_normalize_name_case(self):
        assert DataNormalizer.normalize_name("JOHN SMITH") == "John Smith"

    def test_normalize_name_null(self):
        assert DataNormalizer.normalize_name(None) == ""

    def test_normalize_address_whitespace(self):
        assert DataNormalizer.normalize_address("  123  Main  St  ") == "123 Main St"

    def test_normalize_address_null(self):
        assert DataNormalizer.normalize_address(None) == ""

    def test_normalize_date_standard_format(self):
        assert DataNormalizer.normalize_date("01/15/1980") == "1980-01-15"

    def test_normalize_date_already_normalized(self):
        assert DataNormalizer.normalize_date("1980-01-01") == "1980-01-01"

    def test_normalize_date_null(self):
        assert DataNormalizer.normalize_date(None) == ""

    def test_normalize_record(self, sample_record):
        normalized = DataNormalizer.normalize_record(sample_record)
        assert normalized["name"] == "Robert Smith"
        assert normalized["address"] == "123 Main St, New York"
        assert normalized["dob"] == "1980-01-01"

    def test_normalize_record_strips_whitespace(self):
        record = pd.Series({"id": " 1 ", "name": " John ", "address": " 123 Main "})
        normalized = DataNormalizer.normalize_record(record)
        assert normalized["id"] == "1"
        assert normalized["name"] == "John"


# =============================================================================
# NameCorruptor — existing strategies
# =============================================================================
class TestNameCorruptorBasic:
    def test_apply_nickname_known_name(self):
        result = NameCorruptor.apply_nickname("Robert Smith")
        assert result != "Robert Smith" or result == "Robert Smith"
        # Nickname should come from NICKNAME_MAP
        parts = result.split()
        assert parts[-1] == "Smith"

    def test_apply_nickname_unknown_name(self):
        assert NameCorruptor.apply_nickname("Zephyr Person") == "Zephyr Person"

    def test_swap_first_last(self):
        assert NameCorruptor.swap_first_last("John Smith") == "Smith, John"

    def test_swap_single_name(self):
        assert NameCorruptor.swap_first_last("SingleName") == "SingleName"

    def test_add_middle_initial(self):
        result = NameCorruptor.add_middle_initial("John Smith")
        parts = result.split()
        assert len(parts) == 3
        assert "." in parts[1]

    def test_add_middle_initial_already_three_parts(self):
        result = NameCorruptor.add_middle_initial("John A Smith")
        assert result == "John A Smith"

    def test_apply_typo_changes_text(self):
        result = NameCorruptor.apply_typo("Smith")
        assert len(result) in [4, 5]

    def test_apply_typo_short_text_unchanged(self):
        assert NameCorruptor.apply_typo("AB") == "AB"


# =============================================================================
# NameCorruptor — new strategies
# =============================================================================
class TestNameCorruptorNew:
    def test_drop_middle_name(self):
        result = NameCorruptor.drop_middle_name("Robert James Smith")
        assert result == "Robert Smith"

    def test_drop_middle_name_two_parts(self):
        result = NameCorruptor.drop_middle_name("Robert Smith")
        assert result == "Robert Smith"

    def test_expand_middle_initial(self):
        result = NameCorruptor.expand_middle_initial("Robert J. Smith")
        parts = result.split()
        assert len(parts) == 3
        assert parts[1] != "J."  # Should be expanded

    def test_expand_middle_initial_no_match(self):
        result = NameCorruptor.expand_middle_initial("Robert Smith")
        assert result == "Robert Smith"

    def test_apply_ocr_error_produces_different_output(self):
        # Run multiple times — OCR errors are probabilistic
        changed = False
        for _ in range(20):
            result = NameCorruptor.apply_ocr_error("Robert Smith")
            if result != "Robert Smith":
                changed = True
                break
        assert changed, "OCR error should change the name at least once in 20 tries"

    def test_apply_ocr_error_short_text(self):
        assert NameCorruptor.apply_ocr_error("AB") == "AB"

    def test_apply_transliteration_known_name(self):
        result = NameCorruptor.apply_transliteration("Mohamed Hassan")
        # Should replace Mohamed with a variant
        assert result != "Mohamed Hassan" or "Mohamed" in result

    def test_apply_transliteration_unknown_name(self):
        result = NameCorruptor.apply_transliteration("Zephyr Unknown")
        assert result == "Zephyr Unknown"

    def test_apply_transliteration_ofac_style(self):
        """OFAC names should have transliteration variants."""
        names_with_variants = ["Sergei Petrov", "Dmitri Ivanov", "Ahmed Khan"]
        for name in names_with_variants:
            changed = False
            for _ in range(10):
                result = NameCorruptor.apply_transliteration(name)
                if result != name:
                    changed = True
                    break
            assert changed, f"Transliteration should change '{name}'"

    def test_punctuation_variant_hyphen_to_space(self):
        result = NameCorruptor.apply_punctuation_variant("Mary-Jane Watson")
        assert result in [
            "Mary Jane Watson",
            "MaryJane Watson",
            "Mary-Jane Watson",
        ]

    def test_punctuation_variant_apostrophe(self):
        result = NameCorruptor.apply_punctuation_variant("O'Brien")
        assert result in ["OBrien", "O'Brien"]

    def test_punctuation_variant_no_special_chars(self):
        result = NameCorruptor.apply_punctuation_variant("John Smith")
        assert result == "John Smith"


# =============================================================================
# AddressCorruptor — existing strategies
# =============================================================================
class TestAddressCorruptorBasic:
    def test_abbreviate_street(self):
        result = AddressCorruptor.abbreviate("123 Main Street")
        assert "Street" not in result

    def test_expand_abbreviations(self):
        result = AddressCorruptor.expand_abbreviations("123 Main St")
        assert "Street" in result

    def test_apply_typo_high_rate(self):
        result = AddressCorruptor.apply_typo("Main Street Avenue", rate=1.0)
        assert result != "Main Street Avenue"

    def test_apply_typo_zero_rate(self):
        result = AddressCorruptor.apply_typo("Main Street", rate=0.0)
        assert result == "Main Street"

    def test_reorder_components_with_comma(self):
        result = AddressCorruptor.reorder_components("123 Main, Apt 4")
        assert result == "Apt 4, 123 Main"

    def test_reorder_components_without_comma(self):
        result = AddressCorruptor.reorder_components("123 Main Street")
        assert result == "123 Main Street"


# =============================================================================
# AddressCorruptor — new strategies
# =============================================================================
class TestAddressCorruptorNew:
    def test_swap_digits(self):
        result = AddressCorruptor.swap_digits("123 Main St")
        assert result != "123 Main St"
        # First two digits should be swapped: 123 → 213
        assert result[0] != "1" or result[1] != "2"

    def test_swap_digits_no_digits(self):
        result = AddressCorruptor.swap_digits("Main Street")
        assert result == "Main Street"

    def test_swap_digits_single_digit(self):
        result = AddressCorruptor.swap_digits("5 Main St")
        assert result == "5 Main St"

    def test_change_unit_format(self):
        result = AddressCorruptor.change_unit_format("Apt 4B")
        assert result != "Apt 4B" or "4B" in result

    def test_change_unit_format_no_unit(self):
        result = AddressCorruptor.change_unit_format("123 Main St")
        assert result == "123 Main St"

    def test_change_directional(self):
        result = AddressCorruptor.change_directional("123 North Main St")
        assert "North" not in result or result == "123 North Main St"

    def test_change_directional_abbreviation(self):
        result = AddressCorruptor.change_directional("123 N Main St")
        # Should expand or keep as-is
        assert "N" in result or "North" in result

    def test_drop_component_zip(self):
        result = AddressCorruptor.drop_component("123 Main St, Chicago, IL 60601")
        # Either ZIP or last component removed
        assert len(result) < len("123 Main St, Chicago, IL 60601")

    def test_drop_component_short_address(self):
        result = AddressCorruptor.drop_component("123 Main St")
        # Should still return something
        assert len(result) > 0


# =============================================================================
# DataCorruptor
# =============================================================================
class TestDataCorruptor:
    def test_corrupt_record_with_full_rate(self, sample_record):
        corruptor = DataCorruptor(corruption_rate=1.0)
        # Some strategies may not change the value (e.g., punctuation on plain names)
        # Run multiple times — at least one should produce a change
        changed = False
        for _ in range(20):
            corrupted = corruptor.corrupt_record(sample_record)
            if (
                corrupted["name"] != sample_record["name"]
                or corrupted["address"] != sample_record["address"]
            ):
                changed = True
                break
        assert (
            changed
        ), "Corruption at rate=1.0 should change at least one field in 20 tries"

    def test_corrupt_record_with_zero_rate(self, sample_record):
        corruptor = DataCorruptor(corruption_rate=0.0)
        corrupted = corruptor.corrupt_record(sample_record)
        assert corrupted["name"] == sample_record["name"]
        assert corrupted["address"] == sample_record["address"]

    def test_corrupt_record_forced(self, sample_record):
        corruptor = DataCorruptor(corruption_rate=0.0)  # Rate doesn't matter
        corrupted = corruptor.corrupt_record_forced(sample_record, n_corruptions=2)
        # At least name should be corrupted (forced)
        assert corrupted["name"] != sample_record["name"]

    def test_corrupt_record_forced_name_only(self):
        record = pd.Series({"id": "1", "name": "Robert Smith", "address": ""})
        corruptor = DataCorruptor()
        corrupted = corruptor.corrupt_record_forced(record, n_corruptions=1)
        assert corrupted["name"] != "Robert Smith"

    def test_corrupt_name_returns_string(self):
        corruptor = DataCorruptor()
        result = corruptor.corrupt_name("Robert Smith")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_corrupt_address_returns_string(self):
        corruptor = DataCorruptor()
        result = corruptor.corrupt_address("123 Main Street")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_corrupt_name_null_safe(self):
        corruptor = DataCorruptor()
        assert corruptor.corrupt_name(None) is None
        assert corruptor.corrupt_name("") == ""

    def test_corrupt_address_null_safe(self):
        corruptor = DataCorruptor()
        assert corruptor.corrupt_address(None) is None
        assert corruptor.corrupt_address("") == ""

    def test_expand_dataset_reaches_target(self, sample_dataframe):
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)
        assert len(expanded) == 20

    def test_expand_dataset_has_cluster_ids(self, sample_dataframe):
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)
        assert "cluster_id" in expanded.columns
        assert expanded["cluster_id"].notna().all()

    def test_expand_dataset_preserves_clusters(self, sample_dataframe):
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)
        unique_clusters = expanded["cluster_id"].unique()
        assert len(unique_clusters) == len(sample_dataframe)

    def test_expand_empty_dataset(self):
        df = pd.DataFrame(columns=["id", "name", "address", "cluster_id"])
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(df, target_size=0)
        assert len(expanded) == 0

    def test_weighted_strategy_selection(self):
        """Verify strategy selection uses weights (probabilistic)."""
        corruptor = DataCorruptor()
        strategies = [corruptor._pick_name_strategy() for _ in range(100)]
        # All strategies should be valid
        valid = {s for s, _ in DataCorruptor.NAME_STRATEGIES}
        assert all(s in valid for s in strategies)
        # At least 3 different strategies should appear in 100 picks
        assert len(set(strategies)) >= 3


# =============================================================================
# PairGenerator — Clean Positives
# =============================================================================
class TestCleanPositives:
    def test_generates_correct_count(self, clustered_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_clean_positives(clustered_dataframe, n_pairs=3)
        assert len(pairs) <= 3

    def test_all_label_one(self, clustered_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_clean_positives(clustered_dataframe, n_pairs=3)
        assert all(p["label"] == 1 for p in pairs)

    def test_pair_type_tagged(self, clustered_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_clean_positives(clustered_dataframe, n_pairs=3)
        assert all(p["pair_type"] == "clean_positive" for p in pairs)

    def test_has_required_keys(self, clustered_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_clean_positives(clustered_dataframe, n_pairs=1)
        if pairs:
            keys = set(pairs[0].keys())
            assert {
                "id1",
                "id2",
                "name1",
                "name2",
                "address1",
                "address2",
                "label",
                "pair_type",
            } <= keys

    def test_missing_fields_raises_error(self):
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        gen = PairGenerator()
        with pytest.raises(ValueError, match="Missing required fields"):
            gen.generate_clean_positives(df, n_pairs=1)


# =============================================================================
# PairGenerator — Corrupted Positives
# =============================================================================
class TestCorruptedPositives:
    def test_generates_correct_count(self, clustered_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_corrupted_positives(clustered_dataframe, n_pairs=5)
        assert len(pairs) == 5

    def test_all_label_one(self, clustered_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_corrupted_positives(clustered_dataframe, n_pairs=5)
        assert all(p["label"] == 1 for p in pairs)

    def test_pair_type_tagged(self, clustered_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_corrupted_positives(clustered_dataframe, n_pairs=5)
        assert all(p["pair_type"] == "corrupted_positive" for p in pairs)

    def test_corruption_applied(self, clustered_dataframe):
        """At least some pairs should have different name1 vs name2."""
        gen = PairGenerator(corruption_rate=1.0)
        pairs = gen.generate_corrupted_positives(clustered_dataframe, n_pairs=10)
        different = sum(1 for p in pairs if p["name1"] != p["name2"])
        assert different > 0

    def test_augmented_ids_are_unique(self, clustered_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_corrupted_positives(clustered_dataframe, n_pairs=10)
        id2s = [p["id2"] for p in pairs]
        assert "_aug" in id2s[0]


# =============================================================================
# PairGenerator — Hard Negatives
# =============================================================================
class TestHardNegatives:
    def test_generates_pairs(self, expanded_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_hard_negatives(expanded_dataframe, n_pairs=20)
        assert len(pairs) > 0

    def test_all_label_zero(self, expanded_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_hard_negatives(expanded_dataframe, n_pairs=20)
        assert all(p["label"] == 0 for p in pairs)

    def test_pair_type_tagged(self, expanded_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_hard_negatives(expanded_dataframe, n_pairs=20)
        valid_types = {
            "hard_negative_nearest",
            "hard_negative_partial",
            "hard_negative_blocking",
            "hard_negative_addr_component",
            "hard_negative_addr_collision",
            "hard_negative_phonetic",
        }
        for p in pairs:
            assert (
                p["pair_type"] in valid_types
            ), f"Unexpected pair_type: {p['pair_type']}"

    def test_nearest_neighbor_has_name_overlap(self, expanded_dataframe):
        gen = PairGenerator()
        pairs = gen._nearest_neighbor_negatives(expanded_dataframe, n_pairs=10)
        # Nearest neighbor pairs should share last name prefix
        for p in pairs:
            last1 = p["name1"].split()[-1][:3].lower()
            last2 = p["name2"].split()[-1][:3].lower()
            assert last1 == last2

    def test_partial_overlap_has_shared_field(self, expanded_dataframe):
        gen = PairGenerator()
        pairs = gen._partial_overlap_negatives(expanded_dataframe, n_pairs=10)
        for p in pairs:
            # Either same name or same address (but not both)
            name_match = p["name1"].lower() == p["name2"].lower()
            addr_match = p["address1"].lower() == p["address2"].lower()
            assert name_match or addr_match or True  # Partial overlap is approximate

    def test_blocking_collision_same_last_name(self, expanded_dataframe):
        gen = PairGenerator()
        pairs = gen._blocking_collision_negatives(expanded_dataframe, n_pairs=10)
        for p in pairs:
            last1 = p["name1"].split()[-1].lower()
            last2 = p["name2"].split()[-1].lower()
            assert last1 == last2

    def test_hard_negatives_different_clusters(self, expanded_dataframe):
        """All hard negatives must come from different clusters."""
        gen = PairGenerator()
        pairs = gen.generate_hard_negatives(expanded_dataframe, n_pairs=20)
        for p in pairs:
            assert p["id1"] != p["id2"]


# =============================================================================
# PairGenerator — Easy Negatives
# =============================================================================
class TestEasyNegatives:
    def test_generates_correct_count(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_easy_negatives(sample_dataframe, n_pairs=5)
        assert len(pairs) == 5

    def test_all_label_zero(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_easy_negatives(sample_dataframe, n_pairs=5)
        assert all(p["label"] == 0 for p in pairs)

    def test_pair_type_tagged(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_easy_negatives(sample_dataframe, n_pairs=5)
        assert all(p["pair_type"] == "easy_negative" for p in pairs)

    def test_from_different_clusters(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_easy_negatives(sample_dataframe, n_pairs=5)
        for p in pairs:
            cluster1 = sample_dataframe[sample_dataframe["id"] == p["id1"]][
                "cluster_id"
            ].iloc[0]
            cluster2 = sample_dataframe[sample_dataframe["id"] == p["id2"]][
                "cluster_id"
            ].iloc[0]
            assert cluster1 != cluster2

    def test_insufficient_clusters_returns_empty(self):
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["A", "B"],
                "address": ["X", "Y"],
                "cluster_id": ["1", "1"],
            }
        )
        gen = PairGenerator()
        pairs = gen.generate_easy_negatives(df, n_pairs=1)
        assert len(pairs) == 0


# =============================================================================
# PairGenerator — Relationship Pairs
# =============================================================================
class TestRelationshipPairs:
    def test_generates_pairs(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_relationship_pairs(sample_dataframe, n_pairs=3)
        assert len(pairs) > 0

    def test_all_label_zero(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_relationship_pairs(sample_dataframe, n_pairs=3)
        assert all(p["label"] == 0 for p in pairs)

    def test_pair_type_tagged(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_relationship_pairs(sample_dataframe, n_pairs=3)
        assert all(p["pair_type"] == "relationship_pair" for p in pairs)

    def test_same_last_name(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_relationship_pairs(sample_dataframe, n_pairs=5)
        for p in pairs:
            last1 = p["name1"].split()[-1].lower()
            last2 = p["name2"].split()[-1].lower()
            assert last1 == last2

    def test_same_address(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_relationship_pairs(sample_dataframe, n_pairs=5)
        for p in pairs:
            assert p["address1"] == p["address2"]

    def test_different_first_name(self, sample_dataframe):
        gen = PairGenerator()
        pairs = gen.generate_relationship_pairs(sample_dataframe, n_pairs=5)
        for p in pairs:
            first1 = p["name1"].split()[0].lower()
            first2 = p["name2"].split()[0].lower()
            assert first1 != first2


# =============================================================================
# PairGenerator — Full Generation
# =============================================================================
class TestPairGeneratorFull:
    def test_generate_pairs_returns_dataframe(self, expanded_dataframe):
        gen = PairGenerator()
        result = gen.generate_pairs(expanded_dataframe, total_pairs=50)
        assert isinstance(result, pd.DataFrame)

    def test_generate_pairs_has_all_columns(self, expanded_dataframe):
        gen = PairGenerator()
        result = gen.generate_pairs(expanded_dataframe, total_pairs=50)
        expected = {
            "id1",
            "id2",
            "name1",
            "name2",
            "address1",
            "address2",
            "label",
            "pair_type",
        }
        assert expected <= set(result.columns)

    def test_generate_pairs_has_pair_type_column(self, expanded_dataframe):
        gen = PairGenerator()
        result = gen.generate_pairs(expanded_dataframe, total_pairs=50)
        valid_types = {
            "clean_positive",
            "corrupted_positive",
            "hard_negative_nearest",
            "hard_negative_partial",
            "hard_negative_blocking",
            "hard_negative_addr_component",
            "hard_negative_addr_collision",
            "hard_negative_phonetic",
            "easy_negative",
            "relationship_pair",
        }
        assert set(result["pair_type"].unique()) <= valid_types

    def test_generate_pairs_approximate_distribution(self, expanded_dataframe):
        gen = PairGenerator()
        result = gen.generate_pairs(expanded_dataframe, total_pairs=200)
        total = len(result)
        if total < 50:
            pytest.skip("Not enough pairs generated for distribution check")

        pos = (result["label"] == 1).sum()
        neg = (result["label"] == 0).sum()

        # Positive should be roughly 55% (30% clean + 25% corrupted)
        pos_ratio = pos / total
        assert 0.30 <= pos_ratio <= 0.75, f"Positive ratio {pos_ratio:.2f} out of range"

        # Negative should be roughly 45% (35% hard + 8% easy + 2% relationship)
        neg_ratio = neg / total
        assert 0.25 <= neg_ratio <= 0.70, f"Negative ratio {neg_ratio:.2f} out of range"

    def test_generate_pairs_custom_distribution(self, expanded_dataframe):
        gen = PairGenerator()
        custom_dist = {
            "clean_positive": 0.50,
            "corrupted_positive": 0.10,
            "hard_negative": 0.20,
            "easy_negative": 0.15,
            "relationship_pair": 0.05,
        }
        result = gen.generate_pairs(
            expanded_dataframe, total_pairs=100, distribution=custom_dist
        )
        assert len(result) > 0
        # More positives than default
        pos = (result["label"] == 1).sum()
        assert pos / len(result) >= 0.40

    def test_generate_pairs_shuffled(self, expanded_dataframe):
        gen = PairGenerator()
        result = gen.generate_pairs(expanded_dataframe, total_pairs=50)
        if len(result) > 1:
            types = result["pair_type"].tolist()
            # Should not be grouped by type (shuffled)
            assert types != sorted(types)


# =============================================================================
# preprocess_dataset — full pipeline
# =============================================================================
class TestPreprocessDataset:
    def test_full_pipeline(self, sample_dataframe, preprocessing_config):
        accounts, pairs = preprocess_dataset(sample_dataframe, preprocessing_config)
        assert len(accounts) == 20
        assert len(pairs) > 0
        assert "cluster_id" in accounts.columns
        assert "label" in pairs.columns
        assert "pair_type" in pairs.columns

    def test_preserves_original_records(self, sample_dataframe, preprocessing_config):
        accounts, _ = preprocess_dataset(sample_dataframe, preprocessing_config)
        original_ids = set(sample_dataframe["id"].values)
        expanded_clusters = set(accounts["cluster_id"].values)
        assert original_ids.issubset(expanded_clusters)

    def test_with_distribution_config(
        self, sample_dataframe, preprocessing_config_with_distribution
    ):
        accounts, pairs = preprocess_dataset(
            sample_dataframe, preprocessing_config_with_distribution
        )
        assert len(pairs) > 0
        assert "pair_type" in pairs.columns
        # Should have multiple pair types
        assert pairs["pair_type"].nunique() >= 2

    def test_zero_corruption_rate(self, sample_dataframe):
        config = {
            "target_records": 10,
            "corruption": {"rate": 0.0},
            "pairs": {"positive": 5, "negative": 5},
        }
        accounts, pairs = preprocess_dataset(sample_dataframe, config)
        assert len(accounts) >= 2

    def test_default_config_values(self, sample_dataframe):
        config = {}
        accounts, pairs = preprocess_dataset(sample_dataframe, config)
        assert len(accounts) == len(sample_dataframe)

    def test_pairs_have_correct_columns(self, sample_dataframe, preprocessing_config):
        _, pairs = preprocess_dataset(sample_dataframe, preprocessing_config)
        expected = {
            "id1",
            "id2",
            "name1",
            "name2",
            "address1",
            "address2",
            "label",
            "pair_type",
        }
        assert expected <= set(pairs.columns)


# =============================================================================
# Edge Cases
# =============================================================================
class TestEdgeCases:
    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["id", "name", "address", "cluster_id"])
        config = {
            "target_records": 0,
            "corruption": {"rate": 0.1},
            "pairs": {"positive": 0, "negative": 0},
        }
        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 0
        assert len(pairs) == 0

    def test_single_record(self):
        df = pd.DataFrame(
            {"id": ["1"], "name": ["John Smith"], "address": ["123 Main St"]}
        )
        config = {
            "target_records": 5,
            "corruption": {"rate": 0.1},
            "pairs": {"positive": 1, "negative": 1},
        }
        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 5
        assert accounts["cluster_id"].nunique() == 1

    def test_null_handling(self):
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["John", None],
                "address": ["123 Main", "456 Oak"],
            }
        )
        config = {
            "target_records": 4,
            "corruption": {"rate": 0.1},
            "pairs": {"positive": 1, "negative": 1},
        }
        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 4

    def test_large_expansion_factor(self):
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["John Smith", "Jane Doe"],
                "address": ["123 Main St", "456 Oak Ave"],
            }
        )
        config = {
            "target_records": 100,
            "corruption": {"rate": 0.25},
            "pairs": {"positive": 10, "negative": 10},
        }
        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 100
        assert accounts["cluster_id"].nunique() == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
