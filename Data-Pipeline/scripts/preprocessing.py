"""
Preprocessing Module for Entity Resolution Pipeline.

Handles four stages:
1. Normalization — clean and standardize raw PERSON records
2. Expansion — generate corrupted variants to reach target dataset size
3. Corruption — apply realistic corruption strategies for augmented positives
4. Pair Generation — create balanced training pairs with five pair types:
   - Clean positives (30%) — existing same-cluster pairs
   - Corrupted positives (25%) — augmented matches with OCR, transliteration, etc.
   - Hard negatives (35%) — high-overlap non-matches via nearest neighbor,
     partial overlap, and blocking collisions
   - Easy negatives (8%) — random non-matches
   - Relationship pairs (2%) — same surname + address, different people

Used by the DAG's transform_dataset task (Phase 3).
"""

import logging
import random
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
from corruption_maps import (
    ADDRESS_COMPONENTS,
    CITY_STATE_ZIP_POOLS,
    COMMON_MIDDLE_NAMES,
    DIRECTIONAL_MAP,
    NICKNAME_MAP,
    OCR_ERROR_MAP,
    PHONETIC_CONFUSION_PAIRS,
    STREET_ABBREVIATIONS,
    TRANSLITERATION_MAP,
    UNIT_FORMATS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RANDOM_SEED = 42


# =============================================================================
# NORMALIZATION
# =============================================================================
class DataNormalizer:
    """Normalize PERSON record values before corruption and matching."""

    @staticmethod
    def normalize_name(name: str) -> str:
        if pd.isna(name):
            return ""
        name = re.sub(r"\s+", " ", str(name).strip())
        name = name.title()
        return name.strip()

    @staticmethod
    def normalize_address(address: str) -> str:
        if pd.isna(address):
            return ""
        address = str(address).strip()
        address = re.sub(r"\s+", " ", address)
        address = address.title()
        return address.strip()

    @staticmethod
    def normalize_date(date_str: str) -> str:
        if pd.isna(date_str):
            return ""
        try:
            if isinstance(date_str, str):
                cleaned = re.sub(r"[^\d\-/]", "", str(date_str))
                dt = pd.to_datetime(cleaned)
                return dt.strftime("%Y-%m-%d")
            return str(date_str)
        except Exception as e:
            logger.warning(f"Could not parse date '{date_str}': {e}")
            return str(date_str)

    @staticmethod
    def normalize_record(record: pd.Series) -> pd.Series:
        normalized = record.copy()
        if "name" in normalized and pd.notna(normalized["name"]):
            normalized["name"] = DataNormalizer.normalize_name(normalized["name"])
        if "address" in normalized and pd.notna(normalized["address"]):
            normalized["address"] = DataNormalizer.normalize_address(
                normalized["address"]
            )
        if "dob" in normalized and pd.notna(normalized["dob"]):
            normalized["dob"] = DataNormalizer.normalize_date(normalized["dob"])
        for col in normalized.index:
            if isinstance(normalized[col], str):
                normalized[col] = normalized[col].strip()
        return normalized


# =============================================================================
# NAME CORRUPTION
# =============================================================================
class NameCorruptor:
    """Generate realistic name variants using multiple corruption strategies."""

    @staticmethod
    def apply_nickname(name: str) -> str:
        parts = name.split()
        if len(parts) > 0:
            first_name = parts[0].title()
            if first_name in NICKNAME_MAP:
                parts[0] = random.choice(NICKNAME_MAP[first_name])
        return " ".join(parts)

    @staticmethod
    def swap_first_last(name: str) -> str:
        parts = name.split()
        if len(parts) >= 2:
            return f"{parts[-1]}, {' '.join(parts[:-1])}"
        return name

    @staticmethod
    def add_middle_initial(name: str) -> str:
        parts = name.split()
        if len(parts) == 2:
            initial = chr(random.randint(65, 90))
            return f"{parts[0]} {initial}. {parts[1]}"
        return name

    @staticmethod
    def drop_middle_name(name: str) -> str:
        """Remove middle name/initial: 'Robert James Smith' → 'Robert Smith'."""
        parts = name.split()
        if len(parts) >= 3:
            return f"{parts[0]} {parts[-1]}"
        return name

    @staticmethod
    def expand_middle_initial(name: str) -> str:
        """Expand middle initial: 'Robert J. Smith' → 'Robert James Smith'."""
        parts = name.split()
        if len(parts) == 3:
            middle = parts[1].rstrip(".")
            if len(middle) == 1 and middle.upper() in COMMON_MIDDLE_NAMES:
                expansion = random.choice(COMMON_MIDDLE_NAMES[middle.upper()])
                return f"{parts[0]} {expansion} {parts[2]}"
        return name

    @staticmethod
    def apply_typo(text: str) -> str:
        if len(text) < 3:
            return text
        pos = random.randint(0, len(text) - 1)
        typo_type = random.choice(["delete", "substitute", "transpose"])
        if typo_type == "delete":
            return text[:pos] + text[pos + 1 :]
        elif typo_type == "substitute":
            if text[pos].isalpha():
                new_char = chr(random.randint(97, 122))
            elif text[pos].isdigit():
                new_char = str(random.randint(0, 9))
            else:
                return text
            return text[:pos] + new_char + text[pos + 1 :]
        else:
            if pos < len(text) - 1:
                chars = list(text)
                chars[pos], chars[pos + 1] = chars[pos + 1], chars[pos]
                return "".join(chars)
        return text

    @staticmethod
    def apply_ocr_error(name: str) -> str:
        """Introduce OCR-style character substitution."""
        if len(name) < 3:
            return name
        chars = list(name)
        # Apply 1-2 OCR substitutions
        n_errors = random.randint(1, min(2, len(chars)))
        for _ in range(n_errors):
            pos = random.randint(0, len(chars) - 1)
            char = chars[pos]
            if char in OCR_ERROR_MAP:
                replacement = random.choice(OCR_ERROR_MAP[char])
                chars[pos] = replacement
            # Check for multi-char patterns
            if pos < len(chars) - 1:
                bigram = chars[pos] + chars[pos + 1]
                if bigram in OCR_ERROR_MAP:
                    replacement = random.choice(OCR_ERROR_MAP[bigram])
                    chars[pos] = replacement
                    chars[pos + 1] = ""
        return "".join(chars)

    @staticmethod
    def apply_transliteration(name: str) -> str:
        """Replace name with a transliteration variant."""
        parts = name.split()
        for i, part in enumerate(parts):
            title_part = part.title()
            if title_part in TRANSLITERATION_MAP:
                parts[i] = random.choice(TRANSLITERATION_MAP[title_part])
                break  # Only transliterate one part per call
        return " ".join(parts)

    @staticmethod
    def apply_punctuation_variant(name: str) -> str:
        """Apply punctuation/formatting changes."""
        variant_type = random.choice(
            [
                "remove_hyphen",
                "remove_apostrophe",
                "join_hyphenated",
            ]
        )
        if variant_type == "remove_hyphen" and "-" in name:
            return name.replace("-", " ")
        elif variant_type == "join_hyphenated" and "-" in name:
            return name.replace("-", "")
        elif variant_type == "remove_apostrophe" and "'" in name:
            return name.replace("'", "")
        return name


# =============================================================================
# ADDRESS CORRUPTION
# =============================================================================
class AddressCorruptor:
    """Generate realistic address variants and corruptions."""

    @staticmethod
    def abbreviate(text: str) -> str:
        for full, abbrevs in STREET_ABBREVIATIONS.items():
            if full in text:
                text = text.replace(full, random.choice(abbrevs))
        return text

    @staticmethod
    def expand_abbreviations(text: str) -> str:
        for full, abbrevs in STREET_ABBREVIATIONS.items():
            for abbrev in abbrevs:
                pattern = r"\b" + re.escape(abbrev) + r"\b"
                text = re.sub(pattern, full, text, flags=re.IGNORECASE)
        return text

    @staticmethod
    def apply_typo(text: str, rate: float = 0.15) -> str:
        words = text.split()
        corrupted = []
        for word in words:
            if random.random() < rate and len(word) > 2:
                word = NameCorruptor.apply_typo(word)
            corrupted.append(word)
        return " ".join(corrupted)

    @staticmethod
    def reorder_components(text: str) -> str:
        parts = text.split(",")
        if len(parts) >= 2:
            return f"{parts[1].strip()}, {parts[0].strip()}"
        return text

    @staticmethod
    def swap_digits(text: str) -> str:
        """Swap adjacent digits to simulate data entry errors: 123 → 132."""
        digits = [(i, c) for i, c in enumerate(text) if c.isdigit()]
        if len(digits) >= 2:
            chars = list(text)
            idx1, idx2 = digits[0][0], digits[1][0]
            chars[idx1], chars[idx2] = chars[idx2], chars[idx1]
            return "".join(chars)
        return text

    @staticmethod
    def change_unit_format(text: str) -> str:
        """Change apartment/unit format: 'Apt 4' → '#4', 'Unit 4', etc."""
        for unit_word, variants in UNIT_FORMATS.items():
            pattern = rf"\b{re.escape(unit_word)}\.?\s*"
            if re.search(pattern, text, re.IGNORECASE):
                replacement = random.choice(variants)
                text = re.sub(pattern, f"{replacement} ", text, flags=re.IGNORECASE)
                break
        return text.strip()

    @staticmethod
    def change_directional(text: str) -> str:
        """Change directional format: 'North Main' → 'N Main' or vice versa."""
        for direction, variants in DIRECTIONAL_MAP.items():
            pattern = rf"\b{re.escape(direction)}\b"
            if re.search(pattern, text):
                replacement = random.choice(variants)
                text = re.sub(pattern, replacement, text, count=1)
                break
        return text

    @staticmethod
    def drop_component(text: str) -> str:
        """Remove a component: drop ZIP code, city, or state."""
        drop_type = random.choice(["zip", "last_part"])
        if drop_type == "zip":
            text = re.sub(ADDRESS_COMPONENTS["zip_pattern"], "", text)
        elif drop_type == "last_part":
            parts = [p.strip() for p in text.split(",")]
            if len(parts) > 1:
                text = ", ".join(parts[:-1])
        return text.strip().rstrip(",").strip()


# =============================================================================
# DATA CORRUPTOR (orchestrator)
# =============================================================================
class DataCorruptor:
    """Main corruption orchestrator with expanded strategy set."""

    # Corruption strategies with relative weights
    NAME_STRATEGIES = [
        ("nickname", 15),
        ("swap", 10),
        ("middle_initial", 10),
        ("drop_middle", 10),
        ("expand_middle", 5),
        ("typo", 15),
        ("ocr_error", 15),
        ("transliteration", 10),
        ("punctuation", 10),
    ]

    ADDRESS_STRATEGIES = [
        ("abbreviate", 15),
        ("expand", 10),
        ("typo", 15),
        ("reorder", 5),
        ("swap_digits", 15),
        ("change_unit", 10),
        ("change_directional", 10),
        ("drop_component", 10),
        ("ocr_error", 10),
    ]

    def __init__(self, corruption_rate: float = 0.15):
        self.corruption_rate = corruption_rate
        self._name_strategies, self._name_weights = zip(*self.NAME_STRATEGIES)
        self._addr_strategies, self._addr_weights = zip(*self.ADDRESS_STRATEGIES)

    def _pick_name_strategy(self) -> str:
        return random.choices(self._name_strategies, self._name_weights, k=1)[0]

    def _pick_addr_strategy(self) -> str:
        return random.choices(self._addr_strategies, self._addr_weights, k=1)[0]

    def corrupt_name(self, name: str) -> str:
        """Apply a single corruption strategy to a name."""
        if not name or pd.isna(name) or not str(name).strip():
            return name
        name = str(name)
        strategy = self._pick_name_strategy()
        try:
            if strategy == "nickname":
                return NameCorruptor.apply_nickname(name)
            elif strategy == "swap":
                return NameCorruptor.swap_first_last(name)
            elif strategy == "middle_initial":
                return NameCorruptor.add_middle_initial(name)
            elif strategy == "drop_middle":
                return NameCorruptor.drop_middle_name(name)
            elif strategy == "expand_middle":
                return NameCorruptor.expand_middle_initial(name)
            elif strategy == "typo":
                return NameCorruptor.apply_typo(name)
            elif strategy == "ocr_error":
                return NameCorruptor.apply_ocr_error(name)
            elif strategy == "transliteration":
                return NameCorruptor.apply_transliteration(name)
            elif strategy == "punctuation":
                return NameCorruptor.apply_punctuation_variant(name)
        except Exception as e:
            logger.warning(f"Name corruption ({strategy}) failed: {e}")
        return name

    def corrupt_address(self, address: str) -> str:
        """Apply a single corruption strategy to an address."""
        if not address or pd.isna(address) or not str(address).strip():
            return address
        address = str(address)
        strategy = self._pick_addr_strategy()
        try:
            if strategy == "abbreviate":
                return AddressCorruptor.abbreviate(address)
            elif strategy == "expand":
                return AddressCorruptor.expand_abbreviations(address)
            elif strategy == "typo":
                return AddressCorruptor.apply_typo(address, rate=self.corruption_rate)
            elif strategy == "reorder":
                return AddressCorruptor.reorder_components(address)
            elif strategy == "swap_digits":
                return AddressCorruptor.swap_digits(address)
            elif strategy == "change_unit":
                return AddressCorruptor.change_unit_format(address)
            elif strategy == "change_directional":
                return AddressCorruptor.change_directional(address)
            elif strategy == "drop_component":
                return AddressCorruptor.drop_component(address)
            elif strategy == "ocr_error":
                return NameCorruptor.apply_ocr_error(address)
        except Exception as e:
            logger.warning(f"Address corruption ({strategy}) failed: {e}")
        return address

    def corrupt_record(self, record: pd.Series) -> pd.Series:
        """Apply corruption to a single record's name and address."""
        corrupted = record.copy()
        if (
            "name" in corrupted
            and pd.notna(corrupted["name"])
            and str(corrupted["name"]).strip()
        ):
            if random.random() < self.corruption_rate:
                corrupted["name"] = self.corrupt_name(str(record["name"]))
        if (
            "address" in corrupted
            and pd.notna(corrupted["address"])
            and str(corrupted["address"]).strip()
        ):
            if random.random() < self.corruption_rate:
                corrupted["address"] = self.corrupt_address(str(record["address"]))
        return corrupted

    def corrupt_record_forced(
        self, record: pd.Series, n_corruptions: int = 2
    ) -> pd.Series:
        """
        Apply guaranteed corruption (ignoring corruption_rate).
        Used for generating corrupted positive pairs where corruption must happen.
        """
        corrupted = record.copy()
        fields_corrupted = 0
        if (
            "name" in corrupted
            and pd.notna(corrupted["name"])
            and str(corrupted["name"]).strip()
        ):
            corrupted["name"] = self.corrupt_name(str(record["name"]))
            fields_corrupted += 1
        if (
            fields_corrupted < n_corruptions
            and "address" in corrupted
            and pd.notna(corrupted["address"])
            and str(corrupted["address"]).strip()
        ):
            corrupted["address"] = self.corrupt_address(str(record["address"]))
        return corrupted

    def expand_dataset(self, df: pd.DataFrame, target_size: int) -> pd.DataFrame:
        """Expand dataset by creating corrupted variants."""
        if len(df) == 0 or target_size == 0:
            logger.warning("Empty dataset or target_size=0, returning empty DataFrame")
            return df

        expansion_factor = max(1, target_size // len(df))
        logger.info(
            f"Expanding dataset: {len(df)} -> {target_size} records "
            f"(factor: {expansion_factor})"
        )

        expanded_records = []
        record_counter = 0

        for idx, row in df.iterrows():
            original = row.copy()
            original["cluster_id"] = row["id"]
            expanded_records.append(original)
            record_counter += 1

            for variant_num in range(1, expansion_factor):
                corrupted = self.corrupt_record(row)
                corrupted["id"] = f"{row['id']}_var{variant_num}"
                corrupted["cluster_id"] = row["id"]
                expanded_records.append(corrupted)
                record_counter += 1
                if record_counter >= target_size:
                    break
            if record_counter >= target_size:
                break

        expanded_df = pd.DataFrame(expanded_records)
        logger.info(f"Expansion complete: {len(expanded_df)} records generated")
        return expanded_df[:target_size]


# =============================================================================
# PAIR GENERATION
# =============================================================================
class PairGenerator:
    """
    Generate training pairs with five pair types:
    - clean_positive (30%): existing same-cluster matches
    - corrupted_positive (25%): augmented matches with forced corruption
    - hard_negative (35%): high-overlap non-matches
    - easy_negative (8%): random non-matches
    - relationship_pair (2%): same surname + address, different people
    """

    PAIR_COLUMNS = [
        "id1",
        "id2",
        "name1",
        "name2",
        "address1",
        "address2",
        "label",
        "pair_type",
    ]
    REQUIRED_FIELDS = ["id", "cluster_id", "name", "address"]

    DEFAULT_DISTRIBUTION = {
        "clean_positive": 0.30,
        "corrupted_positive": 0.25,
        "hard_negative": 0.35,
        "easy_negative": 0.08,
        "relationship_pair": 0.02,
    }

    HARD_NEGATIVE_METHODS = {
        "nearest_neighbor": 0.25,
        "partial_overlap": 0.20,
        "blocking_collision": 0.15,
        "address_component": 0.20,
        "address_collision": 0.10,
        "phonetic_near_miss": 0.10,
    }

    def __init__(self, corruption_rate: float = 0.25):
        self.corruptor = DataCorruptor(corruption_rate)

    @staticmethod
    def _validate_fields(df: pd.DataFrame):
        missing = [f for f in PairGenerator.REQUIRED_FIELDS if f not in df.columns]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

    @staticmethod
    def _make_pair(
        rec1: pd.Series, rec2: pd.Series, label: int, pair_type: str
    ) -> dict:
        return {
            "id1": rec1["id"],
            "id2": rec2["id"],
            "name1": rec1["name"],
            "name2": rec2["name"],
            "address1": rec1.get("address", ""),
            "address2": rec2.get("address", ""),
            "label": label,
            "pair_type": pair_type,
        }

    # -----------------------------------------------------------------
    # Clean positives: existing same-cluster pairs
    # -----------------------------------------------------------------
    def generate_clean_positives(self, df: pd.DataFrame, n_pairs: int) -> List[dict]:
        self._validate_fields(df)
        grouped = df.groupby("cluster_id")
        pairs = []
        pairs_per_cluster = max(1, n_pairs // len(grouped))

        for cluster_id, group in grouped:
            if len(group) < 2:
                continue
            max_possible = min(pairs_per_cluster, len(group) * (len(group) - 1) // 2)
            for _ in range(max_possible):
                sample = group.sample(2)
                pairs.append(
                    self._make_pair(
                        sample.iloc[0],
                        sample.iloc[1],
                        label=1,
                        pair_type="clean_positive",
                    )
                )
            if len(pairs) >= n_pairs:
                break

        logger.info(f"Clean positives: {len(pairs[:n_pairs])} (requested {n_pairs})")
        return pairs[:n_pairs]

    # -----------------------------------------------------------------
    # Address component negatives: same name + street, different city/zip
    # Teaches: postcode and city are discriminating signals
    # -----------------------------------------------------------------
    def _address_component_negatives(
        self, df: pd.DataFrame, n_pairs: int
    ) -> List[dict]:
        """
        Same name, same street name, different city/state/ZIP.
        Examples:
          Mike Green, 45 MG Road, Boston MA 02145 vs
          Michael Greene, 45 MG Road, Los Angeles CA 90001 → label 0

          John Smith, 100 Main St, Austin TX 73301 vs
          John Smith, 100 Main St, Austin MN 55912 → label 0
        """
        pairs = []
        records = df.to_dict("records")
        streets = [
            "Main St",
            "Oak Ave",
            "Elm St",
            "Pine Dr",
            "Cedar Ln",
            "Maple Rd",
            "Park Ave",
            "Washington St",
            "Broad St",
            "MG Road",
        ]

        for _ in range(n_pairs):
            if not records:
                break
            base = random.choice(records)
            base_name = str(base.get("name", ""))
            if not base_name.strip():
                continue

            # Pick a street (reuse base street or pick a common one)
            base_addr = str(base.get("address", ""))
            street_num = str(random.randint(1, 999))

            # Extract street name from base address or use a common one
            addr_parts = base_addr.split(",")
            if addr_parts:
                street_part = addr_parts[0].strip()
            else:
                street_part = f"{street_num} {random.choice(streets)}"

            # Pick two different cities
            city1, city2 = random.sample(CITY_STATE_ZIP_POOLS, 2)

            addr1 = f"{street_part}, {city1['city']}, {city1['state']} {city1['zip']}"
            addr2 = f"{street_part}, {city2['city']}, {city2['state']} {city2['zip']}"

            # Optionally apply name variant to one side (nickname, OCR)
            name_variant = base_name
            if random.random() < 0.5:
                name_variant = self.corruptor.corrupt_name(base_name)

            rec1 = pd.Series(
                {
                    "id": f"{base['id']}_ac1_{random.randint(1000, 9999)}",
                    "name": base_name,
                    "address": addr1,
                    "cluster_id": f"ac_{base['cluster_id']}_1",
                }
            )
            rec2 = pd.Series(
                {
                    "id": f"{base['id']}_ac2_{random.randint(1000, 9999)}",
                    "name": name_variant,
                    "address": addr2,
                    "cluster_id": f"ac_{base['cluster_id']}_2",
                }
            )

            pairs.append(
                self._make_pair(
                    rec1,
                    rec2,
                    label=0,
                    pair_type="hard_negative_addr_component",
                )
            )

        logger.info(f"Address component negatives: {len(pairs)} (requested {n_pairs})")
        return pairs[:n_pairs]

    # -----------------------------------------------------------------
    # Address collision negatives: same address, different names
    # Teaches: address alone doesn't mean same person
    # -----------------------------------------------------------------
    def _address_collision_negatives(
        self, df: pd.DataFrame, n_pairs: int
    ) -> List[dict]:
        """
        Same address, completely different names.
        Examples:
          Ivan Petrov, 48 Main St vs Ahmed Hassan, 48 Main St → label 0
          Jennifer Wilson, 200 Oak Ave vs Carlos Garcia, 200 Oak Ave → label 0
        """
        pairs = []
        records = df.to_dict("records")

        for _ in range(n_pairs):
            if len(records) < 2:
                break

            # Pick two records with different names
            r1 = random.choice(records)
            r2 = random.choice(records)
            attempts = 0
            while (
                str(r1.get("name", "")).lower() == str(r2.get("name", "")).lower()
                or str(r1["cluster_id"]) == str(r2["cluster_id"])
            ) and attempts < 20:
                r2 = random.choice(records)
                attempts += 1

            if str(r1.get("name", "")).lower() == str(r2.get("name", "")).lower():
                continue

            # Give both the same address (use r1's address)
            shared_address = str(r1.get("address", ""))
            if not shared_address.strip():
                continue

            rec1 = pd.Series(
                {
                    "id": f"{r1['id']}_col1_{random.randint(1000, 9999)}",
                    "name": str(r1["name"]),
                    "address": shared_address,
                    "cluster_id": str(r1["cluster_id"]),
                }
            )
            rec2 = pd.Series(
                {
                    "id": f"{r2['id']}_col2_{random.randint(1000, 9999)}",
                    "name": str(r2["name"]),
                    "address": shared_address,
                    "cluster_id": str(r2["cluster_id"]),
                }
            )

            pairs.append(
                self._make_pair(
                    rec1,
                    rec2,
                    label=0,
                    pair_type="hard_negative_addr_collision",
                )
            )

        logger.info(f"Address collision negatives: {len(pairs)} (requested {n_pairs})")
        return pairs[:n_pairs]

    # -----------------------------------------------------------------
    # Phonetic near-miss negatives: similar-sounding name, different address
    # Teaches: name phonetic similarity alone is insufficient
    # -----------------------------------------------------------------
    def _phonetic_near_miss_negatives(
        self, df: pd.DataFrame, n_pairs: int
    ) -> List[dict]:
        """
        Similar-sounding last names paired with completely different addresses.
        Examples:
          Garcia, 100 Main St Boston vs Garsia, 500 Oak Ave Chicago → label 0
          Smith, 48 Elm St vs Smyth, 200 Pine Dr → label 0
          Green, 45 MG Road Boston vs Greene, 45 MG Road LA → label 0
        """
        pairs = []
        records = df.to_dict("records")
        confusion_keys = list(PHONETIC_CONFUSION_PAIRS.keys())

        for _ in range(n_pairs):
            if not records:
                break
            base = random.choice(records)
            base_name = str(base.get("name", ""))
            base_addr = str(base.get("address", ""))
            parts = base_name.split()
            if len(parts) < 2 or not base_addr.strip():
                continue

            last_name = parts[-1]
            first_name = parts[0]

            # Try to find a phonetic variant for the last name
            phonetic_variant = None
            last_title = last_name.title()
            if last_title in PHONETIC_CONFUSION_PAIRS:
                phonetic_variant = random.choice(PHONETIC_CONFUSION_PAIRS[last_title])
            else:
                # Try matching any key as substring
                for key in confusion_keys:
                    if key.lower() == last_name.lower():
                        phonetic_variant = random.choice(PHONETIC_CONFUSION_PAIRS[key])
                        break

            if not phonetic_variant:
                # Fall back to OCR-style corruption of the last name
                phonetic_variant = NameCorruptor.apply_ocr_error(last_name)
                if phonetic_variant == last_name:
                    phonetic_variant = NameCorruptor.apply_typo(last_name)

            # Get a completely different address from another record
            other = random.choice(records)
            attempts = 0
            while str(other["cluster_id"]) == str(base["cluster_id"]) and attempts < 10:
                other = random.choice(records)
                attempts += 1
            other_addr = str(other.get("address", ""))

            # Optionally also vary the first name slightly
            variant_first = first_name
            if random.random() < 0.3:
                variant_first = NameCorruptor.apply_typo(first_name)

            rec1 = pd.Series(
                {
                    "id": f"{base['id']}_ph1_{random.randint(1000, 9999)}",
                    "name": base_name,
                    "address": base_addr,
                    "cluster_id": str(base["cluster_id"]),
                }
            )
            rec2 = pd.Series(
                {
                    "id": f"{other['id']}_ph2_{random.randint(1000, 9999)}",
                    "name": f"{variant_first} {phonetic_variant}",
                    "address": other_addr,
                    "cluster_id": str(other["cluster_id"]),
                }
            )

            pairs.append(
                self._make_pair(
                    rec1,
                    rec2,
                    label=0,
                    pair_type="hard_negative_phonetic",
                )
            )

        logger.info(f"Phonetic near-miss negatives: {len(pairs)} (requested {n_pairs})")
        return pairs[:n_pairs]

    # -----------------------------------------------------------------
    # Corrupted positives: forced corruption on one side of a match
    # -----------------------------------------------------------------
    def generate_corrupted_positives(
        self, df: pd.DataFrame, n_pairs: int
    ) -> List[dict]:
        self._validate_fields(df)
        grouped = df.groupby("cluster_id")
        pairs = []

        clusters_with_records = [(cid, grp) for cid, grp in grouped if len(grp) >= 1]
        if not clusters_with_records:
            return []

        for _ in range(n_pairs):
            cid, group = random.choice(clusters_with_records)
            original = group.sample(1).iloc[0]
            corrupted = self.corruptor.corrupt_record_forced(original, n_corruptions=2)
            corrupted["id"] = f"{original['id']}_aug{random.randint(1000, 9999)}"
            pairs.append(
                self._make_pair(
                    original,
                    corrupted,
                    label=1,
                    pair_type="corrupted_positive",
                )
            )

        logger.info(f"Corrupted positives: {len(pairs)} (requested {n_pairs})")
        return pairs

    # -----------------------------------------------------------------
    # Hard negatives: high-overlap non-matches
    # -----------------------------------------------------------------
    def generate_hard_negatives(self, df: pd.DataFrame, n_pairs: int) -> List[dict]:
        self._validate_fields(df)
        methods = self.HARD_NEGATIVE_METHODS
        n_nearest = int(n_pairs * methods["nearest_neighbor"])
        n_partial = int(n_pairs * methods["partial_overlap"])
        n_blocking = int(n_pairs * methods["blocking_collision"])
        n_addr_component = int(n_pairs * methods["address_component"])
        n_addr_collision = int(n_pairs * methods["address_collision"])
        n_phonetic = (
            n_pairs
            - n_nearest
            - n_partial
            - n_blocking
            - n_addr_component
            - n_addr_collision
        )

        pairs = []
        pairs.extend(self._nearest_neighbor_negatives(df, n_nearest))
        pairs.extend(self._partial_overlap_negatives(df, n_partial))
        pairs.extend(self._blocking_collision_negatives(df, n_blocking))
        pairs.extend(self._address_component_negatives(df, n_addr_component))
        pairs.extend(self._address_collision_negatives(df, n_addr_collision))
        pairs.extend(self._phonetic_near_miss_negatives(df, n_phonetic))

        random.shuffle(pairs)
        logger.info(f"Hard negatives: {len(pairs)} (requested {n_pairs})")
        return pairs[:n_pairs]

    def _nearest_neighbor_negatives(self, df: pd.DataFrame, n_pairs: int) -> List[dict]:
        """
        Find pairs with high name similarity but different cluster_ids.
        Uses blocking on first 3 characters of last name for efficiency.
        """
        pairs = []
        blocks = defaultdict(list)

        for idx, row in df.iterrows():
            name = str(row.get("name", ""))
            parts = name.split()
            if parts:
                key = parts[-1][:3].lower()
                blocks[key].append(row)

        for key, records in blocks.items():
            if len(records) < 2:
                continue
            for i in range(min(len(records) - 1, n_pairs // max(len(blocks), 1))):
                r1 = records[i]
                r2 = records[i + 1]
                if str(r1["cluster_id"]) != str(r2["cluster_id"]):
                    pairs.append(
                        self._make_pair(
                            r1,
                            r2,
                            label=0,
                            pair_type="hard_negative_nearest",
                        )
                    )

        return pairs[:n_pairs]

    def _partial_overlap_negatives(self, df: pd.DataFrame, n_pairs: int) -> List[dict]:
        """
        Take a real match pair and change one critical field to create
        a convincing non-match. E.g., same name + different address.
        """
        pairs = []
        grouped = df.groupby("cluster_id")
        all_records = df.to_dict("records")

        for cluster_id, group in grouped:
            if len(group) < 1:
                continue
            anchor = group.sample(1).iloc[0]

            # Find a record from a different cluster
            other = random.choice(all_records)
            attempts = 0
            while str(other["cluster_id"]) == str(cluster_id) and attempts < 10:
                other = random.choice(all_records)
                attempts += 1

            if str(other["cluster_id"]) == str(cluster_id):
                continue

            other_series = pd.Series(other)

            # Create partial overlap: keep anchor's name, use other's address
            overlap_type = random.choice(["swap_address", "swap_name"])
            if overlap_type == "swap_address":
                fake = anchor.copy()
                fake["address"] = other_series["address"]
                fake["id"] = f"{anchor['id']}_po{random.randint(1000, 9999)}"
                fake["cluster_id"] = f"fake_{anchor['cluster_id']}"
                pairs.append(
                    self._make_pair(
                        anchor,
                        fake,
                        label=0,
                        pair_type="hard_negative_partial",
                    )
                )
            else:
                fake = other_series.copy()
                fake["name"] = anchor["name"]
                fake["id"] = f"{other_series['id']}_po{random.randint(1000, 9999)}"
                pairs.append(
                    self._make_pair(
                        anchor,
                        fake,
                        label=0,
                        pair_type="hard_negative_partial",
                    )
                )

            if len(pairs) >= n_pairs:
                break

        return pairs[:n_pairs]

    def _blocking_collision_negatives(
        self, df: pd.DataFrame, n_pairs: int
    ) -> List[dict]:
        """
        Same last name + same street but different people.
        Simulates blocking collisions in production entity resolution.
        """
        pairs = []

        # Group by last name
        last_name_groups = defaultdict(list)
        for idx, row in df.iterrows():
            name = str(row.get("name", ""))
            parts = name.split()
            if parts:
                last_name_groups[parts[-1].lower()].append(row)

        for last_name, records in last_name_groups.items():
            if len(records) < 2:
                continue
            for i in range(len(records)):
                for j in range(i + 1, min(len(records), i + 3)):
                    r1 = records[i]
                    r2 = records[j]
                    if str(r1["cluster_id"]) != str(r2["cluster_id"]):
                        pairs.append(
                            self._make_pair(
                                r1,
                                r2,
                                label=0,
                                pair_type="hard_negative_blocking",
                            )
                        )
                if len(pairs) >= n_pairs:
                    break
            if len(pairs) >= n_pairs:
                break

        return pairs[:n_pairs]

    # -----------------------------------------------------------------
    # Easy negatives: random non-matches
    # -----------------------------------------------------------------
    def generate_easy_negatives(self, df: pd.DataFrame, n_pairs: int) -> List[dict]:
        self._validate_fields(df)
        cluster_list = list(df["cluster_id"].unique())
        if len(cluster_list) < 2:
            return []

        cluster_to_records = {c: df[df["cluster_id"] == c] for c in cluster_list}
        pairs = []

        for _ in range(n_pairs):
            try:
                c1, c2 = random.sample(cluster_list, 2)
                r1 = cluster_to_records[c1].sample(1).iloc[0]
                r2 = cluster_to_records[c2].sample(1).iloc[0]
                pairs.append(
                    self._make_pair(
                        r1,
                        r2,
                        label=0,
                        pair_type="easy_negative",
                    )
                )
            except Exception as e:
                logger.warning(f"Easy negative generation failed: {e}")
                continue

        logger.info(f"Easy negatives: {len(pairs)} (requested {n_pairs})")
        return pairs

    # -----------------------------------------------------------------
    # Relationship pairs: same surname + address, different people
    # -----------------------------------------------------------------
    def generate_relationship_pairs(self, df: pd.DataFrame, n_pairs: int) -> List[dict]:
        """
        Generate pairs simulating family members at the same address.
        Same last name, same address, different first name → label 0.
        """
        pairs = []
        first_names_m = [
            "James",
            "John",
            "Robert",
            "Michael",
            "William",
            "David",
            "Richard",
            "Joseph",
            "Thomas",
            "Charles",
        ]
        first_names_f = [
            "Mary",
            "Patricia",
            "Jennifer",
            "Linda",
            "Barbara",
            "Elizabeth",
            "Susan",
            "Jessica",
            "Sarah",
            "Karen",
        ]

        records = df.to_dict("records")
        for _ in range(n_pairs):
            if not records:
                break
            base = random.choice(records)
            base_name = str(base.get("name", ""))
            parts = base_name.split()
            if len(parts) < 2:
                continue

            last_name = parts[-1]
            # Generate a family member with different first name
            new_first = random.choice(first_names_m + first_names_f)
            while new_first.lower() == parts[0].lower():
                new_first = random.choice(first_names_m + first_names_f)

            family_member = pd.Series(
                {
                    "id": f"{base['id']}_fam{random.randint(1000, 9999)}",
                    "name": f"{new_first} {last_name}",
                    "address": base.get("address", ""),
                    "cluster_id": f"fam_{base['cluster_id']}",
                }
            )

            pairs.append(
                self._make_pair(
                    pd.Series(base),
                    family_member,
                    label=0,
                    pair_type="relationship_pair",
                )
            )

        logger.info(f"Relationship pairs: {len(pairs)} (requested {n_pairs})")
        return pairs[:n_pairs]

    # -----------------------------------------------------------------
    # Main entry point: generate all pair types
    # -----------------------------------------------------------------
    def generate_pairs(
        self,
        df: pd.DataFrame,
        total_pairs: int,
        distribution: Optional[Dict[str, float]] = None,
    ) -> pd.DataFrame:
        """
        Generate balanced training pairs with configurable distribution.

        Args:
            df: Expanded dataset with cluster_id column
            total_pairs: Total number of pairs to generate
            distribution: Dict mapping pair_type → ratio (must sum to 1.0)

        Returns:
            DataFrame with columns: id1, id2, name1, name2, address1, address2,
                                    label, pair_type
        """
        dist = distribution or self.DEFAULT_DISTRIBUTION

        n_clean = int(total_pairs * dist["clean_positive"])
        n_corrupted = int(total_pairs * dist["corrupted_positive"])
        n_hard = int(total_pairs * dist["hard_negative"])
        n_easy = int(total_pairs * dist["easy_negative"])
        n_relationship = total_pairs - n_clean - n_corrupted - n_hard - n_easy

        logger.info(
            f"Generating {total_pairs} pairs: "
            f"clean_pos={n_clean} corrupted_pos={n_corrupted} "
            f"hard_neg={n_hard} easy_neg={n_easy} relationship={n_relationship}"
        )

        all_pairs = []
        all_pairs.extend(self.generate_clean_positives(df, n_clean))
        all_pairs.extend(self.generate_corrupted_positives(df, n_corrupted))
        all_pairs.extend(self.generate_hard_negatives(df, n_hard))
        all_pairs.extend(self.generate_easy_negatives(df, n_easy))
        all_pairs.extend(self.generate_relationship_pairs(df, n_relationship))

        result_df = pd.DataFrame(all_pairs, columns=self.PAIR_COLUMNS)

        if len(result_df) > 0:
            result_df = result_df.sample(frac=1, random_state=RANDOM_SEED).reset_index(
                drop=True
            )

        # Log distribution
        if len(result_df) > 0:
            pos = (result_df["label"] == 1).sum()
            neg = (result_df["label"] == 0).sum()
            logger.info(
                f"Final pairs: {len(result_df)} (positive={pos}, negative={neg})"
            )
            for pt, count in result_df["pair_type"].value_counts().items():
                logger.info(f"  {pt}: {count} ({count / len(result_df) * 100:.1f}%)")

        return result_df


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================
def preprocess_dataset(
    df: pd.DataFrame, config: Dict
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main preprocessing pipeline.

    Args:
        df: Input dataframe with raw records
        config: Configuration dict with target_records, corruption rate,
                pairs config, and optional pair_distribution

    Returns:
        Tuple of (expanded_accounts_df, pairs_df)
    """
    random.seed(RANDOM_SEED)

    logger.info(f"[Preprocessing] Starting with {len(df)} base records")

    if len(df) == 0:
        logger.warning("[Preprocessing] Empty input dataframe")
        return df, pd.DataFrame(columns=PairGenerator.PAIR_COLUMNS)

    # Step 1: Normalize
    logger.info("[Preprocessing] Normalizing values...")
    try:
        normalizer = DataNormalizer()
        df = df.apply(normalizer.normalize_record, axis=1)
    except Exception as e:
        logger.error(f"[Preprocessing] Normalization failed: {e}")
        raise

    # Step 2: Expand with corruption
    target_size = config.get("target_records", len(df))
    corruption_rate = config.get("corruption", {}).get("rate", 0.15)

    logger.info(
        f"[Preprocessing] Expanding to {target_size} records "
        f"(corruption rate: {corruption_rate})"
    )

    try:
        corruptor = DataCorruptor(corruption_rate)
        expanded_df = corruptor.expand_dataset(df, target_size)
    except Exception as e:
        logger.error(f"[Preprocessing] Expansion failed: {e}")
        raise

    logger.info(f"[Preprocessing] Expanded to {len(expanded_df)} records")

    # Step 3: Generate pairs
    pair_config = config.get("pairs", {})
    total_pairs = pair_config.get("positive", 1000) + pair_config.get("negative", 1000)
    pair_distribution = config.get("pair_distribution", None)

    try:
        generator = PairGenerator(corruption_rate)
        pairs_df = generator.generate_pairs(
            expanded_df,
            total_pairs,
            distribution=pair_distribution,
        )
    except Exception as e:
        logger.error(f"[Preprocessing] Pair generation failed: {e}")
        raise

    logger.info(f"[Preprocessing] Generated {len(pairs_df)} pairs")
    pos = (pairs_df["label"] == 1).sum()
    neg = (pairs_df["label"] == 0).sum()
    logger.info(f"  Positive: {pos} | Negative: {neg}")

    return expanded_df, pairs_df
