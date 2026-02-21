import logging
import random
import re
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataNormalizer:
    """Normalize values before corruption and matching."""

    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize person/product/author names."""
        if pd.isna(name):
            return ""

        # Remove extra whitespace
        name = re.sub(r"\s+", " ", str(name).strip())

        # Title case (proper names)
        name = name.title()

        # Keep alphanumeric, spaces, hyphens, periods (FIXED: added 0-9)
        name = re.sub(r"[^a-zA-Z0-9\s\-\.]", "", name)

        return name.strip()

    @staticmethod
    def normalize_address(address: str) -> str:
        """Normalize addresses/descriptions/titles."""
        if pd.isna(address):
            return ""

        address = str(address).strip()

        # Remove extra whitespace
        address = re.sub(r"\s+", " ", address)

        # Standardize common abbreviations (word boundaries to avoid partial matches)
        address = re.sub(r"\bSt\b\.?", "Street", address, flags=re.IGNORECASE)
        address = re.sub(r"\bAve\b\.?", "Avenue", address, flags=re.IGNORECASE)
        address = re.sub(r"\bBlvd\b\.?", "Boulevard", address, flags=re.IGNORECASE)
        address = re.sub(r"\bRd\b\.?", "Road", address, flags=re.IGNORECASE)
        address = re.sub(r"\bDr\b\.?", "Drive", address, flags=re.IGNORECASE)
        address = re.sub(r"\bLn\b\.?", "Lane", address, flags=re.IGNORECASE)

        # Title case
        address = address.title()

        return address.strip()

    @staticmethod
    def normalize_date(date_str: str) -> str:
        """Normalize dates to YYYY-MM-DD format."""
        if pd.isna(date_str):
            return ""

        try:
            # Try parsing common formats
            if isinstance(date_str, str):
                # Remove any non-numeric except dashes/slashes
                cleaned = re.sub(r"[^\d\-/]", "", str(date_str))
                dt = pd.to_datetime(cleaned)
                return dt.strftime("%Y-%m-%d")
            return str(date_str)
        except Exception as e:
            logger.warning(f"Could not parse date '{date_str}': {e}")
            return str(date_str)

    @staticmethod
    def normalize_record(record: pd.Series) -> pd.Series:
        """Normalize entire record."""
        normalized = record.copy()

        if "name" in normalized and pd.notna(normalized["name"]):
            normalized["name"] = DataNormalizer.normalize_name(normalized["name"])

        if "address" in normalized and pd.notna(normalized["address"]):
            normalized["address"] = DataNormalizer.normalize_address(
                normalized["address"]
            )

        if "dob" in normalized and pd.notna(normalized["dob"]):
            normalized["dob"] = DataNormalizer.normalize_date(normalized["dob"])

        # Remove leading/trailing whitespace from all string columns
        for col in normalized.index:
            if isinstance(normalized[col], str):
                normalized[col] = normalized[col].strip()

        return normalized


class NameCorruptor:
    """Generate realistic name variants and corruptions."""

    NICKNAME_MAP = {
        "Robert": ["Bob", "Rob", "Bobby"],
        "William": ["Bill", "Will", "Billy"],
        "James": ["Jim", "Jimmy", "Jamie"],
        "Michael": ["Mike", "Mikey", "Mick"],
        "Richard": ["Rick", "Dick", "Richie"],
        "Thomas": ["Tom", "Tommy"],
        "Charles": ["Charlie", "Chuck"],
        "Elizabeth": ["Liz", "Beth", "Betty", "Eliza"],
        "Jennifer": ["Jen", "Jenny"],
        "Jessica": ["Jess", "Jessie"],
        "Margaret": ["Maggie", "Meg", "Peggy"],
        "Katherine": ["Kate", "Kathy", "Katie"],
    }

    @staticmethod
    def apply_nickname(name: str) -> str:
        """Convert formal name to nickname."""
        parts = name.split()
        if len(parts) > 0 and parts[0] in NameCorruptor.NICKNAME_MAP:
            nicknames = NameCorruptor.NICKNAME_MAP[parts[0]]
            parts[0] = random.choice(nicknames)
        return " ".join(parts)

    @staticmethod
    def swap_first_last(name: str) -> str:
        """Swap first and last name."""
        parts = name.split()
        if len(parts) >= 2:
            return f"{parts[-1]}, {' '.join(parts[:-1])}"
        return name

    @staticmethod
    def add_middle_initial(name: str) -> str:
        """Add middle initial."""
        parts = name.split()
        if len(parts) == 2:
            initial = chr(random.randint(65, 90))  # A-Z
            return f"{parts[0]} {initial}. {parts[1]}"
        return name

    @staticmethod
    def apply_typo(text: str) -> str:
        """Introduce random typo (works for any text)."""
        if len(text) < 3:
            return text

        pos = random.randint(0, len(text) - 1)
        typo_type = random.choice(["delete", "substitute", "transpose"])

        if typo_type == "delete":
            return text[:pos] + text[pos + 1 :]
        elif typo_type == "substitute":
            # Choose appropriate replacement (letter for letter, digit for digit)
            if text[pos].isalpha():
                new_char = chr(random.randint(97, 122))  # a-z
            elif text[pos].isdigit():
                new_char = str(random.randint(0, 9))
            else:
                return text
            return text[:pos] + new_char + text[pos + 1 :]
        else:  # transpose
            if pos < len(text) - 1:
                chars = list(text)
                chars[pos], chars[pos + 1] = chars[pos + 1], chars[pos]
                return "".join(chars)
        return text


class AddressCorruptor:
    """Generate realistic address/text variants and corruptions."""

    ABBREVIATIONS = {
        "Street": ["St", "St.", "Str"],
        "Avenue": ["Ave", "Ave.", "Av"],
        "Boulevard": ["Blvd", "Blvd.", "Boul"],
        "Road": ["Rd", "Rd."],
        "Drive": ["Dr", "Dr.", "Drv"],
        "Lane": ["Ln", "Ln."],
        "Court": ["Ct", "Ct."],
        "Apartment": ["Apt", "Apt.", "#"],
        "North": ["N", "N."],
        "South": ["S", "S."],
        "East": ["E", "E."],
        "West": ["W", "W."],
    }

    @staticmethod
    def abbreviate(text: str) -> str:
        """Convert full words to abbreviations."""
        for full, abbrevs in AddressCorruptor.ABBREVIATIONS.items():
            if full in text:
                text = text.replace(full, random.choice(abbrevs))
        return text

    @staticmethod
    def expand_abbreviations(text: str) -> str:
        """Expand abbreviations to full words."""
        for full, abbrevs in AddressCorruptor.ABBREVIATIONS.items():
            for abbrev in abbrevs:
                # Use word boundaries to avoid partial matches
                pattern = r"\b" + re.escape(abbrev) + r"\b"
                text = re.sub(pattern, full, text, flags=re.IGNORECASE)
        return text

    @staticmethod
    def apply_typo(text: str, rate: float = 0.15) -> str:
        """Introduce typos at specified rate."""
        words = text.split()
        corrupted = []

        for word in words:
            if random.random() < rate and len(word) > 2:
                word = NameCorruptor.apply_typo(word)
            corrupted.append(word)

        return " ".join(corrupted)

    @staticmethod
    def reorder_components(text: str) -> str:
        """Reorder text components (works for comma-separated text)."""
        parts = text.split(",")
        if len(parts) >= 2:
            return f"{parts[1].strip()}, {parts[0].strip()}"
        return text


class DataCorruptor:
    """Main corruption orchestrator."""

    def __init__(self, corruption_rate: float = 0.15):
        self.corruption_rate = corruption_rate
        self.name_corruptor = NameCorruptor()
        self.address_corruptor = AddressCorruptor()

    def corrupt_record(self, record: pd.Series) -> pd.Series:
        """Apply corruption to single record."""
        corrupted = record.copy()

        # Name corruption (only if field exists and has value)
        if (
            "name" in corrupted
            and pd.notna(corrupted["name"])
            and str(corrupted["name"]).strip()
        ):
            if random.random() < self.corruption_rate:
                corruption_type = random.choice(
                    ["nickname", "swap", "middle_initial", "typo"]
                )

                try:
                    if corruption_type == "nickname":
                        corrupted["name"] = self.name_corruptor.apply_nickname(
                            str(record["name"])
                        )
                    elif corruption_type == "swap":
                        corrupted["name"] = self.name_corruptor.swap_first_last(
                            str(record["name"])
                        )
                    elif corruption_type == "middle_initial":
                        corrupted["name"] = self.name_corruptor.add_middle_initial(
                            str(record["name"])
                        )
                    elif corruption_type == "typo":
                        corrupted["name"] = self.name_corruptor.apply_typo(
                            str(record["name"])
                        )
                except Exception as e:
                    logger.warning(f"Name corruption failed: {e}")

        # Address corruption (only if field exists and has value)
        if (
            "address" in corrupted
            and pd.notna(corrupted["address"])
            and str(corrupted["address"]).strip()
        ):
            if random.random() < self.corruption_rate:
                corruption_type = random.choice(
                    ["abbreviate", "expand", "typo", "reorder"]
                )

                try:
                    if corruption_type == "abbreviate":
                        corrupted["address"] = self.address_corruptor.abbreviate(
                            str(record["address"])
                        )
                    elif corruption_type == "expand":
                        corrupted["address"] = (
                            self.address_corruptor.expand_abbreviations(
                                str(record["address"])
                            )
                        )
                    elif corruption_type == "typo":
                        corrupted["address"] = self.address_corruptor.apply_typo(
                            str(record["address"])
                        )
                    elif corruption_type == "reorder":
                        corrupted["address"] = (
                            self.address_corruptor.reorder_components(
                                str(record["address"])
                            )
                        )
                except Exception as e:
                    logger.warning(f"Address corruption failed: {e}")

        return corrupted

    def expand_dataset(self, df: pd.DataFrame, target_size: int) -> pd.DataFrame:
        """Expand dataset by creating corrupted variants."""
        if len(df) == 0 or target_size == 0:
            logger.warning("Empty dataset or target_size=0, returning empty DataFrame")
            return df

        expansion_factor = max(1, target_size // len(df))
        logger.info(
            f"Expanding dataset: {len(df)} → {target_size} records (factor: {expansion_factor})"
        )

        expanded_records = []
        record_counter = 0

        for idx, row in df.iterrows():
            # Keep original
            original = row.copy()
            original["cluster_id"] = row["id"]
            expanded_records.append(original)
            record_counter += 1

            # Generate corrupted variants
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


class PairGenerator:
    """Generate positive and negative training pairs."""

    @staticmethod
    def generate_positive_pairs(df: pd.DataFrame, n_pairs: int) -> pd.DataFrame:
        """Generate positive pairs (same cluster_id)."""
        # Validate required fields exist
        required_fields = ["id", "cluster_id", "name", "address"]
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        if len(df) == 0:
            logger.warning("Empty dataframe, returning empty positive pairs")
            return pd.DataFrame(
                columns=[
                    "id1",
                    "id2",
                    "name1",
                    "name2",
                    "address1",
                    "address2",
                    "label",
                ]
            )

        # Group by cluster_id
        grouped = df.groupby("cluster_id")

        if len(grouped) == 0:
            logger.warning("No cluster groups found, returning empty positive pairs")
            return pd.DataFrame(
                columns=[
                    "id1",
                    "id2",
                    "name1",
                    "name2",
                    "address1",
                    "address2",
                    "label",
                ]
            )

        pairs = []
        pairs_per_cluster = max(1, n_pairs // len(grouped))

        for cluster_id, group in grouped:
            if len(group) < 2:
                continue

            # Sample pairs within cluster
            max_possible_pairs = min(
                pairs_per_cluster, len(group) * (len(group) - 1) // 2
            )

            for _ in range(max_possible_pairs):
                if len(group) < 2:
                    break

                sample = group.sample(min(2, len(group)))
                if len(sample) < 2:
                    continue

                pairs.append(
                    {
                        "id1": sample.iloc[0]["id"],
                        "id2": sample.iloc[1]["id"],
                        "name1": sample.iloc[0]["name"],
                        "name2": sample.iloc[1]["name"],
                        "address1": sample.iloc[0]["address"],
                        "address2": sample.iloc[1]["address"],
                        "label": 1,
                    }
                )

        result_df = pd.DataFrame(pairs[:n_pairs])
        logger.info(f"Generated {len(result_df)} positive pairs (requested: {n_pairs})")

        return result_df

    @staticmethod
    def generate_negative_pairs(df: pd.DataFrame, n_pairs: int) -> pd.DataFrame:
        """Generate negative pairs (different cluster_id)."""
        # Validate required fields exist
        required_fields = ["id", "cluster_id", "name", "address"]
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        unique_clusters = df["cluster_id"].unique()

        if len(unique_clusters) < 2:
            logger.warning(
                f"Insufficient clusters ({len(unique_clusters)}), returning empty negative pairs"
            )
            return pd.DataFrame(
                columns=[
                    "id1",
                    "id2",
                    "name1",
                    "name2",
                    "address1",
                    "address2",
                    "label",
                ]
            )

        pairs = []

        for _ in range(n_pairs):
            try:
                # Sample two different clusters
                cluster1, cluster2 = random.sample(list(unique_clusters), 2)

                record1 = df[df["cluster_id"] == cluster1].sample(1).iloc[0]
                record2 = df[df["cluster_id"] == cluster2].sample(1).iloc[0]

                pairs.append(
                    {
                        "id1": record1["id"],
                        "id2": record2["id"],
                        "name1": record1["name"],
                        "name2": record2["name"],
                        "address1": record1["address"],
                        "address2": record2["address"],
                        "label": 0,
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to generate negative pair: {e}")
                continue

        result_df = pd.DataFrame(pairs)
        logger.info(f"Generated {len(result_df)} negative pairs (requested: {n_pairs})")

        return result_df

    @staticmethod
    def generate_pairs(
        df: pd.DataFrame, n_positive: int, n_negative: int
    ) -> pd.DataFrame:
        """Generate balanced positive and negative pairs."""
        logger.info(f"Generating pairs: {n_positive} positive + {n_negative} negative")

        positive_pairs = PairGenerator.generate_positive_pairs(df, n_positive)
        negative_pairs = PairGenerator.generate_negative_pairs(df, n_negative)

        all_pairs = pd.concat([positive_pairs, negative_pairs], ignore_index=True)

        # Shuffle pairs
        if len(all_pairs) > 0:
            all_pairs = all_pairs.sample(frac=1, random_state=42).reset_index(drop=True)

        logger.info(f"Total pairs generated: {len(all_pairs)}")

        return all_pairs


def preprocess_dataset(
    df: pd.DataFrame, config: Dict
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main preprocessing pipeline.

    Args:
        df: Input dataframe with raw records
        config: Configuration dict with target_records, corruption rate, pairs config

    Returns:
        Tuple of (expanded_accounts_df, pairs_df)
    """
    logger.info(f"[Preprocessing] Starting with {len(df)} base records")

    # Validate input
    if len(df) == 0:
        logger.warning("[Preprocessing] Empty input dataframe")
        return df, pd.DataFrame(
            columns=["id1", "id2", "name1", "name2", "address1", "address2", "label"]
        )

    # Step 1: Normalize values FIRST (before corruption)
    logger.info("[Preprocessing] Normalizing values...")
    try:
        normalizer = DataNormalizer()
        df = df.apply(normalizer.normalize_record, axis=1)
    except Exception as e:
        logger.error(f"[Preprocessing] Normalization failed: {e}")
        raise

    # Step 2: Expand dataset with corruption
    target_size = config.get("target_records", len(df))
    corruption_rate = config.get("corruption", {}).get("rate", 0.15)

    logger.info(
        f"[Preprocessing] Expanding to {target_size} records (corruption rate: {corruption_rate})"
    )

    try:
        corruptor = DataCorruptor(corruption_rate)
        expanded_df = corruptor.expand_dataset(df, target_size)
    except Exception as e:
        logger.error(f"[Preprocessing] Expansion failed: {e}")
        raise

    logger.info(f"[Preprocessing] Expanded to {len(expanded_df)} records")

    # Step 3: Generate pairs
    pair_config = config.get("pairs", {"positive": 1000, "negative": 1000})
    n_positive = pair_config.get("positive", 1000)
    n_negative = pair_config.get("negative", 1000)

    try:
        pairs_df = PairGenerator.generate_pairs(expanded_df, n_positive, n_negative)
    except Exception as e:
        logger.error(f"[Preprocessing] Pair generation failed: {e}")
        raise

    logger.info(f"[Preprocessing] Generated {len(pairs_df)} pairs")
    logger.info(f"  - Positive pairs: {(pairs_df['label'] == 1).sum()}")
    logger.info(f"  - Negative pairs: {(pairs_df['label'] == 0).sum()}")

    return expanded_df, pairs_df
