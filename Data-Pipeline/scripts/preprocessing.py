"""Data corruption and pair generation for entity resolution."""
import pandas as pd
import random
import numpy as np
import re
from typing import List, Tuple, Dict


class DataNormalizer:
    """Normalize values before corruption and matching."""

    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize person/product names."""
        if pd.isna(name):
            return ""

        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', str(name).strip())

        # Title case (proper names)
        name = name.title()

        # Remove special characters (keep letters, spaces, hyphens)
        name = re.sub(r'[^a-zA-Z0-9\s\-\.]', '', name)

        return name

    @staticmethod
    def normalize_address(address: str) -> str:
        """Normalize addresses."""
        if pd.isna(address):
            return ""

        address = str(address).strip()

        # Remove extra whitespace
        address = re.sub(r'\s+', ' ', address)

        # Standardize common abbreviations
        address = re.sub(r'\bSt\b\.?', 'Street', address, flags=re.IGNORECASE)
        address = re.sub(r'\bAve\b\.?', 'Avenue', address, flags=re.IGNORECASE)
        address = re.sub(r'\bBlvd\b\.?', 'Boulevard', address, flags=re.IGNORECASE)
        address = re.sub(r'\bRd\b\.?', 'Road', address, flags=re.IGNORECASE)

        # Title case
        address = address.title()

        return address

    @staticmethod
    def normalize_date(date_str: str) -> str:
        """Normalize dates to YYYY-MM-DD format."""
        if pd.isna(date_str):
            return ""

        # Try parsing common formats
        try:
            from datetime import datetime
            # Try ISO format first
            if isinstance(date_str, str):
                # Remove any non-numeric except dashes/slashes
                cleaned = re.sub(r'[^\d\-/]', '', str(date_str))
                dt = pd.to_datetime(cleaned)
                return dt.strftime('%Y-%m-%d')
            return str(date_str)
        except:
            return str(date_str)

    @staticmethod
    def normalize_record(record: pd.Series) -> pd.Series:
        """Normalize entire record."""
        normalized = record.copy()

        if 'name' in normalized:
            normalized['name'] = DataNormalizer.normalize_name(normalized['name'])

        if 'address' in normalized:
            normalized['address'] = DataNormalizer.normalize_address(normalized['address'])

        if 'dob' in normalized:
            normalized['dob'] = DataNormalizer.normalize_date(normalized['dob'])

        # Remove leading/trailing whitespace from all string columns
        for col in normalized.index:
            if isinstance(normalized[col], str):
                normalized[col] = normalized[col].strip()

        return normalized


class NameCorruptor:
    """Generating realistic name variants and corruptions."""

    NICKNAME_MAP = {
        'Robert': ['Bob', 'Rob', 'Bobby'],
        'William': ['Bill', 'Will', 'Billy'],
        'James': ['Jim', 'Jimmy', 'Jamie'],
        'Michael': ['Mike', 'Mikey', 'Mick'],
        'Richard': ['Rick', 'Dick', 'Richie'],
        'Thomas': ['Tom', 'Tommy'],
        'Charles': ['Charlie', 'Chuck'],
        'Elizabeth': ['Liz', 'Beth', 'Betty', 'Eliza'],
        'Jennifer': ['Jen', 'Jenny'],
        'Jessica': ['Jess', 'Jessie'],
        'Margaret': ['Maggie', 'Meg', 'Peggy'],
        'Katherine': ['Kate', 'Kathy', 'Katie']
    }

    @staticmethod
    def apply_nickname(name: str) -> str:
        """Convert formal name to nickname."""
        parts = name.split()
        if len(parts) > 0 and parts[0] in NameCorruptor.NICKNAME_MAP:
            nicknames = NameCorruptor.NICKNAME_MAP[parts[0]]
            parts[0] = random.choice(nicknames)
        return ' '.join(parts)

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
    def apply_typo(name: str) -> str:
        """Introduce random typo."""
        if len(name) < 3:
            return name

        pos = random.randint(0, len(name) - 1)
        typo_type = random.choice(['delete', 'substitute', 'transpose'])

        if typo_type == 'delete':
            return name[:pos] + name[pos+1:]
        elif typo_type == 'substitute':
            new_char = chr(random.randint(97, 122))
            return name[:pos] + new_char + name[pos+1:]
        else:  # transpose
            if pos < len(name) - 1:
                chars = list(name)
                chars[pos], chars[pos+1] = chars[pos+1], chars[pos]
                return ''.join(chars)
        return name


class AddressCorruptor:
    """Generate realistic address variants and corruptions."""

    ABBREVIATIONS = {
        'Street': ['St', 'St.', 'Str'],
        'Avenue': ['Ave', 'Ave.', 'Av'],
        'Boulevard': ['Blvd', 'Blvd.', 'Boul'],
        'Road': ['Rd', 'Rd.'],
        'Drive': ['Dr', 'Dr.', 'Drv'],
        'Lane': ['Ln', 'Ln.'],
        'Court': ['Ct', 'Ct.'],
        'Apartment': ['Apt', 'Apt.', '#'],
        'North': ['N', 'N.'],
        'South': ['S', 'S.'],
        'East': ['E', 'E.'],
        'West': ['W', 'W.']
    }

    @staticmethod
    def abbreviate(address: str) -> str:
        """Convert full words to abbreviations."""
        for full, abbrevs in AddressCorruptor.ABBREVIATIONS.items():
            if full in address:
                address = address.replace(full, random.choice(abbrevs))
        return address

    @staticmethod
    def expand_abbreviations(address: str) -> str:
        """Expand abbreviations to full words."""

        for full, abbrevs in AddressCorruptor.ABBREVIATIONS.items():
            for abbrev in abbrevs:
                # Use word boundaries
                import re
                pattern = r'\b' + re.escape(abbrev) + r'\b'
                address = re.sub(pattern, full, address)
        return address

    @staticmethod
    def apply_typo(address: str, rate: float = 0.15) -> str:
        """Introduce typos at specified rate."""
        words = address.split()
        corrupted = []

        for word in words:
            if random.random() < rate and len(word) > 2:
                word = NameCorruptor.apply_typo(word)  # Reuse typo logic
            corrupted.append(word)

        return ' '.join(corrupted)

    @staticmethod
    def reorder_components(address: str) -> str:
        """Reorder address components (e.g., move apartment number)."""
        # Simple implementation: swap first two components
        parts = address.split(',')
        if len(parts) >= 2:
            return f"{parts[1].strip()}, {parts[0].strip()}"
        return address


class DataCorruptor:
    """Main corruption orchestrator."""

    def __init__(self, corruption_rate: float = 0.15):
        self.corruption_rate = corruption_rate
        self.name_corruptor = NameCorruptor()
        self.address_corruptor = AddressCorruptor()

    def corrupt_record(self, record: pd.Series) -> pd.Series:
        """Apply corruption to single record."""
        corrupted = record.copy()

        # Name corruption (only if field exists)
        if 'name' in corrupted and pd.notna(corrupted['name']):
            if random.random() < self.corruption_rate:
                corruption_type = random.choice([
                    'nickname', 'swap', 'middle_initial', 'typo'
                ])

                if corruption_type == 'nickname':
                    corrupted['name'] = self.name_corruptor.apply_nickname(record['name'])
                elif corruption_type == 'swap':
                    corrupted['name'] = self.name_corruptor.swap_first_last(record['name'])
                elif corruption_type == 'middle_initial':
                    corrupted['name'] = self.name_corruptor.add_middle_initial(record['name'])
                elif corruption_type == 'typo':
                    corrupted['name'] = self.name_corruptor.apply_typo(record['name'])

        # Address corruption (only if field exists)
        if 'address' in corrupted and pd.notna(corrupted['address']):
            if random.random() < self.corruption_rate:
                corruption_type = random.choice([
                    'abbreviate', 'expand', 'typo', 'reorder'
                ])

                if corruption_type == 'abbreviate':
                    corrupted['address'] = self.address_corruptor.abbreviate(record['address'])
                elif corruption_type == 'expand':
                    corrupted['address'] = self.address_corruptor.expand_abbreviations(record['address'])
                elif corruption_type == 'typo':
                    corrupted['address'] = self.address_corruptor.apply_typo(record['address'])
                elif corruption_type == 'reorder':
                    corrupted['address'] = self.address_corruptor.reorder_components(record['address'])

        return corrupted

    def expand_dataset(self, df: pd.DataFrame, target_size: int) -> pd.DataFrame:
        """Expand dataset by creating corrupted variants."""
        if len(df) == 0 or target_size == 0:  # Add this check
            return df

        expansion_factor = target_size // len(df)

        expanded_records = []
        record_counter = 0

        for idx, row in df.iterrows():
            # Keep original
            original = row.copy()
            original['cluster_id'] = row['id']  # Track original entity
            expanded_records.append(original)
            record_counter += 1

            # Generate corrupted variants
            for variant_num in range(1, expansion_factor):
                corrupted = self.corrupt_record(row)
                corrupted['id'] = f"{row['id']}_var{variant_num}"
                corrupted['cluster_id'] = row['id']
                expanded_records.append(corrupted)
                record_counter += 1

                if record_counter >= target_size:
                    break

            if record_counter >= target_size:
                break

        expanded_df = pd.DataFrame(expanded_records)
        return expanded_df[:target_size]


class PairGenerator:
    """Generate positive and negative training pairs."""

    @staticmethod
    def generate_positive_pairs(df: pd.DataFrame, n_pairs: int) -> pd.DataFrame:
        """Generate positive pairs (same cluster_id)."""
        pairs = []

        # Validate required fields exist
        required_fields = ['id', 'cluster_id', 'name', 'address']
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        if len(df) == 0:
            return pd.DataFrame(columns=['id1', 'id2', 'name1', 'name2', 'address1', 'address2', 'label'])

        # Group by cluster_id
        grouped = df.groupby('cluster_id')

        if len(grouped) == 0:  # Add this line
            return pd.DataFrame(columns=['id1', 'id2', 'name1', 'name2', 'address1', 'address2', 'label'])

        pairs_per_cluster = n_pairs // len(grouped)

        for cluster_id, group in grouped:
            if len(group) < 2:
                continue

            # Sample pairs within cluster
            for _ in range(min(pairs_per_cluster, len(group) * (len(group) - 1) // 2)):
                sample = group.sample(2)
                pairs.append({
                    'id1': sample.iloc[0]['id'],
                    'id2': sample.iloc[1]['id'],
                    'name1': sample.iloc[0]['name'],
                    'name2': sample.iloc[1]['name'],
                    'address1': sample.iloc[0]['address'],
                    'address2': sample.iloc[1]['address'],
                    'label': 1
                })

        return pd.DataFrame(pairs[:n_pairs])

    @staticmethod
    def generate_negative_pairs(df: pd.DataFrame, n_pairs: int) -> pd.DataFrame:
        """Generate negative pairs (different cluster_id)."""
        pairs = []
        unique_clusters = df['cluster_id'].unique()

        if len(unique_clusters) < 2:
            # Return empty dataframe instead of error
            return pd.DataFrame(columns=['id1', 'id2', 'name1', 'name2', 'address1', 'address2', 'label'])

        # Validate required fields exist
        required_fields = ['id', 'cluster_id', 'name', 'address']
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        unique_clusters = df['cluster_id'].unique()

        if len(unique_clusters) < 2:
            raise ValueError("Need at least 2 unique clusters to generate negative pairs")

        for _ in range(n_pairs):
            # Sample two different clusters
            cluster1, cluster2 = random.sample(list(unique_clusters), 2)

            record1 = df[df['cluster_id'] == cluster1].sample(1).iloc[0]
            record2 = df[df['cluster_id'] == cluster2].sample(1).iloc[0]

            pairs.append({
                'id1': record1['id'],
                'id2': record2['id'],
                'name1': record1['name'],
                'name2': record2['name'],  # Fixed: was record1['name']
                'address1': record1['address'],
                'address2': record2['address'],
                'label': 0
            })

        return pd.DataFrame(pairs)

    @staticmethod
    def generate_pairs(df: pd.DataFrame, n_positive: int, n_negative: int) -> pd.DataFrame:
        """Generate balanced positive and negative pairs."""
        positive_pairs = PairGenerator.generate_positive_pairs(df, n_positive)
        negative_pairs = PairGenerator.generate_negative_pairs(df, n_negative)

        all_pairs = pd.concat([positive_pairs, negative_pairs], ignore_index=True)

        # Return what all, even if less than requested
        return all_pairs.sample(frac=1, random_state=42) if len(all_pairs) > 0 else all_pairs


def preprocess_dataset(df: pd.DataFrame, config: Dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Main preprocessing pipeline."""
    print(f"[Preprocessing] Starting with {len(df)} base records")

    # Step 1: Normalize values FIRST (before corruption)
    print(f"[Preprocessing] Normalizing values...")
    normalizer = DataNormalizer()
    df = df.apply(normalizer.normalize_record, axis=1)

    # Step 2: Expand dataset with corruption
    target_size = config['target_records']
    corruption_rate = config['corruption']['rate']

    corruptor = DataCorruptor(corruption_rate)
    expanded_df = corruptor.expand_dataset(df, target_size)

    print(f"[Preprocessing] Expanded to {len(expanded_df)} records")

    # Step 3: Generate pairs
    pair_config = config['pairs']
    pairs_df = PairGenerator.generate_pairs(
        expanded_df,
        pair_config['positive'],
        pair_config['negative']
    )

    print(f"[Preprocessing] Generated {len(pairs_df)} pairs")
    print(f"  - Positive: {pair_config['positive']}")
    print(f"  - Negative: {pair_config['negative']}")

    return expanded_df, pairs_df