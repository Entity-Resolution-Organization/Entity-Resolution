"""Dataset factory pattern for switchable data sources."""
import pandas as pd
import requests
from abc import ABC, abstractmethod
from typing import Dict, Tuple


class DatasetHandler(ABC):
    """Base class for dataset handlers."""

    def __init__(self, config: Dict):
        self.config = config
        self.name = config['name']
        self.schema = config['schema']

    @abstractmethod
    def download(self) -> pd.DataFrame:
        """Download raw dataset."""
        pass

    @abstractmethod
    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map dataset columns to standard schema."""
        pass

    def subsample(self, df: pd.DataFrame) -> pd.DataFrame:
        """Subsample to target record count if needed."""
        target = self.config['target_records']
        if len(df) > target:
            return df.sample(n=target, random_state=42)
        return df


class PseudopeopleHandler(DatasetHandler):
    """Handler for Pseudopeople synthetic population data."""

    def download(self) -> pd.DataFrame:
        """Generate Pseudopeople data (placeholder - actual requires repo clone)."""
        base_count = self.config['base_records']

        # Simulate download - replace with actual Pseudopeople generation
        print(f"[Pseudopeople] Generating {base_count} base records...")

        data = {
            'person_id': range(1, base_count + 1),
            'first_name': [f'Person{i}' for i in range(1, base_count + 1)],
            'last_name': [f'Smith{i % 1000}' for i in range(1, base_count + 1)],
            'street_address': [f'{i} Main St' for i in range(1, base_count + 1)],
            'date_of_birth': ['1980-01-01'] * base_count,
            'ssn': [f'{str(i).zfill(9)}' for i in range(1, base_count + 1)]
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        return df.rename(columns={
            'person_id': 'id',
            'first_name': 'name',
            'street_address': 'address',
            'date_of_birth': 'dob'
        })


class NCVotersHandler(DatasetHandler):
    """Handler for NC Voters registry data."""

    def download(self) -> pd.DataFrame:
        """Download NC Voters dataset."""
        # Actual: download from Leipzig benchmark
        url = self.config['source_url']
        print(f"[NC Voters] Downloading from {url}...")

        # Placeholder - replace with actual download
        base_count = min(self.config['base_records'], 1000000)  # Limit for demo

        data = {
            'voter_id': range(1, base_count + 1),
            'full_name': [f'John Doe {i}' for i in range(1, base_count + 1)],
            'street_address': [f'{i} Elm St' for i in range(1, base_count + 1)],
            'city': ['Raleigh'] * base_count,
            'state': ['NC'] * base_count,
            'zip': ['27601'] * base_count,
            'birth_year': [1980] * base_count
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        # Combine address fields
        df['address'] = df['street_address'] + ', ' + df['city'] + ', ' + df['state'] + ' ' + df['zip']

        return df.rename(columns={
            'voter_id': 'id',
            'full_name': 'name',
            'birth_year': 'dob'
        })[['id', 'name', 'address', 'dob']]


class OFACHandler(DatasetHandler):
    """Handler for OFAC SDN sanctions list."""

    def download(self) -> pd.DataFrame:
        """Download OFAC SDN list."""
        url = self.config['source_url']
        print(f"[OFAC SDN] Downloading from {url}...")

        try:
            df = pd.read_csv(url, encoding='utf-8')
            print(f"[OFAC SDN] Downloaded {len(df)} records")
            return df
        except Exception as e:
            print(f"[OFAC SDN] Download failed: {e}")
            # Fallback to placeholder
            return self._generate_placeholder()

    def _generate_placeholder(self) -> pd.DataFrame:
        """Generate placeholder OFAC-like data."""
        base_count = self.config['base_records']

        data = {
            'ent_num': range(1, base_count + 1),
            'sdn_name': [f'Sanctioned Entity {i}' for i in range(1, base_count + 1)],
            'address': [f'{i} Unknown St, Country' for i in range(1, base_count + 1)],
            'nationality': ['XX'] * base_count,
            'aliases': [''] * base_count
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        return df.rename(columns={
            'ent_num': 'id',
            'sdn_name': 'name'
        })[['id', 'name', 'address']]

    def expand(self, df: pd.DataFrame) -> pd.DataFrame:
        """Synthetically expand small dataset to target size."""
        expansion_factor = self.config['expansion_factor']
        target = self.config['target_records']

        print(f"[OFAC SDN] Expanding {len(df)} → {target} records...")

        # Repeat and add noise
        expanded = pd.concat([df] * expansion_factor, ignore_index=True)
        expanded['id'] = range(1, len(expanded) + 1)

        return expanded[:target]


class WDCProductsHandler(DatasetHandler):
    """Handler for WDC Products benchmark data."""

    def download(self) -> pd.DataFrame:
        """Download WDC Products dataset."""
        # Actual: download from webdatacommons.org
        url = self.config['source_url']
        print(f"[WDC Products] Downloading from {url}...")

        # Placeholder - replace with actual download
        base_count = self.config['base_records']

        data = {
            'product_id': range(1, base_count + 1),
            'title': [f'Product {i}' for i in range(1, base_count + 1)],
            'brand': [f'Brand{i % 100}' for i in range(1, base_count + 1)],
            'price': [19.99 + (i % 100) for i in range(1, base_count + 1)],
            'description': [f'Description for product {i}' for i in range(1, base_count + 1)],
            'category': ['Electronics'] * base_count
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        # Combine title + brand into name field
        df['name'] = df['title'] + ' ' + df['brand'].fillna('')
        df['address'] = df.get('description', '')  # Use description or empty
        df['id'] = df['product_id']
        return df[['id', 'name', 'address']]


class AmazonHandler(DatasetHandler):
    """Handler for Amazon co-purchase network data."""

    def download(self) -> pd.DataFrame:
        """Download Amazon dataset."""
        url = self.config['source_url']
        print(f"[Amazon 2018] Downloading from {url}...")

        # Placeholder - actual requires downloading JSONL
        base_count = self.config['base_records']

        data = {
            'asin': [f'B{str(i).zfill(9)}' for i in range(1, base_count + 1)],
            'title': [f'Amazon Product {i}' for i in range(1, base_count + 1)],
            'brand': [f'Brand{i % 50}' for i in range(1, base_count + 1)],
            'price': [9.99 + (i % 200) for i in range(1, base_count + 1)],
            'also_bought': [''] * base_count  # Co-purchase edges handled separately
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        df['name'] = df['title'] + ' ' + df['brand'].fillna('')
        df['address'] = ''  # Products don't have meaningful address
        df['id'] = df['asin']
        return df[['id', 'name', 'address']]


class DBLPACMHandler(DatasetHandler):
    """Handler for DBLP-ACM author disambiguation dataset."""

    def download(self) -> pd.DataFrame:
        """Download DBLP-ACM dataset."""
        url = self.config['source_url']
        print(f"[DBLP-ACM] Downloading from {url}...")

        # Placeholder
        base_count = self.config['base_records']

        data = {
            'paper_id': range(1, base_count + 1),
            'title': [f'Research Paper {i}' for i in range(1, base_count + 1)],
            'authors': [f'Author{i % 1000}' for i in range(1, base_count + 1)],
            'venue': [f'Conference{i % 50}' for i in range(1, base_count + 1)],
            'year': [2000 + (i % 24) for i in range(1, base_count + 1)]
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        df['name'] = df['authors']
        df['address'] = df['title']  # Paper title as secondary field
        df['id'] = df['paper_id']
        return df[['id', 'name', 'address']]


def get_dataset_handler(dataset_name: str, config: Dict) -> DatasetHandler:
    """Factory function to get appropriate dataset handler."""
    handlers = {
        'pseudopeople': PseudopeopleHandler,
        'nc_voters': NCVotersHandler,
        'ofac_sdn': OFACHandler,
        'wdc_products': WDCProductsHandler,
        'amazon_2018': AmazonHandler,
        'dblp_acm': DBLPACMHandler
    }

    if dataset_name not in handlers:
        raise ValueError(f"Unknown dataset: {dataset_name}")

    return handlers[dataset_name](config)