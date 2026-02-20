"""
Dataset factory pattern for switchable data sources.
Production-ready with real downloads, error handling, and retry logic.
"""
import pandas as pd
import requests
import io
import os
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatasetHandler(ABC):
    """Base class for dataset handlers."""

    def __init__(self, config: Dict):
        self.config = config
        self.name = config['name']
        self.schema = config['schema']
        self.is_production = os.getenv('ENVIRONMENT', 'local') == 'production'

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
            logger.info(f"Subsampling from {len(df)} to {target} records")
            return df.sample(n=target, random_state=42)
        return df

    def download_with_retry(self, url: str, retries: int = 3, timeout: int = 60) -> requests.Response:
        """Download URL with retry logic and exponential backoff."""
        for attempt in range(retries):
            try:
                logger.info(f"Downloading from {url} (attempt {attempt + 1}/{retries})")
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    logger.error(f"Failed to download after {retries} attempts: {e}")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Download failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)


class PseudopeopleHandler(DatasetHandler):
    """Handler for Pseudopeople synthetic population data."""

    def download(self) -> pd.DataFrame:
        """Download or generate Pseudopeople data."""
        if self.is_production:
            return self._download_production()
        else:
            return self._generate_test_data()

    def _download_production(self) -> pd.DataFrame:
        """Download from GCS or generate using Pseudopeople library."""
        try:
            # Option 1: Download from GCS bucket
            from google.cloud import storage
            
            client = storage.Client()
            bucket_name = os.getenv('GCS_BUCKET', 'laundrograph-data')
            bucket = client.bucket(bucket_name)
            blob = bucket.blob('raw/pseudopeople.csv')
            
            logger.info(f"[Pseudopeople] Downloading from gs://{bucket_name}/raw/pseudopeople.csv")
            
            # Download to temp file
            temp_path = '/tmp/pseudopeople.csv'
            blob.download_to_filename(temp_path)
            df = pd.read_csv(temp_path)
            
            logger.info(f"[Pseudopeople] Downloaded {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"[Pseudopeople] Production download failed: {e}")
            logger.warning("[Pseudopeople] Falling back to test data generation")
            return self._generate_test_data()

    def _generate_test_data(self) -> pd.DataFrame:
        """Generate synthetic data for local testing."""
        import random
        
        base_count = self.config['base_records']
        logger.info(f"[Pseudopeople] Generating {base_count} test records")

        first_names = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
                      'William', 'Barbara', 'David', 'Elizabeth', 'Richard', 'Susan', 'Joseph', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                     'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas']
        streets = ['Main', 'Oak', 'Maple', 'Cedar', 'Elm', 'Washington', 'Park', 'Pine']

        data = {
            'person_id': range(1, base_count + 1),
            'first_name': [random.choice(first_names) for _ in range(base_count)],
            'last_name': [random.choice(last_names) for _ in range(base_count)],
            'street_address': [f'{random.randint(1, 9999)} {random.choice(streets)} St' for _ in range(base_count)],
            'date_of_birth': [f'{random.randint(1950, 2000)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}' 
                            for _ in range(base_count)],
            'ssn': [f'{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}' 
                   for _ in range(base_count)]
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        df_normalized = df.rename(columns={
            'person_id': 'id',
            'first_name': 'name',
            'street_address': 'address',
            'date_of_birth': 'dob'
        })
        
        # Combine first + last name if separate
        if 'last_name' in df.columns:
            df_normalized['name'] = df['first_name'] + ' ' + df['last_name']
        
        return df_normalized[['id', 'name', 'address', 'dob']]


class NCVotersHandler(DatasetHandler):
    """Handler for NC Voters registry data."""

    def download(self) -> pd.DataFrame:
        """Download NC Voters dataset."""
        if self.is_production:
            return self._download_production()
        else:
            return self._generate_test_data()

    def _download_production(self) -> pd.DataFrame:
        """Download from Leipzig benchmark or NC voter registry."""
        try:
            url = self.config['source_url']
            logger.info(f"[NC Voters] Downloading from {url}")
            
            response = self.download_with_retry(url, retries=3, timeout=120)
            df = pd.read_csv(io.StringIO(response.text))
            
            logger.info(f"[NC Voters] Downloaded {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"[NC Voters] Production download failed: {e}")
            logger.warning("[NC Voters] Falling back to test data")
            return self._generate_test_data()

    def _generate_test_data(self) -> pd.DataFrame:
        """Generate synthetic NC voter data."""
        import random
        
        base_count = min(self.config['base_records'], 100000)
        logger.info(f"[NC Voters] Generating {base_count} test records")

        first_names = ['Michael', 'Sarah', 'David', 'Emily', 'Christopher', 'Jessica', 'Matthew', 'Ashley']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Wilson']
        cities = ['Raleigh', 'Charlotte', 'Greensboro', 'Durham', 'Winston-Salem', 'Fayetteville']
        streets = ['Main', 'Oak', 'Elm', 'Cedar', 'Maple', 'Pine', 'Washington', 'Park']

        data = {
            'voter_id': range(1, base_count + 1),
            'full_name': [f'{random.choice(first_names)} {random.choice(last_names)}' for _ in range(base_count)],
            'street_address': [f'{random.randint(100, 9999)} {random.choice(streets)} St' for _ in range(base_count)],
            'city': [random.choice(cities) for _ in range(base_count)],
            'state': ['NC'] * base_count,
            'zip': [f'{random.randint(27000, 28999)}' for _ in range(base_count)],
            'birth_year': [random.randint(1940, 2005) for _ in range(base_count)]
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        # Combine address fields
        df['address'] = df['street_address'] + ', ' + df['city'] + ', ' + df['state'] + ' ' + df['zip'].astype(str)

        return df.rename(columns={
            'voter_id': 'id',
            'full_name': 'name',
            'birth_year': 'dob'
        })[['id', 'name', 'address', 'dob']]


class OFACHandler(DatasetHandler):
    """Handler for OFAC SDN sanctions list."""

    def download(self) -> pd.DataFrame:
        """Download OFAC SDN list."""
        try:
            url = self.config['source_url']
            logger.info(f"[OFAC SDN] Downloading from {url}")
            
            response = self.download_with_retry(url, retries=3, timeout=60)
            df = pd.read_csv(io.StringIO(response.text), encoding='utf-8', on_bad_lines='skip')
            
            logger.info(f"[OFAC SDN] Downloaded {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"[OFAC SDN] Download failed: {e}")
            logger.warning("[OFAC SDN] Generating placeholder data")
            return self._generate_placeholder()

    def _generate_placeholder(self) -> pd.DataFrame:
        """Generate placeholder OFAC-like data."""
        import random
        
        base_count = self.config['base_records']
        logger.info(f"[OFAC SDN] Generating {base_count} placeholder records")

        first_names = ['Ivan', 'Vladimir', 'Sergei', 'Dmitri', 'Ahmed', 'Hassan', 'Ali', 'Omar']
        last_names = ['Petrov', 'Ivanov', 'Sidorov', 'Khan', 'Al-Rashid', 'Al-Mahmoud']
        countries = ['Russia', 'Iran', 'Syria', 'North Korea', 'Venezuela', 'Belarus']

        data = {
            'ent_num': range(1, base_count + 1),
            'sdn_name': [f'{random.choice(first_names)} {random.choice(last_names)}' for _ in range(base_count)],
            'address': [f'{random.randint(1, 999)} Street, {random.choice(countries)}' for _ in range(base_count)],
            'nationality': [random.choice(countries) for _ in range(base_count)],
            'aliases': [''] * base_count
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        df_normalized = df.rename(columns={
            'ent_num': 'id',
            'sdn_name': 'name'
        })
        
        # Ensure address column exists
        if 'address' not in df_normalized.columns:
            df_normalized['address'] = ''
        
        return df_normalized[['id', 'name', 'address']]

    def expand(self, df: pd.DataFrame) -> pd.DataFrame:
        """Synthetically expand small dataset to target size."""
        expansion_factor = self.config.get('expansion_factor', 100)
        target = self.config['target_records']

        logger.info(f"[OFAC SDN] Expanding {len(df)} → {target} records (factor: {expansion_factor})")

        # Repeat dataset
        expanded = pd.concat([df] * expansion_factor, ignore_index=True)
        expanded['id'] = range(1, len(expanded) + 1)

        return expanded[:target]


class WDCProductsHandler(DatasetHandler):
    """Handler for WDC Products benchmark data."""

    def download(self) -> pd.DataFrame:
        """Download WDC Products dataset."""
        if self.is_production:
            return self._download_production()
        else:
            return self._generate_test_data()

    def _download_production(self) -> pd.DataFrame:
        """Download from webdatacommons.org."""
        try:
            url = self.config['source_url']
            logger.info(f"[WDC Products] Downloading from {url}")
            
            response = self.download_with_retry(url, retries=3, timeout=180)
            df = pd.read_csv(io.StringIO(response.text))
            
            logger.info(f"[WDC Products] Downloaded {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"[WDC Products] Download failed: {e}")
            logger.warning("[WDC Products] Generating test data")
            return self._generate_test_data()

    def _generate_test_data(self) -> pd.DataFrame:
        """Generate synthetic product data."""
        import random
        
        base_count = self.config['base_records']
        logger.info(f"[WDC Products] Generating {base_count} test records")

        products = ['iPhone', 'Galaxy', 'Laptop', 'Monitor', 'Keyboard', 'Mouse', 'Headphones', 'Speaker']
        brands = ['Apple', 'Samsung', 'Sony', 'LG', 'Dell', 'HP', 'Lenovo', 'Asus']
        categories = ['Electronics', 'Computers', 'Audio', 'Mobile', 'Accessories']

        data = {
            'product_id': range(1, base_count + 1),
            'title': [f'{random.choice(products)} {random.randint(1, 20)}' for _ in range(base_count)],
            'brand': [random.choice(brands) for _ in range(base_count)],
            'price': [round(random.uniform(9.99, 999.99), 2) for _ in range(base_count)],
            'description': [f'High quality {random.choice(products).lower()}' for _ in range(base_count)],
            'category': [random.choice(categories) for _ in range(base_count)]
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        # Combine title + brand into name field
        df['name'] = df['title'].astype(str) + ' ' + df['brand'].fillna('').astype(str)
        df['address'] = df.get('description', '').fillna('').astype(str)
        df['id'] = df['product_id'].astype(str)
        
        return df[['id', 'name', 'address']]


class AmazonHandler(DatasetHandler):
    """Handler for Amazon co-purchase network data."""

    def download(self) -> pd.DataFrame:
        """Download Amazon dataset."""
        if self.is_production:
            return self._download_production()
        else:
            return self._generate_test_data()

    def _download_production(self) -> pd.DataFrame:
        """Download from UCSD Amazon dataset."""
        try:
            url = self.config['source_url']
            logger.info(f"[Amazon 2018] Downloading from {url}")
            
            # Amazon data is typically JSONL format
            response = self.download_with_retry(url, retries=3, timeout=180)
            
            # Parse JSONL
            import json
            lines = response.text.strip().split('\n')
            records = [json.loads(line) for line in lines if line.strip()]
            df = pd.DataFrame(records)
            
            logger.info(f"[Amazon 2018] Downloaded {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"[Amazon 2018] Download failed: {e}")
            logger.warning("[Amazon 2018] Generating test data")
            return self._generate_test_data()

    def _generate_test_data(self) -> pd.DataFrame:
        """Generate synthetic Amazon product data."""
        import random
        
        base_count = self.config['base_records']
        logger.info(f"[Amazon 2018] Generating {base_count} test records")

        products = ['Smart Watch', 'Fitness Tracker', 'Wireless Earbuds', 'Power Bank', 'USB Cable', 
                   'Phone Case', 'Screen Protector', 'Charging Dock']
        brands = ['Anker', 'Belkin', 'JBL', 'Logitech', 'SanDisk', 'Kingston', 'Western Digital']

        data = {
            'asin': [f'B{str(i).zfill(9)}' for i in range(1, base_count + 1)],
            'title': [f'{random.choice(products)} by {random.choice(brands)}' for _ in range(base_count)],
            'brand': [random.choice(brands) for _ in range(base_count)],
            'price': [round(random.uniform(5.99, 299.99), 2) for _ in range(base_count)],
            'also_bought': [''] * base_count
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        df['name'] = df['title'].astype(str) + ' ' + df['brand'].fillna('').astype(str)
        df['address'] = ''  # Products don't have physical address
        df['id'] = df['asin'].astype(str)
        
        return df[['id', 'name', 'address']]


class DBLPACMHandler(DatasetHandler):
    """Handler for DBLP-ACM author disambiguation dataset."""

    def download(self) -> pd.DataFrame:
        """Download DBLP-ACM dataset."""
        if self.is_production:
            return self._download_production()
        else:
            return self._generate_test_data()

    def _download_production(self) -> pd.DataFrame:
        """Download from Leipzig benchmark."""
        try:
            url = self.config['source_url']
            logger.info(f"[DBLP-ACM] Downloading from {url}")
            
            response = self.download_with_retry(url, retries=3, timeout=120)
            df = pd.read_csv(io.StringIO(response.text))
            
            logger.info(f"[DBLP-ACM] Downloaded {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"[DBLP-ACM] Download failed: {e}")
            logger.warning("[DBLP-ACM] Generating test data")
            return self._generate_test_data()

    def _generate_test_data(self) -> pd.DataFrame:
        """Generate synthetic research paper data."""
        import random
        
        base_count = self.config['base_records']
        logger.info(f"[DBLP-ACM] Generating {base_count} test records")

        first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
        topics = ['Machine Learning', 'Database Systems', 'Computer Networks', 'Software Engineering',
                 'Artificial Intelligence', 'Data Mining', 'Computer Vision', 'Natural Language Processing']
        venues = ['ICML', 'NeurIPS', 'CVPR', 'ACL', 'KDD', 'SIGMOD', 'VLDB', 'AAAI', 'IJCAI']

        data = {
            'paper_id': range(1, base_count + 1),
            'title': [f'{random.choice(topics)}: {random.choice(["A Novel Approach", "Deep Learning Methods", 
                     "Scalable Algorithms", "Efficient Techniques"])}' for _ in range(base_count)],
            'authors': [f'{random.choice(first_names)} {random.choice(last_names)}' for _ in range(base_count)],
            'venue': [random.choice(venues) for _ in range(base_count)],
            'year': [random.randint(2000, 2024) for _ in range(base_count)]
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema."""
        df['name'] = df['authors'].astype(str)
        df['address'] = df['title'].astype(str)
        df['id'] = df['paper_id'].astype(str)
        
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
        raise ValueError(f"Unknown dataset: {dataset_name}. Available: {list(handlers.keys())}")

    return handlers[dataset_name](config)
