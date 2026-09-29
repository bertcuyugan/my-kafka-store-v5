"""Shared settings for the data lake services (lake_writer + lake_uploader).

Everything is read from .env so you can switch destinations without
touching code.
"""
import os
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# ── Kafka ─────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
TOPICS = [
    'order-placed',
    'payment-received',
    'item-shipped',
    'item-delivered',
]

# ── Local folders ─────────────────────────────────────────────────
# landing/ = files waiting to be uploaded
# archive/ = files already uploaded (local backup, safe to delete later)
DATA_LAKE_ROOT = os.path.join(PROJECT_ROOT, 'data-lake')
LANDING_DIR = os.path.join(DATA_LAKE_ROOT, 'landing')
ARCHIVE_DIR = os.path.join(DATA_LAKE_ROOT, 'archive')

# ── Lake writer batching ──────────────────────────────────────────
FLUSH_INTERVAL_SECONDS = int(os.getenv('LAKE_FLUSH_INTERVAL_SECONDS', '30'))
FLUSH_BATCH_SIZE = int(os.getenv('LAKE_FLUSH_BATCH_SIZE', '50'))

# ── Uploader ──────────────────────────────────────────────────────
UPLOAD_INTERVAL_SECONDS = int(os.getenv('LAKE_UPLOAD_INTERVAL_SECONDS', '15'))

# Which destinations to upload to: "volume", "adls", or "volume,adls"
LAKE_SINKS = [s.strip().lower()
              for s in os.getenv('LAKE_SINKS', 'volume').split(',')
              if s.strip()]

# Every remote file lands under <root>/raw/<topic>/<date>/<file>.parquet
REMOTE_RAW_PREFIX = 'raw'

# Databricks Free Edition (Unity Catalog volume)
DATABRICKS_VOLUME_PATH = os.getenv(
    'DATABRICKS_VOLUME_PATH', '/Volumes/workspace/kafka_store/landing')

# Azure Data Lake Storage Gen2
AZURE_STORAGE_ACCOUNT = os.getenv('AZURE_STORAGE_ACCOUNT', '')
AZURE_STORAGE_CONTAINER = os.getenv('AZURE_STORAGE_CONTAINER', 'landing')
AZURE_STORAGE_SAS_TOKEN = os.getenv('AZURE_STORAGE_SAS_TOKEN', '')
AZURE_STORAGE_ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY', '')