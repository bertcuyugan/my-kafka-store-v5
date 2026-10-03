"""Run dbt with the settings in .env.

Usage (from the project root, .venv active):
    python run_dbt.py debug
    python run_dbt.py build
    python run_dbt.py source freshness
    python run_dbt.py docs generate
    python run_dbt.py docs serve --port 8081
"""
import os
import sys

from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DBT_DIR = os.path.join(PROJECT_ROOT, 'dbt_kafka_store')

load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

for var in ('DATABRICKS_HOST', 'DATABRICKS_TOKEN', 'DATABRICKS_HTTP_PATH'):
    if not os.getenv(var):
        sys.exit(f"❌ {var} is missing from .env")

from dbt.cli.main import dbtRunner  # imported after .env is loaded

args = sys.argv[1:] or ['build']
args += ['--project-dir', DBT_DIR, '--profiles-dir', DBT_DIR]

result = dbtRunner().invoke(args)
sys.exit(0 if result.success else 1)