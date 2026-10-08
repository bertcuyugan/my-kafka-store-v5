"""DB Snapshot Extractor (V5.3 — ELT)

Copies tables from PostgreSQL into data-lake/landing/db/<table>/<date>/ as
Parquet files, UNCHANGED (no business logic — that's dbt's job).
lake_uploader.py then uploads them like any other file, and the Databricks
notebook 01b_bronze_db loads them into bronze_db_<table> tables.

  • All tables are read inside ONE repeatable-read transaction, so every
    snapshot is consistent (an order never points to a user that "doesn't
    exist yet").
  • Every row gets _extracted_at (the same value for all tables in a run).
  • Timestamps are sent as text and typed later in dbt (avoids Parquet
    timestamp-precision problems between pandas and Spark).
  • password_hash is NEVER extracted.
  • Empty tables are skipped (nothing to load yet).

Usage (project root, .venv active):
    python services/db_snapshot.py --once   # one snapshot, then exit
    python services/db_snapshot.py          # every DB_SNAPSHOT_INTERVAL_SECONDS
"""
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from fastparquet import write as write_parquet
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lake_config import LANDING_DIR   # importing lake_config also loads .env

DATABASE_URL = os.getenv('DATABASE_URL')
INTERVAL_SECONDS = int(os.getenv('DB_SNAPSHOT_INTERVAL_SECONDS', '1800'))


def ts(column):
    """Timestamp → text, e.g. '2026-10-05 01:02:03.123456' (NULL stays NULL)."""
    return f"CAST({column} AS TEXT) AS {column}"


# One SELECT per table. Column lists are explicit on purpose:
# new columns in PostgreSQL never break the Bronze schema contract.
TABLES = {
    'users': f"""
        SELECT id, name, email, {ts('created_at')}
        FROM users""",
    'products': f"""
        SELECT id, name, description, price, stock, image_url, category,
               {ts('created_at')}
        FROM products""",
    'orders': f"""
        SELECT id, order_number, user_id, product_id, quantity, total_price,
               status, {ts('created_at')}, {ts('updated_at')}
        FROM orders""",
    'payments': f"""
        SELECT id, order_id, amount, status, stripe_payment_id,
               {ts('created_at')}
        FROM payments""",
    'shipments': f"""
        SELECT id, order_id, tracking_number, status,
               {ts('shipped_at')}, {ts('delivered_at')}, {ts('created_at')}
        FROM shipments""",
}

running = True


def stop(_sig, _frame):
    global running
    print("\n⚠️  Stopping DB Snapshot Extractor...")
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def snapshot_once(engine):
    now = datetime.now(timezone.utc)
    extracted_at = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    day = now.strftime('%Y-%m-%d')
    stamp = now.strftime('%H%M%S')

    print(f"📸 Snapshot at {extracted_at} UTC")
    with engine.begin() as conn:
        # Same consistent view of the database for every table below
        conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")

        for table, sql in TABLES.items():
            df = pd.read_sql(sql, conn)
            if df.empty:
                print(f"  ⏭️  {table:<10} empty — skipped")
                continue

            df['_extracted_at'] = extracted_at

            folder = os.path.join(LANDING_DIR, 'db', table, day)
            os.makedirs(folder, exist_ok=True)
            name = f"snapshot_{stamp}_{uuid.uuid4().hex[:8]}.parquet"
            final_path = os.path.join(folder, name)
            tmp_path = final_path + '.tmp'      # uploader ignores *.tmp

            write_parquet(tmp_path, df, object_encoding='utf8', write_index=False)
            os.replace(tmp_path, final_path)    # atomic: never a half-written file

            print(f"  💾 {table:<10} {len(df):>5} rows → db/{table}/{day}/{name}")
    print("  ✅ Snapshot complete\n")


def main():
    if not DATABASE_URL:
        sys.exit("❌ DATABASE_URL is missing from .env")

    engine = create_engine(DATABASE_URL)
    once = '--once' in sys.argv

    print("🗄️  DB Snapshot Extractor is running!")
    print(f"📁 Writing to: {os.path.join(LANDING_DIR, 'db')}")
    if not once:
        print(f"⏱️  Every {INTERVAL_SECONDS}s — press Ctrl+C to stop\n")

    while running:
        try:
            snapshot_once(engine)
        except Exception as e:
            print(f"  ❌ Snapshot failed: {e}\n")
        if once:
            break
        for _ in range(INTERVAL_SECONDS):   # 1-second steps so Ctrl+C is responsive
            if not running:
                break
            time.sleep(1)

    engine.dispose()
    print("✅ DB Snapshot Extractor shut down cleanly.")


if __name__ == '__main__':
    main()
