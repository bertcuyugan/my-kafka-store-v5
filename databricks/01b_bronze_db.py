# Databricks notebook: 01b_bronze_db
# Loads raw PostgreSQL snapshots (written by services/db_snapshot.py) into
# bronze_db_<table> Delta tables with Auto Loader. No business logic here (ELT).

# ── Cell 1: Parameters ────────────────────────────────────────────
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "kafka_store")
dbutils.widgets.text("landing_root", "")   # blank = the Free Edition volume
dbutils.widgets.text("state_root", "")     # blank = the pipeline_state volume

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing_root = (dbutils.widgets.get("landing_root")
                or f"/Volumes/{catalog}/{schema}/landing/raw").rstrip("/")
state_root = (dbutils.widgets.get("state_root")
              or f"/Volumes/{catalog}/{schema}/pipeline_state").rstrip("/")

print(f"Reading from : {landing_root}/db/<table>/")
print(f"State folder : {state_root}")
print(f"Writing to   : {catalog}.{schema}.bronze_db_*")

# ── Cell 2: File contracts (must match the SELECTs in db_snapshot.py) ──
from pyspark.sql import functions as F

TABLE_SCHEMAS = {
    "users":     "id BIGINT, name STRING, email STRING, created_at STRING, _extracted_at STRING",
    "products":  "id BIGINT, name STRING, description STRING, price DOUBLE, stock BIGINT, "
                 "image_url STRING, category STRING, created_at STRING, _extracted_at STRING",
    "orders":    "id BIGINT, order_number STRING, user_id BIGINT, product_id BIGINT, quantity BIGINT, "
                 "total_price DOUBLE, status STRING, created_at STRING, updated_at STRING, _extracted_at STRING",
    "payments":  "id BIGINT, order_id BIGINT, amount DOUBLE, status STRING, stripe_payment_id STRING, "
                 "created_at STRING, _extracted_at STRING",
    "shipments": "id BIGINT, order_id BIGINT, tracking_number STRING, status STRING, shipped_at STRING, "
                 "delivered_at STRING, created_at STRING, _extracted_at STRING",
}

# ── Cell 3: Load each table ───────────────────────────────────────
def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


for table, ddl in TABLE_SCHEMAS.items():
    source = f"{landing_root}/db/{table}/"
    target = f"`{catalog}`.`{schema}`.bronze_db_{table}"

    if not path_exists(source):
        print(f"⏭️  {table:<10}: no snapshots uploaded yet — skipping")
        continue

    query = (
        spark.readStream
        .format("cloudFiles")                          # Auto Loader
        .option("cloudFiles.format", "parquet")
        .schema(ddl)
        .load(source)                                  # includes date sub-folders
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_loaded_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{state_root}/checkpoints/bronze_db_{table}")
        .trigger(availableNow=True)                    # process new files, then stop
        .toTable(target)
    )
    query.awaitTermination()

    total = spark.table(target).count()
    print(f"✅ {table:<10} → bronze_db_{table:<10} ({total} rows total, all snapshots)")
