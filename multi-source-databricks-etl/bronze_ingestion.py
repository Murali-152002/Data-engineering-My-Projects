"""
bronze_ingestion.py
--------------------
Bronze layer: land raw, untransformed data from both sources with minimal
processing beyond adding ingestion metadata. Bronze is intentionally "dumb" -
it should be possible to fully rebuild Silver/Gold from Bronze alone, which
is why we keep the raw string values here rather than casting types yet
(that happens in Silver, where bad type coercions can be logged/handled).

Sources landed here:
  1. Daily CSV drops (flat-file source)      -> bronze_sales_transactions (append-only)
  2. Operational SQLite tables (rel. source) -> bronze_customers, bronze_products (snapshot overwrite)

In production on Azure Databricks, step 1 would read from ADLS Gen2 via
Auto Loader (cloudFiles) so new files are picked up incrementally without
re-scanning the whole landing zone, and step 2 would read from Azure SQL
via a JDBC connection (credentials pulled from a Key Vault-backed secret
scope). Both are called out in comments below at the point they'd change.
"""

import glob
import os
import sqlite3

import pandas as pd
from pyspark.sql import functions as F

from config import BRONZE_DIR, LANDING_DIR, OPERATIONAL_DB_PATH
from delta_io import delta_table_exists, read_delta, write_delta
from pipeline_utils import get_logger, retry_on_failure, send_pipeline_alert


@retry_on_failure()
def _read_csv_batch(spark, files):
    """Reads a batch of landing CSVs. Wrapped in retry since a transient
    storage read failure (e.g. blob still mid-write) shouldn't kill the run."""
    if not files:
        return None
    return (
        spark.read.option("header", True).csv(files)
        # Keep everything as string in Bronze - type casting belongs in Silver.
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_sales_csvs(spark, logger):
    """
    Incrementally ingests new CSV files from the landing zone into
    bronze_sales_transactions. 'Incremental' here means: only files not
    already recorded in the Bronze table's _source_file column get
    processed, so re-running the pipeline on the same day is idempotent
    and we don't reprocess months of history on every run.

    Production equivalent: Databricks Auto Loader (cloudFiles format) with
    a checkpoint location tracks exactly this "which files have I already
    seen" state automatically.
    """
    all_files = sorted(glob.glob(os.path.join(LANDING_DIR, "*.csv")))
    if not all_files:
        logger.warning("No CSV files found in landing zone.")
        return

    bronze_table_path = os.path.join(BRONZE_DIR, "bronze_sales_transactions")
    already_ingested = set()

    if delta_table_exists(bronze_table_path):
        existing = read_delta(spark, bronze_table_path)
        already_ingested = {
            os.path.basename(r["_source_file"])
            for r in existing.select("_source_file").distinct().collect()
        }

    new_files = [f for f in all_files if os.path.basename(f) not in already_ingested]
    if not new_files:
        logger.info("No new sales CSV files to ingest - bronze_sales_transactions is up to date.")
        return

    logger.info(f"Ingesting {len(new_files)} new sales file(s): {[os.path.basename(f) for f in new_files]}")

    try:
        df = _read_csv_batch(spark, new_files)
    except Exception as exc:
        send_pipeline_alert(
            subject="Bronze ingestion failed: sales CSVs",
            message=f"Failed to read {len(new_files)} file(s) after retries: {exc}",
            logger=logger,
        )
        raise

    row_count = df.count()
    write_delta(df, bronze_table_path, mode="append")
    logger.info(f"bronze_sales_transactions: appended {row_count} rows from {len(new_files)} file(s).")


@retry_on_failure()
def _read_operational_table(table_name):
    """Pulls a full snapshot of a reference/dimension table from the
    operational source. Wrapped in retry since a locked/mid-write SQLite
    file (or, in production, a brief Azure SQL connection blip) is a
    transient condition worth retrying rather than failing the whole run."""
    conn = sqlite3.connect(OPERATIONAL_DB_PATH)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()


def ingest_operational_tables(spark, logger):
    """
    Snapshots the operational customers/products tables into Bronze.
    Unlike the append-only sales fact data, these are low-volume reference
    tables where a full overwrite each run is simpler and cheap - there's
    no meaningful "incremental" concept for a ~500-row dimension source.

    Production equivalent: JDBC read against Azure SQL:
        spark.read.jdbc(url=jdbc_url, table="customers", properties=conn_props)
    with the connection string resolved from a Key Vault-backed secret scope.
    """
    for table_name in ("customers", "products"):
        try:
            pdf = _read_operational_table(table_name)
        except Exception as exc:
            send_pipeline_alert(
                subject=f"Bronze ingestion failed: operational.{table_name}",
                message=f"Failed to read '{table_name}' from operational source after retries: {exc}",
                logger=logger,
            )
            raise

        sdf = spark.createDataFrame(pdf).withColumn("_ingested_at", F.current_timestamp())
        target_path = os.path.join(BRONZE_DIR, f"bronze_{table_name}")
        write_delta(sdf, target_path, mode="overwrite")
        logger.info(f"bronze_{table_name}: refreshed snapshot, {sdf.count()} rows.")


def run_bronze(spark, logger=None):
    logger = logger or get_logger("bronze_ingestion")[0]
    logger.info("=== Bronze layer: starting ===")
    ingest_sales_csvs(spark, logger)
    ingest_operational_tables(spark, logger)
    logger.info("=== Bronze layer: complete ===")


if __name__ == "__main__":
    from pipeline_utils import get_spark

    spark = get_spark()
    run_bronze(spark)
