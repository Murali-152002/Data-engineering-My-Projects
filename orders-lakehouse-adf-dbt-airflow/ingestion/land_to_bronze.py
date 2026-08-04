"""
land_to_bronze.py
------------------
Simulates the Databricks notebook/job step that would run right after the
ADF pipeline lands JSON pages in the landing zone: reads new landing files
(untouched since the last run - tracked via `_source_file`, same pattern as
the existing multi-source-databricks-etl project's bronze_ingestion.py) and
appends them as raw, minimally-typed rows into a Bronze Delta table.

Bronze stays "dumb" on purpose - JSON values land as strings, no cleaning,
so Silver/Gold (built entirely in dbt for this project - see dbt_project/)
can always be rebuilt from Bronze alone.
"""
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delta_io import delta_table_exists, read_delta, write_delta

HERE = os.path.dirname(__file__)
LANDING_DIR = os.path.join(HERE, "..", "landing", "orders")
BRONZE_DIR = os.path.join(HERE, "..", "warehouse", "bronze_orders")


def get_spark():
    return (
        SparkSession.builder.appName("land_to_bronze")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _already_ingested_files(spark):
    if not delta_table_exists(BRONZE_DIR):
        return set()
    df = read_delta(spark, BRONZE_DIR)
    return {row["_source_file"] for row in df.select("_source_file").distinct().collect()}


def ingest_new_landing_files():
    spark = get_spark()
    all_files = sorted(glob.glob(os.path.join(LANDING_DIR, "run_date=*", "*.json")))
    seen = _already_ingested_files(spark)
    new_files = [f for f in all_files if os.path.abspath(f) not in seen]

    if not new_files:
        print("No new landing files to ingest.")
        return 0

    df = (
        spark.read.option("multiline", "true").json(new_files)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    # input_file_name() returns a file:// URI - normalize to match the
    # plain abspath strings used in `seen` above.
    df = df.withColumn(
        "_source_file",
        F.regexp_replace(F.col("_source_file"), "^file:/+", "/"),
    )

    row_count = df.count()
    if row_count == 0:
        # Some pages can legitimately be empty (e.g. the tail page of a
        # pagination loop, or a re-run that lands right on the watermark
        # boundary) - an empty JSON array contributes 0 rows, so it never
        # gets a chance to register in _already_ingested_files via
        # distinct(). Skip the write (nothing to append, and appending an
        # empty frame would also mismatch the existing table's schema
        # since no real columns get inferred from zero rows) rather than
        # erroring - this is a normal, expected case, not a failure.
        print(f"{len(new_files)} new file(s) found but they contain 0 rows (empty pages) - nothing to append.")
        return 0
    write_delta(df, BRONZE_DIR, mode="append")
    print(f"Ingested {row_count} rows from {len(new_files)} new file(s) into bronze_orders.")
    return row_count


if __name__ == "__main__":
    ingest_new_landing_files()
