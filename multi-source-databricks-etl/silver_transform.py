"""
silver_transform.py
--------------------
Silver layer: clean, deduplicate, and standardize Bronze data into a
trustworthy, typed dataset. This is where the "messy real-world data"
problems get handled explicitly and loudly (logged, not silently dropped)
rather than causing subtle bugs three layers downstream.

Design decision worth calling out: Silver is fully recomputed from Bronze
on every run (not incremental) since Bronze itself is the immutable
source of truth and the data volumes here are small (a few hundred
thousand rows/day). At larger scale you'd make this incremental too
(e.g. only reprocess new Bronze partitions), but recomputing from an
already-landed Bronze table is cheap and removes an entire class of bugs
where Silver and Bronze drift out of sync.
"""

import os

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType

from config import BRONZE_DIR, SILVER_DIR
from delta_io import delta_table_exists, read_delta, write_delta
from pipeline_utils import get_logger


def transform_sales_transactions(spark, logger):
    bronze_path = os.path.join(BRONZE_DIR, "bronze_sales_transactions")
    if not delta_table_exists(bronze_path):
        logger.warning("bronze_sales_transactions does not exist yet - skipping.")
        return

    df = read_delta(spark, bronze_path)
    raw_count = df.count()

    # 1. Drop rows with a null/empty CustomerID - these can't be joined to a
    #    customer dimension and would otherwise silently corrupt Gold-layer
    #    aggregations. Count and log them rather than dropping silently.
    bad_customer = df.filter((F.col("CustomerID").isNull()) | (F.trim(F.col("CustomerID")) == ""))
    bad_customer_count = bad_customer.count()
    df = df.filter((F.col("CustomerID").isNotNull()) & (F.trim(F.col("CustomerID")) != ""))

    # 2. Deduplicate on TransactionID, keeping the most recently ingested
    #    record if the same transaction ID appears more than once (source
    #    system resend, landing-zone retry, etc.)
    from pyspark.sql.window import Window
    dedup_window = Window.partitionBy("TransactionID").orderBy(F.col("_ingested_at").desc())
    df_ranked = df.withColumn("_rn", F.row_number().over(dedup_window))
    dupes_count = df_ranked.filter(F.col("_rn") > 1).count()
    df = df_ranked.filter(F.col("_rn") == 1).drop("_rn")

    # 3. Type coercion - Bronze kept everything as string. Quantity sometimes
    #    lands as "3.0" instead of "3" (upstream export quirk) - cast through
    #    double first so both forms parse cleanly instead of erroring out.
    #    TransactionID is generated as YYYYMMDD * 10000 + sequence (see
    #    data_generator.py), which comfortably exceeds int32 range - it has
    #    to be a LongType or it silently overflows to null, which then
    #    breaks every downstream MERGE match on this key.
    df = (
        df.withColumn("TransactionID", F.col("TransactionID").cast(LongType()))
        .withColumn("CustomerID", F.col("CustomerID").cast(IntegerType()))
        .withColumn("ProductID", F.col("ProductID").cast(IntegerType()))
        .withColumn("OrderDate", F.to_date("OrderDate", "yyyy-MM-dd"))
        .withColumn("Quantity", F.col("Quantity").cast(DoubleType()).cast(IntegerType()))
        .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
        .withColumn("Region", F.trim(F.col("Region")))
    )

    write_delta(df, os.path.join(SILVER_DIR, "silver_sales_transactions"), mode="overwrite")

    logger.info(
        f"silver_sales_transactions: {raw_count} raw -> {df.count()} clean rows "
        f"(dropped {bad_customer_count} null-CustomerID, {dupes_count} duplicate TransactionID)."
    )


def transform_dimension_tables(spark, logger):
    """Light cleaning for the reference tables: trim strings, drop exact dupes."""
    for name in ("customers", "products"):
        bronze_path = os.path.join(BRONZE_DIR, f"bronze_{name}")
        if not delta_table_exists(bronze_path):
            logger.warning(f"bronze_{name} does not exist yet - skipping.")
            continue

        df = read_delta(spark, bronze_path).dropDuplicates()
        string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
        for c in string_cols:
            df = df.withColumn(c, F.trim(F.col(c)))

        write_delta(df, os.path.join(SILVER_DIR, f"silver_{name}"), mode="overwrite")
        logger.info(f"silver_{name}: {df.count()} rows.")


def run_silver(spark, logger=None):
    logger = logger or get_logger("silver_transform")[0]
    logger.info("=== Silver layer: starting ===")
    transform_sales_transactions(spark, logger)
    transform_dimension_tables(spark, logger)
    logger.info("=== Silver layer: complete ===")


if __name__ == "__main__":
    from pipeline_utils import get_spark

    spark = get_spark()
    run_silver(spark)
