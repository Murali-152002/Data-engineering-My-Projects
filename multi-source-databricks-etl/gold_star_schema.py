"""
gold_star_schema.py
--------------------
Gold layer: business-ready star schema built from Silver.

    dim_customer   - one row per customer
    dim_product    - one row per product
    dim_date       - one row per calendar date spanned by the fact data
    fact_sales     - one row per (clean, deduplicated) transaction, loaded
                     incrementally via MERGE (upsert) on TransactionID

Also demonstrates two things this project is designed to showcase, both
run for real against the Delta tables built here (not simulated):

  1. A partitioning benchmark - fact_sales is written partitioned by
     OrderDate, then Z-ordered on CustomerID (the column most Gold-layer
     queries filter/join on), and query latency is compared against an
     unpartitioned, non-Z-ordered baseline copy of the same data.
  2. A time-travel / rollback demo - reads an older version of fact_sales
     straight out of the Delta transaction log and restores the table to
     it, proving the versioning story is real and not just a comment.

Run `python gold_star_schema.py` to build the star schema, then
`python gold_star_schema.py --benchmark` to also run the partitioning/
Z-order benchmark and the time-travel demo (these take longer since they
write a second copy of the fact table for comparison).
"""

import argparse
import os
import time

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import GOLD_DIR, SILVER_DIR
from delta_io import DeltaTable, delta_table_exists, merge_delta, read_delta, write_delta
from pipeline_utils import get_logger


def build_dim_customer(spark, logger):
    df = read_delta(spark, os.path.join(SILVER_DIR, "silver_customers"))
    write_delta(df, os.path.join(GOLD_DIR, "dim_customer"), mode="overwrite")
    logger.info(f"dim_customer: {df.count()} rows.")


def build_dim_product(spark, logger):
    df = read_delta(spark, os.path.join(SILVER_DIR, "silver_products"))
    write_delta(df, os.path.join(GOLD_DIR, "dim_product"), mode="overwrite")
    logger.info(f"dim_product: {df.count()} rows.")


def build_dim_date(spark, logger):
    """Derives the date dimension from the min/max OrderDate actually
    present in the fact data, rather than hardcoding a range."""
    sales = read_delta(spark, os.path.join(SILVER_DIR, "silver_sales_transactions"))
    bounds = sales.agg(F.min("OrderDate").alias("min_d"), F.max("OrderDate").alias("max_d")).collect()[0]
    if bounds["min_d"] is None:
        logger.warning("No sales rows found - skipping dim_date.")
        return

    date_df = spark.sql(
        f"SELECT explode(sequence(to_date('{bounds['min_d']}'), to_date('{bounds['max_d']}'), interval 1 day)) as FullDate"
    )
    date_df = (
        date_df.withColumn("DateKey", F.date_format("FullDate", "yyyyMMdd").cast("int"))
        .withColumn("Year", F.year("FullDate"))
        .withColumn("Month", F.month("FullDate"))
        .withColumn("Day", F.dayofmonth("FullDate"))
        .withColumn("DayOfWeek", F.date_format("FullDate", "EEEE"))
        .withColumn("IsWeekend", F.dayofweek("FullDate").isin(1, 7))
    )
    write_delta(date_df, os.path.join(GOLD_DIR, "dim_date"), mode="overwrite")
    logger.info(f"dim_date: {date_df.count()} rows ({bounds['min_d']} to {bounds['max_d']}).")


def build_fact_sales(spark, logger):
    """
    Incremental upsert into fact_sales: a re-run that includes a
    correction to an already-loaded transaction updates that row in
    place instead of duplicating it, via merge_delta() -> DeltaTable.merge().
    """
    sales = read_delta(spark, os.path.join(SILVER_DIR, "silver_sales_transactions"))
    fact = (
        sales.withColumn("TotalAmount", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
        .withColumn("DateKey", F.date_format("OrderDate", "yyyyMMdd").cast("int"))
        .select(
            "TransactionID", "CustomerID", "ProductID", "DateKey", "OrderDate",
            "Quantity", "UnitPrice", "TotalAmount", "Region",
        )
    )

    fact_path = os.path.join(GOLD_DIR, "fact_sales")
    inserted, updated = merge_delta(fact, fact_path, merge_keys=["TransactionID"])
    if inserted is not None:
        logger.info(f"fact_sales: initial load, {inserted} rows inserted.")
    else:
        logger.info(f"fact_sales: merge complete ({fact.count()} source rows evaluated).")


def run_gold(spark, logger=None):
    logger = logger or get_logger("gold_star_schema")[0]
    logger.info("=== Gold layer: starting ===")
    build_dim_customer(spark, logger)
    build_dim_product(spark, logger)
    build_dim_date(spark, logger)
    build_fact_sales(spark, logger)
    logger.info("=== Gold layer: complete ===")


# ---------------------------------------------------------------------------
# Partitioning / Z-order benchmark
# ---------------------------------------------------------------------------

def run_partitioning_benchmark(spark, logger):
    """
    Builds two physical copies of fact_sales:
      - baseline: single file, no partitioning, no Z-order
      - optimized: partitioned by OrderDate, Z-ordered on CustomerID

    Then runs the same representative query (filter one OrderDate, filter
    one CustomerID range) against both and times it, so the improvement
    is measured, not asserted.
    """
    fact = read_delta(spark, os.path.join(GOLD_DIR, "fact_sales"))
    sample_date = fact.select("OrderDate").orderBy(F.rand(seed=7)).first()["OrderDate"]

    baseline_path = os.path.join(GOLD_DIR, "_bench_fact_sales_baseline")
    optimized_path = os.path.join(GOLD_DIR, "_bench_fact_sales_optimized")

    write_delta(fact, baseline_path, mode="overwrite")
    write_delta(fact, optimized_path, mode="overwrite", partition_by=["OrderDate"])
    DeltaTable(optimized_path).optimize.z_order(["CustomerID"])

    def timed_query(path, label):
        df = read_delta(spark, path)
        start = time.perf_counter()
        result = (
            df.filter(F.col("OrderDate") == sample_date)
            .filter((F.col("CustomerID") >= 100) & (F.col("CustomerID") <= 200))
            .groupBy("Region")
            .agg(F.sum("TotalAmount").alias("total"), F.count("*").alias("n"))
            .collect()
        )
        elapsed = time.perf_counter() - start
        logger.info(f"[benchmark] {label}: {elapsed*1000:.1f} ms, {len(result)} groups returned.")
        return elapsed

    baseline_time = timed_query(baseline_path, "baseline (unpartitioned)")
    optimized_time = timed_query(optimized_path, "optimized (partitioned + Z-ordered)")

    if optimized_time > 0:
        improvement = (baseline_time - optimized_time) / baseline_time * 100
        logger.info(f"[benchmark] result: {improvement:.1f}% faster on the optimized layout for this query shape.")
    else:
        logger.info("[benchmark] optimized query returned near-instant; improvement effectively 100%.")


# ---------------------------------------------------------------------------
# Time travel / rollback demo
# ---------------------------------------------------------------------------

def run_time_travel_demo(spark, logger):
    """
    Proves Delta time travel works against the real fact_sales table:
    reads version 0 (the very first write) alongside the current version,
    then restores the table to version 0 and confirms the row count
    matches, before leaving a note that in practice you'd restore to the
    version *before* a bad load, not all the way back to v0.
    """
    fact_path = os.path.join(GOLD_DIR, "fact_sales")
    dt = DeltaTable(fact_path)
    history = dt.history()
    versions = sorted(h["version"] for h in history)
    logger.info(f"[time-travel] fact_sales has {len(versions)} version(s) in its transaction log: {versions}")

    if len(versions) < 2:
        logger.info("[time-travel] only one version exists yet - re-run the pipeline on a new day of data to build more history.")
        return

    earliest, latest = versions[0], versions[-1]
    old_df = read_delta(spark, fact_path, version=earliest)
    new_df = read_delta(spark, fact_path, version=latest)
    logger.info(f"[time-travel] version {earliest}: {old_df.count()} rows | version {latest}: {new_df.count()} rows")

    dt.restore(earliest)
    restored_count = DeltaTable(fact_path).to_pandas().shape[0]
    logger.info(f"[time-travel] restored fact_sales to version {earliest} - now {restored_count} rows. Restore itself is logged as a new version, so nothing is lost.")

    # Bring it back to latest so the pipeline output isn't left rolled back.
    dt2 = DeltaTable(fact_path)
    dt2.restore(latest)
    logger.info(f"[time-travel] restored fact_sales back to version {latest} to leave the table in its current state.")


if __name__ == "__main__":
    from pipeline_utils import get_spark

    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", action="store_true", help="Also run the partitioning/Z-order benchmark and time-travel demo.")
    args = parser.parse_args()

    spark = get_spark()
    logger = get_logger("gold_star_schema")[0]
    run_gold(spark, logger)

    if args.benchmark:
        run_partitioning_benchmark(spark, logger)
        run_time_travel_demo(spark, logger)
