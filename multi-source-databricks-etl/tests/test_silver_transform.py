"""
Tests for the Silver-layer cleaning rules in silver_transform.py.
Builds small in-memory Spark DataFrames that mirror the shape of
bronze_sales_transactions (including the same messy-data problems
data_generator.py deliberately injects) and asserts the cleaning
logic handles each one correctly.
"""

from pyspark.sql import Row

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType


BRONZE_COLUMNS = [
    "TransactionID", "CustomerID", "ProductID", "OrderDate",
    "Quantity", "UnitPrice", "Region", "_source_file", "_ingested_at",
]


def _bronze_row(txn_id, customer_id, product_id, order_date, quantity, unit_price, region, ingested_at="2026-07-20 00:00:00"):
    return Row(
        TransactionID=txn_id, CustomerID=customer_id, ProductID=product_id,
        OrderDate=order_date, Quantity=quantity, UnitPrice=unit_price,
        Region=region, _source_file="sales_20260720.csv", _ingested_at=ingested_at,
    )


def test_null_customer_id_rows_are_dropped(spark):
    from silver_transform import transform_sales_transactions
    import logging

    df = spark.createDataFrame([
        _bronze_row("1", "10", "5", "2026-07-20", "2", "9.99", "West"),
        _bronze_row("2", "", "5", "2026-07-20", "1", "9.99", "West"),   # bad: empty CustomerID
        _bronze_row("3", None, "5", "2026-07-20", "1", "9.99", "West"),  # bad: null CustomerID
    ])
    _write_bronze_and_run(spark, df, transform_sales_transactions, logging.getLogger("test"))

    result = _read_silver_sales(spark)
    assert result.count() == 1
    assert result.collect()[0]["TransactionID"] == 1


def test_duplicate_transaction_id_keeps_latest(spark):
    from silver_transform import transform_sales_transactions
    import logging

    df = spark.createDataFrame([
        _bronze_row("1", "10", "5", "2026-07-20", "2", "9.99", "West", ingested_at="2026-07-20 00:00:00"),
        _bronze_row("1", "10", "5", "2026-07-20", "3", "9.99", "West", ingested_at="2026-07-20 01:00:00"),  # later resend
    ])
    _write_bronze_and_run(spark, df, transform_sales_transactions, logging.getLogger("test"))

    result = _read_silver_sales(spark)
    assert result.count() == 1
    assert result.collect()[0]["Quantity"] == 3  # the later-ingested row should win


def test_float_string_quantity_is_coerced_to_int(spark):
    from silver_transform import transform_sales_transactions
    import logging

    df = spark.createDataFrame([
        _bronze_row("1", "10", "5", "2026-07-20", "3.0", "9.99", "West"),
    ])
    _write_bronze_and_run(spark, df, transform_sales_transactions, logging.getLogger("test"))

    result = _read_silver_sales(spark)
    row = result.collect()[0]
    # Note: read_delta() round-trips through pandas, which widens int32 to
    # int64 ("bigint") on the way back - that's a storage-width artifact of
    # the delta-rs bridge, not a correctness issue. What actually matters
    # is that "3.0" parsed as a whole number instead of erroring or truncating.
    assert row["Quantity"] == 3
    assert result.schema["Quantity"].dataType.simpleString() in ("int", "bigint")


def test_large_transaction_id_does_not_overflow(spark):
    """Regression test: TransactionID values are generated as
    YYYYMMDD * 10000 + sequence (see data_generator.py), which exceeds
    int32 range. Casting to IntegerType silently overflows to null,
    which breaks every downstream MERGE match - this must stay a
    LongType cast."""
    from silver_transform import transform_sales_transactions
    import logging

    big_id = "202607200001"  # ~2×10^11, well past int32 max of ~2.1×10^9
    df = spark.createDataFrame([
        _bronze_row(big_id, "10", "5", "2026-07-20", "2", "9.99", "West"),
    ])
    _write_bronze_and_run(spark, df, transform_sales_transactions, logging.getLogger("test"))

    result = _read_silver_sales(spark)
    row = result.collect()[0]
    assert row["TransactionID"] == int(big_id)
    assert row["TransactionID"] is not None


# --- helpers -----------------------------------------------------------

def _write_bronze_and_run(spark, bronze_df, transform_fn, logger):
    import os
    import shutil
    import tempfile

    import config
    from delta_io import write_delta

    tmp_dir = tempfile.mkdtemp(prefix="silver_test_")
    bronze_path = os.path.join(tmp_dir, "bronze_sales_transactions")
    silver_path = os.path.join(tmp_dir, "silver_sales_transactions")

    write_delta(bronze_df, bronze_path, mode="overwrite")

    # Point the module at our temp Bronze/Silver dirs for this one call.
    orig_bronze, orig_silver = config.BRONZE_DIR, config.SILVER_DIR
    config.BRONZE_DIR, config.SILVER_DIR = tmp_dir, tmp_dir
    import silver_transform
    silver_transform.BRONZE_DIR, silver_transform.SILVER_DIR = tmp_dir, tmp_dir
    try:
        transform_fn(spark, logger)
    finally:
        config.BRONZE_DIR, config.SILVER_DIR = orig_bronze, orig_silver
        silver_transform.BRONZE_DIR, silver_transform.SILVER_DIR = orig_bronze, orig_silver

    global _last_silver_path
    _last_silver_path = silver_path


_last_silver_path = None


def _read_silver_sales(spark):
    from delta_io import read_delta
    return read_delta(spark, _last_silver_path)
