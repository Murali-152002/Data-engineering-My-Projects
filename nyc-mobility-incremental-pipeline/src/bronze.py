"""Bronze: one raw monthly TLC file -> Delta. Idempotent per month (replaceWhere on file_month)."""
import sys
import time

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

from common import LAKE, RAW, log_metric, path, spark

CANON = [("VendorID", IntegerType()), ("tpep_pickup_datetime", TimestampType()), ("tpep_dropoff_datetime", TimestampType()),
         ("passenger_count", IntegerType()), ("trip_distance", DoubleType()), ("RatecodeID", IntegerType()),
         ("store_and_fwd_flag", StringType()), ("PULocationID", IntegerType()), ("DOLocationID", IntegerType()),
         ("payment_type", IntegerType()), ("fare_amount", DoubleType()), ("extra", DoubleType()), ("mta_tax", DoubleType()),
         ("tip_amount", DoubleType()), ("tolls_amount", DoubleType()), ("improvement_surcharge", DoubleType()),
         ("total_amount", DoubleType()), ("congestion_surcharge", DoubleType()), ("Airport_fee", DoubleType()),
         ("cbd_congestion_fee", DoubleType())]


def run(month, raw=None, lake=None):
    raw, lake = raw or RAW, lake or LAKE
    sp = spark("bronze")
    t0 = time.time()
    df = sp.read.parquet(f"{raw}/yellow_tripdata_{month}.parquet")
    have = {c.lower(): c for c in df.columns}
    cols = []
    for name, typ in CANON:  # canonical schema: tolerates column-case drift and missing columns across months
        src = have.get(name.lower())
        cols.append((F.col(src) if src else F.lit(None)).cast(typ).alias(name))
    out = (df.select(*cols).withColumn("file_month", F.lit(month))
           .withColumn("_source_file", F.lit(f"yellow_tripdata_{month}.parquet")).withColumn("_ingested_at", F.current_timestamp()))
    dest = path("bronze", "trips", lake)
    w = out.write.format("delta").mode("overwrite").partitionBy("file_month")
    w = w.option("replaceWhere", f"file_month = '{month}'")
    w.save(dest)
    n = sp.read.format("delta").load(dest).filter(F.col("file_month") == month).count()
    secs = round(time.time() - t0, 1)
    log_metric("metrics_bronze", month, rows=n, seconds=secs)
    print(f"bronze {month}: {n} rows in {secs}s")
    return n


if __name__ == "__main__":
    run(sys.argv[1])
