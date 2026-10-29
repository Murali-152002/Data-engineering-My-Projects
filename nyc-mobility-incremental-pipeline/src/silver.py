"""Silver: typed + validated trips, MERGE-upserted per month.
Hard rules -> quarantine (with reasons). Soft rules -> flag columns on silver.
Reconciliation: bronze rows == silver-bound rows + quarantined rows, every month."""
import sys
import time

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import LAKE, log_metric, path, spark

MUTABLE = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
           "congestion_surcharge", "Airport_fee", "cbd_congestion_fee", "payment_type", "RatecodeID", "store_and_fwd_flag"]


def classify(bronze, zones, month):
    b = bronze.select("*").withColumn("_dur_s", F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))
    zid = zones.select(F.col("location_id").alias("_z"))
    b = b.join(F.broadcast(zid.withColumnRenamed("_z", "_pu_ok")), F.col("PULocationID") == F.col("_pu_ok"), "left") \
         .join(F.broadcast(zid.withColumnRenamed("_z", "_do_ok")), F.col("DOLocationID") == F.col("_do_ok"), "left")
    rules = {
        "null_pickup_or_dropoff": F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull(),
        "dropoff_before_pickup": F.col("_dur_s") < 0,
        "duration_over_24h": F.col("_dur_s") > 86400,
        "pickup_outside_file_month": F.date_format("tpep_pickup_datetime", "yyyy-MM") != F.lit(month),
        "unknown_location_id": F.col("_pu_ok").isNull() | F.col("_do_ok").isNull(),
        "negative_total_amount": F.col("total_amount") < 0,   # refunds/disputes. NOT negative fare: see README (flex-fare trips)
        "distance_out_of_range": (F.col("trip_distance") < 0) | (F.col("trip_distance") > 200),
    }
    reasons = F.filter(F.array(*[F.when(c, F.lit(k)) for k, c in rules.items()]), lambda x: x.isNotNull())
    b = b.withColumn("reasons", reasons).drop("_pu_ok", "_do_ok")
    b = b.withColumn("trip_id", F.xxhash64("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
                                         "DOLocationID", "trip_distance", "passenger_count"))
    return b


def run(month, lake=None):
    lake = lake or LAKE
    sp = spark("silver")
    t0 = time.time()
    bronze = sp.read.format("delta").load(path("bronze", "trips", lake)).filter(F.col("file_month") == month)
    zones = sp.read.format("delta").load(path("silver", "zones", lake))
    c = classify(bronze, zones, month).persist(StorageLevel.MEMORY_AND_DISK)
    bronze_rows = c.count()
    ok = c.filter(F.size("reasons") == 0)
    # duplicate keys among valid rows -> keep one, quarantine the rest
    w = Window.partitionBy("trip_id").orderBy(F.col("total_amount").desc_nulls_last(), F.col("_ingested_at"))
    ok = ok.withColumn("_rn", F.row_number().over(w))
    dups = ok.filter("_rn > 1").withColumn("reasons", F.array(F.lit("duplicate_trip_key")))
    good = ok.filter("_rn = 1").drop("_rn")
    quarantine = c.filter(F.size("reasons") > 0).unionByName(dups.drop("_rn"))
    quarantine = quarantine.withColumn("reasons_str", F.concat_ws(",", "reasons")).drop("reasons", "_dur_s")
    quarantine.write.format("delta").mode("overwrite").partitionBy("file_month") \
        .option("replaceWhere", f"file_month = '{month}'").save(path("quarantine", "trips", lake))
    q_rows = sp.read.format("delta").load(path("quarantine", "trips", lake)).filter(F.col("file_month") == month).count()
    reasons = {r["reasons_str"]: r["n"] for r in
               sp.read.format("delta").load(path("quarantine", "trips", lake)).filter(F.col("file_month") == month)
               .groupBy("reasons_str").agg(F.count("*").alias("n")).collect()}

    zp = zones.select(F.col("location_id").alias("PULocationID"), F.col("borough").alias("pu_borough"), F.col("zone").alias("pu_zone"))
    zd = zones.select(F.col("location_id").alias("DOLocationID"), F.col("borough").alias("do_borough"), F.col("zone").alias("do_zone"))
    s = (good.join(F.broadcast(zp), "PULocationID", "left").join(F.broadcast(zd), "DOLocationID", "left")
         .withColumn("trip_month", F.col("file_month"))
         .withColumn("pickup_date", F.to_date("tpep_pickup_datetime")).withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
         .withColumn("trip_minutes", F.round(F.col("_dur_s") / 60.0, 2))
         .withColumn("tip_pct", F.when(F.col("fare_amount") > 0, F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)))
         .withColumn("flag_zero_distance", F.col("trip_distance") == 0)
         .withColumn("flag_zero_passengers", F.coalesce(F.col("passenger_count"), F.lit(0)) == 0)
         .withColumn("flag_zero_duration", F.col("_dur_s") == 0)
         .withColumn("flag_negative_fare", F.col("fare_amount") < 0)
         .withColumn("flag_fare_outlier", F.col("fare_amount") > 500)
         .withColumn("row_hash", F.xxhash64(*MUTABLE))
         .drop("_dur_s", "reasons", "file_month"))
    dest = path("silver", "trips", lake)
    first = not DeltaTable.isDeltaTable(sp, dest)
    if first:
        s.write.format("delta").mode("overwrite").partitionBy("trip_month").save(dest)
    else:
        t = DeltaTable.forPath(sp, dest)
        (t.alias("t").merge(s.alias("s"), f"t.trip_month = '{month}' AND t.trip_id = s.trip_id")
         .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash").whenNotMatchedInsertAll().execute())
    hist = DeltaTable.forPath(sp, dest).history(1).collect()[0]
    om = hist["operationMetrics"] or {}
    silver_rows = sp.read.format("delta").load(dest).filter(F.col("trip_month") == month).count()
    secs = round(time.time() - t0, 1)
    rec = dict(bronze_rows=bronze_rows, silver_rows=silver_rows, quarantine_rows=q_rows, quarantine_reasons=reasons,
               reconciles=(bronze_rows == silver_rows + q_rows), operation=hist["operation"],
               merge_inserted=om.get("numTargetRowsInserted"), merge_updated=om.get("numTargetRowsUpdated"),
               flags={k: s.filter(k).count() for k in ("flag_zero_distance", "flag_zero_passengers", "flag_zero_duration", "flag_negative_fare", "flag_fare_outlier")},
               seconds=secs)
    c.unpersist()
    log_metric("metrics_silver", month, **rec)
    print(f"silver {month}: bronze={bronze_rows} silver={silver_rows} quarantine={q_rows} reconciles={rec['reconciles']} op={rec['operation']} {secs}s")
    return rec


if __name__ == "__main__":
    run(sys.argv[1])
