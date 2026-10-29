"""Benchmarks on the real lake: incremental vs full rebuild (from per-month timings), and partitioned vs unpartitioned reads."""
import json
import os
import statistics
import time

from pyspark.sql import functions as F

from common import LAKE, RESULTS, log_metric, path, spark


def timed(sp, q, n=3):
    ts = []
    for _ in range(n):
        t = time.time(); sp.sql(q).collect(); ts.append(time.time() - t)
    return round(statistics.median(ts), 2)


def run():
    sp = spark("benchmark")
    silver_m = json.load(open(os.path.join(RESULTS, "metrics_silver.json")))
    bronze_m = json.load(open(os.path.join(RESULTS, "metrics_bronze.json")))
    per = {m: round(silver_m[m]["seconds"] + bronze_m[m]["seconds"], 1) for m in silver_m if m in bronze_m}
    inc = dict(months=len(per), full_load_seconds=round(sum(per.values()), 1),
               avg_new_month_seconds=round(statistics.mean(per.values()), 1), min_month=min(per.values()), max_month=max(per.values()))
    log_metric("metrics_benchmark", "incremental_vs_full", **inc)

    # Same 4 months (May-Aug 2025, ~16M rows) in both layouts; the unpartitioned copy has rows in arbitrary file order.
    sub = sp.read.format("delta").load(path("silver", "trips", LAKE)).filter(F.col("trip_month").isin("2025-05", "2025-06", "2025-07", "2025-08"))
    sub.createOrReplaceTempView("part")
    un = os.path.join(LAKE, "bench", "silver_unpartitioned")
    sub.repartition(24).write.format("delta").mode("overwrite").save(un)
    sp.read.format("delta").load(un).createOrReplaceTempView("unpart")
    qs = {
        "one_day": ("SELECT count(*), sum(total_amount) FROM {t} WHERE pickup_date = '2025-06-15'{p}", " AND trip_month = '2025-06'"),
        "one_month_by_zone": ("SELECT pu_zone, count(*) FROM {t} WHERE pickup_date >= '2025-06-01' AND pickup_date < '2025-07-01'{p} GROUP BY 1", " AND trip_month = '2025-06'"),
    }
    out = {}
    for name, (q, pred) in qs.items():
        out[name] = dict(partitioned_s=timed(sp, q.format(t="part", p=pred)), unpartitioned_s=timed(sp, q.format(t="unpart", p="")))
        out[name]["speedup_x"] = round(out[name]["unpartitioned_s"] / out[name]["partitioned_s"], 1)
    out["rows_in_each_layout"] = sp.sql("select count(*) from part").collect()[0][0]
    log_metric("metrics_benchmark", "partitioning", **out)
    print(inc); print(out)


if __name__ == "__main__":
    run()
