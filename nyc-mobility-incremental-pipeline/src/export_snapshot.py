"""Export silver to Parquet so DuckDB/dbt can build the gold layer (DuckDB can't read Delta offline here)."""
import os

from common import EXPORT, LAKE, log_metric, path, spark


def run(lake=None):
    lake = lake or LAKE
    sp = spark("export")
    t = sp.read.format("delta").load(path("silver", "trips", lake))
    t.write.mode("overwrite").partitionBy("trip_month").parquet(os.path.join(EXPORT, "trips"))
    for n in ("zones", "weather"):
        sp.read.format("delta").load(path("silver", n, lake)).coalesce(1).write.mode("overwrite") \
            .parquet(os.path.join(EXPORT, n))
    log_metric("metrics_export", "export", trips=t.count())


if __name__ == "__main__":
    run()
