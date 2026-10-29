"""Reference data: taxi zones + daily weather -> typed silver Delta tables."""

from pyspark.sql import functions as F

from common import LAKE, RAW, log_metric, path, spark


def run(raw=None, lake=None):
    raw, lake = raw or RAW, lake or LAKE
    sp = spark("reference")
    z = (sp.read.option("header", True).csv(f"{raw}/taxi_zone_lookup.csv")
         .select(F.col("LocationID").cast("int").alias("location_id"), F.col("Borough").alias("borough"),
                 F.col("Zone").alias("zone"), F.col("service_zone")))
    z.write.format("delta").mode("overwrite").save(path("silver", "zones", lake))

    w = sp.read.option("header", True).csv(f"{raw}/weather_nyc_2025.csv")
    w = (w.select(F.to_date("DATE").alias("weather_date"),
                  F.col("TMAX").cast("double").alias("tmax_f"), F.col("TMIN").cast("double").alias("tmin_f"),
                  F.col("PRCP").cast("double").alias("prcp_in"), F.col("SNOW").cast("double").alias("snow_in"),
                  F.col("SNWD").cast("double").alias("snow_depth_in"), F.col("AWND").cast("double").alias("avg_wind_mph")))
    bad = w.filter("weather_date is null or prcp_in < 0 or tmax_f < tmin_f").count()
    w = w.filter("weather_date is not null and not (prcp_in < 0 or tmax_f < tmin_f)")
    w = w.withColumn("snow_depth_missing", F.col("snow_depth_in").isNull())
    w.write.format("delta").mode("overwrite").save(path("silver", "weather", lake))
    log_metric("metrics_reference", "reference", zones=z.count(), weather_days=w.count(), weather_rejected=bad,
               weather_snow_depth_missing=w.filter("snow_depth_missing").count())
    print("reference loaded", z.count(), w.count(), "rejected", bad)


if __name__ == "__main__":
    run()
