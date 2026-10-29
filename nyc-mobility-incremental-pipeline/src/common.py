"""Shared config + Spark session. Paths are overridable via env vars so the same code runs
locally, in CI (small sample) and in Airflow."""
import json
import os
import re

from pyspark.sql import SparkSession

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW = os.environ.get("NYC_RAW", os.path.join(ROOT, "data_raw"))
LAKE = os.environ.get("NYC_LAKE", os.path.join(ROOT, "lake"))
RESULTS = os.environ.get("NYC_RESULTS", os.path.join(ROOT, "results"))
EXPORT = os.environ.get("NYC_EXPORT", os.path.join(LAKE, "silver_export"))  # Parquet snapshot read by dbt/DuckDB
SPARK_TMP = os.environ.get("NYC_SPARK_TMP")  # optional: where Spark spills shuffle files
DELTA_VERSION = "3.2.0"


def _jar_dirs():
    for d in (os.environ.get("NYC_JARS"), os.path.join(ROOT, "jars"), os.path.join(ROOT, "..", "jars")):
        if d and os.path.exists(os.path.join(d, f"delta-spark_2.12-{DELTA_VERSION}.jar")):
            return d
    return None


def spark(app="nyc_mobility", mem=None):
    mem = mem or os.environ.get("NYC_MEM", "2500m")
    b = (SparkSession.builder.master("local[4]").appName(app)
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.driver.memory", mem).config("spark.sql.shuffle.partitions", os.environ.get("NYC_SHUFFLE", "16"))
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false"))
    if SPARK_TMP:
        b = b.config("spark.local.dir", SPARK_TMP)
    jd = _jar_dirs()
    if jd:  # offline: use local jars
        jars = ",".join(os.path.join(jd, j) for j in (f"delta-spark_2.12-{DELTA_VERSION}.jar", f"delta-storage-{DELTA_VERSION}.jar"))
        b = b.config("spark.jars", jars)
    else:   # online (CI): resolve from Maven Central
        b = b.config("spark.jars.packages", f"io.delta:delta-spark_2.12:{DELTA_VERSION}")
    return b.getOrCreate()


def path(layer, name, lake=None):
    return os.path.join(lake or LAKE, layer, name)


def months(raw=None):
    raw = raw or RAW
    found = sorted(re.findall(r"yellow_tripdata_(\d{4}-\d{2})\.parquet", " ".join(os.listdir(raw))))
    return found


def log_metric(name, stage, results=None, **kw):
    results = results or RESULTS
    os.makedirs(results, exist_ok=True)
    p = os.path.join(results, f"{name}.json")
    m = json.load(open(p)) if os.path.exists(p) else {}
    m[stage] = kw
    json.dump(m, open(p, "w"), indent=2, default=str)
