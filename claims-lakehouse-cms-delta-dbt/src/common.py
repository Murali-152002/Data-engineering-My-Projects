import os, json, time
from pyspark.sql import SparkSession
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW = os.environ.get("CL_RAW", os.path.join(ROOT, "data_raw"))  # CI points this at data_sample/
# Bulky generated layers live outside the synced folder to keep it small; override with CL_LAKE
LAKE = os.environ.get("CL_LAKE", os.path.join(ROOT, "lake"))
RESULTS = os.environ.get("CL_RESULTS", os.path.join(ROOT, "results"))  # CI writes to a temp dir
JARS = ",".join(os.path.join(ROOT, "jars", j) for j in ("delta-spark_2.12-3.2.0.jar", "delta-storage-3.2.0.jar"))
DATASETS = {
    "inpatient": dict(file="MUP_INP_RY26_P03_V10_DY24_PrvSvc.csv"),
    "partd": dict(file="mup_dpr_ry26_p04_v20_dy24_npi.csv"),
}
def spark(app="claims_lakehouse"):
    return (SparkSession.builder.master("local[4]").appName(app)
            .config("spark.jars", JARS)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false").getOrCreate())
def log_metric(ds, stage, **kw):
    os.makedirs(RESULTS, exist_ok=True)
    p = os.path.join(RESULTS, f"metrics_{ds}.json")
    m = json.load(open(p)) if os.path.exists(p) else {}
    m[stage] = kw
    json.dump(m, open(p, "w"), indent=2)
