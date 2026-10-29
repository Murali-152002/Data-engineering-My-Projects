"""Bronze: land the raw CMS CSV as-is (all strings) + ingest metadata. No cleaning here."""
import sys, time
from pyspark.sql import functions as F
from common import *
ds = sys.argv[1]; t0 = time.time()
sp = spark(f"bronze_{ds}")
src = os.path.join(RAW, DATASETS[ds]["file"])
df = (sp.read.option("header", True).option("multiLine", False).option("escape", '"').csv(src)
      .withColumn("_source_file", F.lit(DATASETS[ds]["file"])).withColumn("_ingested_at", F.current_timestamp()))
out = os.path.join(LAKE, "bronze", ds)
df.write.format("delta").mode("overwrite").save(out)
n = sp.read.format("delta").load(out).count()
log_metric(ds, "bronze", rows=n, columns=len(df.columns) - 2, seconds=round(time.time() - t0, 1))
print(ds, "bronze rows", n, "sec", round(time.time() - t0, 1))
