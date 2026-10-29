"""Late-arriving + corrected data test (simulated, and labelled as such).

Delivery 1 = a month with 20,000 rows missing and 5,000 rows carrying stale (wrong) fares.
Delivery 2 = the real file. After the MERGE, silver must equal a clean single-pass load of the real file with zero
duplicate keys, and replaying delivery 2 again must change nothing (idempotent).

Run all steps:  python late_data_test.py [month]            (what CI/README use)
Run one step:   python late_data_test.py [month] <step>     (prepare|delivery1|delivery2|control|replay|compare)
Steps are separate so a long run can be resumed; state lives in WORK (default: a temp dir, override with LATE_WORK)."""
import json
import os
import shutil
import sys
import tempfile
import time

from pyspark.sql import functions as F

import bronze
import reference
import silver
from common import RAW, log_metric, path, spark

WORK = os.environ.get("LATE_WORK") or os.path.join(tempfile.gettempdir(), "late_data_work")
HOLDOUT = int(os.environ.get("LATE_HOLDOUT", 20000))
CORRUPT = int(os.environ.get("LATE_CORRUPT", 5000))


def prepare(month):
    sp = spark("late_data")
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(os.path.join(WORK, "raw"))
    real = sp.read.parquet(f"{RAW}/yellow_tripdata_{month}.parquet")
    n = real.count()
    df = real.withColumn("_r", F.rand(7))
    keep = df.filter(F.col("_r") >= HOLDOUT / n)
    hit = F.col("_r") < (HOLDOUT + CORRUPT) / n
    stale = (keep.withColumn("fare_amount", F.when(hit, F.col("fare_amount") + 5).otherwise(F.col("fare_amount")))
             .withColumn("total_amount", F.when(hit, F.col("total_amount") + 5).otherwise(F.col("total_amount"))).drop("_r"))
    stale.write.parquet(os.path.join(WORK, "raw", f"yellow_tripdata_{month}.parquet"))
    reference.run(RAW, os.path.join(WORK, "lake_t"))
    reference.run(RAW, os.path.join(WORK, "lake_c"))
    json.dump({"raw_rows": n}, open(os.path.join(WORK, "state.json"), "w"))


def _state():
    return json.load(open(os.path.join(WORK, "state.json")))


def _save(**kw):
    s = _state(); s.update(kw); json.dump(s, open(os.path.join(WORK, "state.json"), "w"), default=str)


def delivery1(month):
    lt = os.path.join(WORK, "lake_t")
    bronze.run(month, os.path.join(WORK, "raw"), lt)
    r = silver.run(month, lt)
    _save(silver_after_delivery1=r["silver_rows"])


def delivery2(month):
    lt = os.path.join(WORK, "lake_t")
    t0 = time.time()
    bronze.run(month, RAW, lt)
    r = silver.run(month, lt)
    _save(merge_inserted=r["merge_inserted"], merge_updated=r["merge_updated"], silver_after_fix=r["silver_rows"],
          correction_seconds=round(time.time() - t0, 1))


def control(month):
    lc = os.path.join(WORK, "lake_c")
    bronze.run(month, RAW, lc)
    r = silver.run(month, lc)
    _save(control_clean_load=r["silver_rows"])


def replay(month):
    lt = os.path.join(WORK, "lake_t")
    r = silver.run(month, lt)  # same bronze as delivery 2: replaying the same delivery must be a no-op
    _save(replay_inserted=r["merge_inserted"], replay_updated=r["merge_updated"])


def compare(month):
    sp = spark("late_data")
    lt, lc = os.path.join(WORK, "lake_t"), os.path.join(WORK, "lake_c")
    a = sp.read.format("delta").load(path("silver", "trips", lt)).select("trip_id", "row_hash")
    c = sp.read.format("delta").load(path("silver", "trips", lc)).select("trip_id", "row_hash")
    diff = a.exceptAll(c).count() + c.exceptAll(a).count()
    dups = sp.read.format("delta").load(path("silver", "trips", lt)).groupBy("trip_month", "trip_id").count().filter("count>1").count()
    s = _state()
    res = dict(month=month, raw_rows=s["raw_rows"], held_out_rows=HOLDOUT, stale_fare_rows=CORRUPT,
               silver_after_delivery1=s["silver_after_delivery1"], merge_inserted=s["merge_inserted"], merge_updated=s["merge_updated"],
               silver_after_fix=s["silver_after_fix"], control_clean_load=s["control_clean_load"], rows_differing_from_control=diff,
               duplicate_keys=dups, replay_inserted=s["replay_inserted"], replay_updated=s["replay_updated"],
               correction_seconds=s["correction_seconds"])
    log_metric("metrics_late_data", "late_data_test", **res)
    print(res)
    ok = diff == 0 and dups == 0 and str(res["replay_inserted"]) == "0" and str(res["replay_updated"]) == "0"
    print("LATE DATA TEST", "PASSED" if ok else "FAILED")
    sys.exit(0 if ok else 1)


STEPS = ["prepare", "delivery1", "delivery2", "control", "replay", "compare"]

if __name__ == "__main__":
    month = sys.argv[1] if len(sys.argv) > 1 else "2025-03"
    todo = [sys.argv[2]] if len(sys.argv) > 2 else STEPS
    for st in todo:
        globals()[st](month)
