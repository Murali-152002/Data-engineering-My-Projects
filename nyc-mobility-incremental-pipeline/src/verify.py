"""Post-run integrity checks over the actual Delta tables (used by CI and Airflow). Exits non-zero on failure."""
import sys

from delta.tables import DeltaTable

from common import LAKE, path, spark


def main(expect_merge=False, lake=None):
    lake = lake or LAKE
    sp = spark("verify")
    b = sp.read.format("delta").load(path("bronze", "trips", lake)).groupBy("file_month").count().withColumnRenamed("count", "b")
    s = sp.read.format("delta").load(path("silver", "trips", lake)).groupBy("trip_month").count().withColumnRenamed("count", "s")
    q = sp.read.format("delta").load(path("quarantine", "trips", lake)).groupBy("file_month").count().withColumnRenamed("count", "q")
    r = (b.join(q, "file_month", "left").join(s, b.file_month == s.trip_month, "left").fillna(0, ["q", "s"]).orderBy("file_month").collect())
    ok = True
    for row in r:
        good = row["b"] == row["s"] + row["q"]
        ok &= good
        print(f"{row['file_month']}: bronze={row['b']} silver={row['s']} quarantine={row['q']} -> {'OK' if good else 'MISMATCH'}")
    silver = sp.read.format("delta").load(path("silver", "trips", lake))
    dup = silver.groupBy("trip_month", "trip_id").count().filter("count > 1").count()
    print("duplicate trip_ids in silver:", dup)
    ok &= dup == 0
    if expect_merge:
        ops = [h["operation"] for h in DeltaTable.forPath(sp, path("silver", "trips", lake)).history().collect()]
        print("silver delta ops:", sorted(set(ops)))
        ok &= "MERGE" in ops
    print("VERIFY", "PASSED" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main(expect_merge="--expect-merge" in sys.argv)
