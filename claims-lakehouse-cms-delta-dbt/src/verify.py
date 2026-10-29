"""Post-run checks used by CI: silver row counts reconcile and a re-run (Delta MERGE) did not change them."""
import sys
from delta.tables import DeltaTable
from common import *
ok = True
sp = spark("verify")
for ds in ("inpatient", "partd"):
    b = sp.read.format("delta").load(os.path.join(LAKE, "bronze", ds)).count()
    s = sp.read.format("delta").load(os.path.join(LAKE, "silver", ds)).count()
    q = sp.read.format("delta").load(os.path.join(LAKE, "quarantine", ds)).count()
    ops = [r["operation"] for r in DeltaTable.forPath(sp, os.path.join(LAKE, "silver", ds)).history().select("operation").collect()]
    good = (b == s + q) and ("MERGE" in ops) and s > 0
    print(f"{ds}: bronze={b} silver={s} quarantine={q} delta_ops={ops} -> {'OK' if good else 'FAIL'}")
    ok &= good
sys.exit(0 if ok else 1)
