"""Run one month end-to-end (bronze -> silver). Airflow calls this per logical month."""
import sys

import bronze
import silver

if __name__ == "__main__":
    m = sys.argv[1]
    bronze.run(m)
    r = silver.run(m)
    if not r["reconciles"]:
        raise SystemExit(f"RECONCILIATION FAILED for {m}")
