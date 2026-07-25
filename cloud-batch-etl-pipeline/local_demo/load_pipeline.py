"""
load_pipeline.py
------------------
Local stand-in for what CopyNewSalesRowsToStaging + usp_MergeSalesStaging
do in the real ADF pipeline: read new CSV drops, clean them (drop null
CustomerID, coerce "3.0"-style quantities, dedupe on TransactionID keeping
the last occurrence), and upsert into fact_sales. Also loads the reference
CSVs into dim_customer/dim_product and derives dim_date - same watermark
pattern as the production pipeline, just driven by "which files haven't
been loaded yet" instead of a live blob listing.

    python load_pipeline.py
"""

import csv
import glob
import os
import sqlite3
from datetime import datetime, timedelta

from config import DB_PATH, LANDING_DIR


def load_dimensions(conn):
    with open(os.path.join(LANDING_DIR, "customers.csv")) as f:
        rows = [(r["CustomerID"], r["CustomerName"], r["Segment"], r["SignupDate"]) for r in csv.DictReader(f)]
    conn.executemany(
        "INSERT OR REPLACE INTO dim_customer (CustomerID, CustomerName, Segment, SignupDate) VALUES (?, ?, ?, ?)",
        rows,
    )

    with open(os.path.join(LANDING_DIR, "products.csv")) as f:
        rows = [(r["ProductID"], r["ProductName"], r["Category"], r["UnitCost"]) for r in csv.DictReader(f)]
    conn.executemany(
        "INSERT OR REPLACE INTO dim_product (ProductID, ProductName, Category, UnitCost) VALUES (?, ?, ?, ?)",
        rows,
    )
    print(f"dim_customer/dim_product loaded: {len(rows)} products, plus customers.")


def _ensure_date_dim(conn, date_strs):
    if not date_strs:
        return
    existing = {r[0] for r in conn.execute("SELECT DateKey FROM dim_date")}
    for d in sorted(set(date_strs)):
        dt = datetime.strptime(d, "%Y-%m-%d")
        date_key = int(dt.strftime("%Y%m%d"))
        if date_key in existing:
            continue
        conn.execute(
            "INSERT INTO dim_date (DateKey, FullDate, Year, Month, Day, DayOfWeek, IsWeekend) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (date_key, d, dt.year, dt.month, dt.day, dt.strftime("%A"), 1 if dt.weekday() >= 5 else 0),
        )


def load_new_sales_files(conn):
    all_files = sorted(glob.glob(os.path.join(LANDING_DIR, "sales_*.csv")))
    watermark_row = conn.execute(
        "SELECT WatermarkValue FROM etl_watermark WHERE TableName = 'fact_sales'"
    ).fetchone()
    already_loaded_marker = watermark_row[0] if watermark_row else "1900-01-01"

    # Track loaded files via a simple marker file (stands in for ADF's file-listing
    # LookupNewWatermark step, which compares against the blob's LastModified time).
    loaded_marker_path = os.path.join(LANDING_DIR, ".loaded_files")
    already_loaded = set()
    if os.path.exists(loaded_marker_path):
        with open(loaded_marker_path) as f:
            already_loaded = set(line.strip() for line in f)

    new_files = [f for f in all_files if os.path.basename(f) not in already_loaded]
    if not new_files:
        print("No new sales files to load - fact_sales is up to date.")
        return

    raw_rows = []
    for path in new_files:
        with open(path) as f:
            for r in csv.DictReader(f):
                raw_rows.append(r)
    print(f"Read {len(raw_rows)} raw rows from {len(new_files)} new file(s): {[os.path.basename(f) for f in new_files]}")

    cleaned = {}
    rejected = 0
    for r in raw_rows:
        try:
            txn_id = int(r["TransactionID"])
            customer_id_raw = r["CustomerID"]
            if customer_id_raw is None or str(customer_id_raw).strip() == "":
                rejected += 1
                continue
            customer_id = int(customer_id_raw)
            product_id = int(r["ProductID"])
            order_date = r["OrderDate"]
            quantity = int(float(r["Quantity"]))  # handles "3.0"-style strings, same as TRY_CAST(FLOAT) then INT in T-SQL
            unit_price = round(float(r["UnitPrice"]), 2)
            region = r["Region"].strip() if r["Region"] else None
        except (ValueError, KeyError):
            rejected += 1
            continue

        date_key = int(order_date.replace("-", ""))
        total_amount = round(quantity * unit_price, 2)
        # dict keyed by TransactionID -> later rows overwrite earlier ones,
        # same "keep the last-staged row" behavior as the ROW_NUMBER() dedupe
        # in usp_MergeSalesStaging.
        cleaned[txn_id] = (txn_id, customer_id, product_id, date_key, order_date, quantity, unit_price, total_amount, region)

    dupes_collapsed = len(raw_rows) - rejected - len(cleaned)

    _ensure_date_dim(conn, [row[4] for row in cleaned.values()])

    conn.executemany(
        """INSERT INTO fact_sales
           (TransactionID, CustomerID, ProductID, DateKey, OrderDate, Quantity, UnitPrice, TotalAmount, Region)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(TransactionID) DO UPDATE SET
               CustomerID=excluded.CustomerID, ProductID=excluded.ProductID, DateKey=excluded.DateKey,
               OrderDate=excluded.OrderDate, Quantity=excluded.Quantity, UnitPrice=excluded.UnitPrice,
               TotalAmount=excluded.TotalAmount, Region=excluded.Region""",
        list(cleaned.values()),
    )

    new_watermark = datetime.utcnow().isoformat()
    conn.execute(
        "UPDATE etl_watermark SET WatermarkValue = ? WHERE TableName = 'fact_sales'", (new_watermark,)
    )

    with open(loaded_marker_path, "a") as f:
        for path in new_files:
            f.write(os.path.basename(path) + "\n")

    print(
        f"fact_sales: upserted {len(cleaned)} clean rows "
        f"(dropped {rejected} unparseable/null-CustomerID rows, collapsed {dupes_collapsed} duplicate TransactionIDs)."
    )


def run():
    from build_star_schema import build

    if not os.path.exists(DB_PATH):
        build()

    conn = sqlite3.connect(DB_PATH)
    load_dimensions(conn)
    load_new_sales_files(conn)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    run()
