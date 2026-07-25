"""
data_generator.py
------------------
Generates synthetic multi-source data so the pipeline can be run and demoed
end-to-end without any real company data:

  1. Daily CSV "drops" (flat-file source) -> sample_data/landing/sales_YYYYMMDD.csv
  2. An "operational" relational source (SQLite standing in for Azure SQL) with
     customers and products reference tables.

Run directly to (re)generate a few days of sample data:
    python data_generator.py --days 5
"""

import argparse
import os
import random
import sqlite3
from datetime import datetime, timedelta

from config import LANDING_DIR, OPERATIONAL_DB_PATH

random.seed(42)

REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
CATEGORIES = ["Electronics", "Home & Kitchen", "Apparel", "Sporting Goods", "Toys"]


def _ensure_dirs():
    os.makedirs(LANDING_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OPERATIONAL_DB_PATH), exist_ok=True)


def generate_operational_db(n_customers=5000, n_products=300):
    """Creates/refreshes the SQLite 'operational source' with customers + products."""
    _ensure_dirs()
    conn = sqlite3.connect(OPERATIONAL_DB_PATH)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS customers")
    cur.execute("""
        CREATE TABLE customers (
            CustomerID INTEGER PRIMARY KEY,
            CustomerName TEXT,
            Segment TEXT,
            SignupDate TEXT
        )
    """)
    segments = ["Consumer", "Corporate", "Small Business"]
    for cid in range(1, n_customers + 1):
        signup = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 900))
        cur.execute(
            "INSERT INTO customers VALUES (?, ?, ?, ?)",
            (cid, f"Customer_{cid:04d}", random.choice(segments), signup.strftime("%Y-%m-%d")),
        )

    cur.execute("DROP TABLE IF EXISTS products")
    cur.execute("""
        CREATE TABLE products (
            ProductID INTEGER PRIMARY KEY,
            ProductName TEXT,
            Category TEXT,
            UnitCost REAL
        )
    """)
    for pid in range(1, n_products + 1):
        category = random.choice(CATEGORIES)
        cost = round(random.uniform(5, 250), 2)
        cur.execute(
            "INSERT INTO products VALUES (?, ?, ?, ?)",
            (pid, f"Product_{pid:04d}", category, cost),
        )

    conn.commit()
    conn.close()
    print(f"Operational DB refreshed: {n_customers} customers, {n_products} products -> {OPERATIONAL_DB_PATH}")


def generate_daily_csv(date, n_customers=5000, n_products=300, min_rows=120000, max_rows=150000, inject_dirty_data=True):
    """
    Generates one day's sales transaction CSV, deliberately including some
    messy real-world data (nulls, dupes, bad types) that the Silver layer
    is responsible for cleaning up.
    """
    _ensure_dirs()
    filename = os.path.join(LANDING_DIR, f"sales_{date.strftime('%Y%m%d')}.csv")
    n_rows = random.randint(min_rows, max_rows)

    rows = []
    header = "TransactionID,CustomerID,ProductID,OrderDate,Quantity,UnitPrice,Region"
    rows.append(header)

    # Multiplier must exceed the largest possible daily row count or two
    # different days' transaction IDs collide (each day gets its own
    # 1,000,000-wide ID block: YYYYMMDD * 1,000,000 + sequence).
    txn_start = int(date.strftime("%Y%m%d")) * 1_000_000
    for i in range(n_rows):
        txn_id = txn_start + i
        customer_id = random.randint(1, n_customers)
        product_id = random.randint(1, n_products)
        order_date = date.strftime("%Y-%m-%d")
        quantity = random.randint(1, 8)
        unit_price = round(random.uniform(5, 300), 2)
        region = random.choice(REGIONS)

        if inject_dirty_data:
            # ~3% null CustomerID
            if random.random() < 0.03:
                customer_id = ""
            # ~2% quantity stored as float-looking string ("3.0") to test type coercion
            if random.random() < 0.02:
                quantity = f"{quantity}.0"
            # ~1% duplicate the previous row's TransactionID to test dedupe logic
            if random.random() < 0.01 and rows:
                txn_id = txn_start + max(i - 1, 0)

        rows.append(f"{txn_id},{customer_id},{product_id},{order_date},{quantity},{unit_price},{region}")

    with open(filename, "w") as f:
        f.write("\n".join(rows))

    print(f"Generated {n_rows} rows -> {filename}")
    return filename


def generate_n_days(n_days=5, start_date=None):
    start_date = start_date or (datetime.today() - timedelta(days=n_days))
    generate_operational_db()
    files = []
    for d in range(n_days):
        day = start_date + timedelta(days=d)
        files.append(generate_daily_csv(day))
    return files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic source data for the ETL pipeline demo.")
    parser.add_argument("--days", type=int, default=5, help="How many days of CSV drops to generate.")
    args = parser.parse_args()
    generate_n_days(args.days)
