"""
Tests for the cleaning/dedup/upsert logic in load_pipeline.py - the same
rules implemented in T-SQL by sql/03_merge_procedure.sql (usp_MergeSalesStaging):
null CustomerID dropped, "3.0"-style quantity strings coerced, duplicate
TransactionID collapsed to the last occurrence, watermark advanced, and a
re-run of the same files is a no-op.
"""

import csv
import os
import shutil
import sqlite3
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def isolated_env(monkeypatch):
    """Points config.LANDING_DIR / config.DB_PATH at a fresh temp dir for
    each test so tests don't interfere with each other or real sample data."""
    tmp_dir = tempfile.mkdtemp(prefix="load_pipeline_test_")
    landing_dir = os.path.join(tmp_dir, "landing")
    db_path = os.path.join(tmp_dir, "warehouse.db")
    os.makedirs(landing_dir, exist_ok=True)

    import config
    monkeypatch.setattr(config, "LANDING_DIR", landing_dir)
    monkeypatch.setattr(config, "DB_PATH", db_path)

    import load_pipeline
    import build_star_schema
    monkeypatch.setattr(load_pipeline, "LANDING_DIR", landing_dir)
    monkeypatch.setattr(load_pipeline, "DB_PATH", db_path)
    monkeypatch.setattr(build_star_schema, "DB_PATH", db_path)

    yield landing_dir, db_path
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _write_customers_products(landing_dir):
    with open(os.path.join(landing_dir, "customers.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["CustomerID", "CustomerName", "Segment", "SignupDate"])
        for cid in range(1, 6):
            w.writerow([cid, f"Cust_{cid}", "Consumer", "2024-01-01"])

    with open(os.path.join(landing_dir, "products.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ProductID", "ProductName", "Category", "UnitCost"])
        for pid in range(1, 4):
            w.writerow([pid, f"Prod_{pid}", "Electronics", 10.0])


def _write_sales_csv(landing_dir, filename, rows):
    with open(os.path.join(landing_dir, filename), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["TransactionID", "CustomerID", "ProductID", "OrderDate", "Quantity", "UnitPrice", "Region"])
        w.writerows(rows)


def test_null_customer_id_dropped(isolated_env):
    landing_dir, db_path = isolated_env
    _write_customers_products(landing_dir)
    _write_sales_csv(landing_dir, "sales_20260701.csv", [
        [1, 1, 1, "2026-07-01", 2, 9.99, "West"],
        [2, "", 1, "2026-07-01", 1, 9.99, "West"],  # bad: no customer
    ])

    from build_star_schema import build
    build(db_path)
    import load_pipeline
    conn = sqlite3.connect(db_path)
    load_pipeline.load_dimensions(conn)
    load_pipeline.load_new_sales_files(conn)
    conn.commit()

    rows = conn.execute("SELECT TransactionID FROM fact_sales").fetchall()
    assert [r[0] for r in rows] == [1]


def test_duplicate_transaction_id_keeps_last(isolated_env):
    landing_dir, db_path = isolated_env
    _write_customers_products(landing_dir)
    _write_sales_csv(landing_dir, "sales_20260701.csv", [
        [1, 1, 1, "2026-07-01", 2, 9.99, "West"],
        [1, 1, 1, "2026-07-01", 5, 9.99, "West"],  # same TransactionID, later in file
    ])

    from build_star_schema import build
    build(db_path)
    import load_pipeline
    conn = sqlite3.connect(db_path)
    load_pipeline.load_dimensions(conn)
    load_pipeline.load_new_sales_files(conn)
    conn.commit()

    row = conn.execute("SELECT Quantity FROM fact_sales WHERE TransactionID = 1").fetchone()
    assert row[0] == 5


def test_float_string_quantity_coerced(isolated_env):
    landing_dir, db_path = isolated_env
    _write_customers_products(landing_dir)
    _write_sales_csv(landing_dir, "sales_20260701.csv", [
        [1, 1, 1, "2026-07-01", "3.0", 9.99, "West"],
    ])

    from build_star_schema import build
    build(db_path)
    import load_pipeline
    conn = sqlite3.connect(db_path)
    load_pipeline.load_dimensions(conn)
    load_pipeline.load_new_sales_files(conn)
    conn.commit()

    row = conn.execute("SELECT Quantity FROM fact_sales WHERE TransactionID = 1").fetchone()
    assert row[0] == 3


def test_rerun_with_no_new_files_is_noop(isolated_env):
    landing_dir, db_path = isolated_env
    _write_customers_products(landing_dir)
    _write_sales_csv(landing_dir, "sales_20260701.csv", [[1, 1, 1, "2026-07-01", 2, 9.99, "West"]])

    from build_star_schema import build
    build(db_path)
    import load_pipeline
    conn = sqlite3.connect(db_path)
    load_pipeline.load_dimensions(conn)
    load_pipeline.load_new_sales_files(conn)
    conn.commit()
    load_pipeline.load_new_sales_files(conn)  # second call, same files
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    assert count == 1
