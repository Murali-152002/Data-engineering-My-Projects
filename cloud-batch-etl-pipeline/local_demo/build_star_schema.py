"""
build_star_schema.py
----------------------
Creates the local proxy warehouse (SQLite) with the same table shapes and
the same two nonclustered indexes on fact_sales (CustomerID, OrderDate)
defined in sql/01_create_star_schema.sql, so the benchmark script measures
the real effect of those specific indexes rather than a generic one.
"""

import os
import sqlite3

from config import DB_PATH


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    CustomerID    INTEGER PRIMARY KEY,
    CustomerName  TEXT NOT NULL,
    Segment       TEXT,
    SignupDate    TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    ProductID    INTEGER PRIMARY KEY,
    ProductName  TEXT NOT NULL,
    Category     TEXT,
    UnitCost     REAL
);

CREATE TABLE IF NOT EXISTS dim_date (
    DateKey     INTEGER PRIMARY KEY,
    FullDate    TEXT NOT NULL,
    Year        INTEGER NOT NULL,
    Month       INTEGER NOT NULL,
    Day         INTEGER NOT NULL,
    DayOfWeek   TEXT NOT NULL,
    IsWeekend   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    TransactionID   INTEGER PRIMARY KEY,
    CustomerID      INTEGER NOT NULL,
    ProductID       INTEGER NOT NULL,
    DateKey         INTEGER NOT NULL,
    OrderDate       TEXT NOT NULL,
    Quantity        INTEGER NOT NULL,
    UnitPrice       REAL NOT NULL,
    TotalAmount     REAL NOT NULL,
    Region          TEXT,
    FOREIGN KEY (CustomerID) REFERENCES dim_customer(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES dim_product(ProductID),
    FOREIGN KEY (DateKey) REFERENCES dim_date(DateKey)
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    TableName       TEXT PRIMARY KEY,
    WatermarkValue  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_rejected_rows (
    RejectedAt  TEXT,
    Reason      TEXT,
    RawRow      TEXT
);
"""


def build(db_path=DB_PATH, with_indexes=True):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)
    if with_indexes:
        create_indexes(conn)
    conn.execute(
        "INSERT OR IGNORE INTO etl_watermark (TableName, WatermarkValue) VALUES ('fact_sales', '1900-01-01')"
    )
    conn.commit()
    conn.close()
    print(f"Star schema built at {db_path} (indexes={'on' if with_indexes else 'off'}).")


def create_indexes(conn):
    conn.execute("CREATE INDEX IF NOT EXISTS IX_fact_sales_CustomerID ON fact_sales (CustomerID)")
    conn.execute("CREATE INDEX IF NOT EXISTS IX_fact_sales_OrderDate ON fact_sales (OrderDate)")


def drop_indexes(conn):
    conn.execute("DROP INDEX IF EXISTS IX_fact_sales_CustomerID")
    conn.execute("DROP INDEX IF EXISTS IX_fact_sales_OrderDate")


if __name__ == "__main__":
    build()
