"""
run_index_benchmark.py
------------------------
Empirically measures the effect of the two nonclustered indexes defined in
sql/01_create_star_schema.sql (on CustomerID and OrderDate) by running the
same representative reporting queries from sql/04_query_performance_demo.sql
against the loaded fact_sales table with the indexes dropped, then again
with them present, and reporting the real elapsed-time difference.

Run load_pipeline.py first to populate fact_sales with data, then:
    python run_index_benchmark.py
"""

import sqlite3
import time

from build_star_schema import create_indexes, drop_indexes
from config import DB_PATH


CUSTOMER_DRILLDOWN_QUERY = """
    SELECT c.CustomerName, COUNT(*) AS TransactionCount, SUM(f.TotalAmount) AS TotalSpend
    FROM fact_sales f
    JOIN dim_customer c ON c.CustomerID = f.CustomerID
    WHERE f.CustomerID = 137
    GROUP BY c.CustomerName
"""

DATE_RANGE_QUERY = """
    SELECT f.OrderDate, f.Region, SUM(f.TotalAmount) AS DailyRevenue, COUNT(*) AS TransactionCount
    FROM fact_sales f
    WHERE f.OrderDate = (SELECT MAX(OrderDate) FROM fact_sales)
    GROUP BY f.OrderDate, f.Region
"""


def _time_query(conn, sql, repeats=20):
    cur = conn.cursor()
    start = time.perf_counter()
    for _ in range(repeats):
        cur.execute(sql).fetchall()
    return (time.perf_counter() - start) / repeats


def run(repeats=20):
    conn = sqlite3.connect(DB_PATH)
    row_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    print(f"Benchmarking against fact_sales with {row_count} rows, {repeats} runs per query.\n")

    drop_indexes(conn)
    conn.execute("ANALYZE")
    before_customer = _time_query(conn, CUSTOMER_DRILLDOWN_QUERY, repeats)
    before_date = _time_query(conn, DATE_RANGE_QUERY, repeats)

    create_indexes(conn)
    conn.execute("ANALYZE")
    after_customer = _time_query(conn, CUSTOMER_DRILLDOWN_QUERY, repeats)
    after_date = _time_query(conn, DATE_RANGE_QUERY, repeats)

    conn.close()

    def pct(before, after):
        return (before - after) / before * 100 if before > 0 else 0.0

    print(f"{'Query':32s} {'No index (ms)':>15s} {'With index (ms)':>17s} {'Improvement':>13s}")
    print(f"{'Customer drill-down':32s} {before_customer*1000:15.2f} {after_customer*1000:17.2f} {pct(before_customer, after_customer):12.1f}%")
    print(f"{'Date-range report':32s} {before_date*1000:15.2f} {after_date*1000:17.2f} {pct(before_date, after_date):12.1f}%")

    avg_improvement = (pct(before_customer, after_customer) + pct(before_date, after_date)) / 2
    print(f"\nAverage improvement across both query shapes: {avg_improvement:.1f}%")


if __name__ == "__main__":
    run()
