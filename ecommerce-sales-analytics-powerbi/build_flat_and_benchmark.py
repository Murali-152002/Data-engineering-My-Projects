import duckdb
import time

con = duckdb.connect("/tmp/ecommerce-bi/warehouse.duckdb")

con.execute("""
CREATE OR REPLACE TABLE fact_sales AS SELECT * FROM read_parquet('/tmp/ecommerce-bi/data/fact_sales.parquet');
CREATE OR REPLACE TABLE dim_date AS SELECT * FROM read_parquet('/tmp/ecommerce-bi/data/dim_date.parquet');
CREATE OR REPLACE TABLE dim_product AS SELECT * FROM read_parquet('/tmp/ecommerce-bi/data/dim_product.parquet');
CREATE OR REPLACE TABLE dim_region AS SELECT * FROM read_parquet('/tmp/ecommerce-bi/data/dim_region.parquet');
CREATE OR REPLACE TABLE dim_customer AS SELECT * FROM read_parquet('/tmp/ecommerce-bi/data/dim_customer.parquet');
""")

# Build the flat, fully-denormalized wide table (what a non-modeled "just export everything" report would use)
con.execute("""
CREATE OR REPLACE TABLE flat_sales AS
SELECT
    f.transaction_id, f.quantity, f.discount_pct, f.revenue, f.cost, f.profit,
    d.full_date, d.year, d.month, d.quarter, d.day_of_week, d.is_weekend, d.month_name,
    p.product_name, p.category, p.unit_cost, p.unit_price,
    r.region_name,
    c.segment, c.signup_year
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_region r ON f.region_key = r.region_key
JOIN dim_customer c ON f.customer_key = c.customer_key;
""")

import os
star_bytes = os.path.getsize("/tmp/ecommerce-bi/data/fact_sales.parquet") + \
             os.path.getsize("/tmp/ecommerce-bi/data/dim_date.parquet") + \
             os.path.getsize("/tmp/ecommerce-bi/data/dim_product.parquet") + \
             os.path.getsize("/tmp/ecommerce-bi/data/dim_region.parquet") + \
             os.path.getsize("/tmp/ecommerce-bi/data/dim_customer.parquet")

con.execute("COPY flat_sales TO '/tmp/ecommerce-bi/data/flat_sales.parquet' (FORMAT PARQUET)")
flat_bytes = os.path.getsize("/tmp/ecommerce-bi/data/flat_sales.parquet")

print(f"Star-schema parquet total: {star_bytes/1e6:.2f} MB")
print(f"Flat denormalized parquet: {flat_bytes/1e6:.2f} MB")
print(f"Storage reduction (star vs flat): {(1 - star_bytes/flat_bytes)*100:.1f}%")

# ---- Benchmark: typical BI aggregation query, run N times each, take median ----
star_query = """
SELECT dd.year, dd.month_name, dp.category, SUM(f.revenue) AS revenue, SUM(f.profit) AS profit
FROM fact_sales f
JOIN dim_date dd ON f.date_key = dd.date_key
JOIN dim_product dp ON f.product_key = dp.product_key
GROUP BY dd.year, dd.month_name, dp.category
ORDER BY dd.year, dd.month_name, dp.category;
"""
flat_query = """
SELECT year, month_name, category, SUM(revenue) AS revenue, SUM(profit) AS profit
FROM flat_sales
GROUP BY year, month_name, category
ORDER BY year, month_name, category;
"""

def timeit(sql, runs=7):
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t0)
    times.sort()
    return times[len(times)//2]  # median

star_t = timeit(star_query)
flat_t = timeit(flat_query)
print(f"\nStar-schema query (median of 7): {star_t*1000:.1f} ms")
print(f"Flat table query (median of 7):  {flat_t*1000:.1f} ms")
print(f"Speedup (flat / star): {flat_t/star_t:.2f}x   |   Time reduction: {(1 - star_t/flat_t)*100:.1f}%")

con.close()
