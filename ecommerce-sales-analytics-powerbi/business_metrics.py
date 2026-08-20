import duckdb
con = duckdb.connect("/tmp/ecommerce-bi/warehouse.duckdb")

print("=== Overall scale ===")
print(con.execute("SELECT COUNT(*) AS txns, ROUND(SUM(revenue),2) AS total_revenue, ROUND(SUM(profit),2) AS total_profit FROM fact_sales").fetchdf())

print("\n=== Revenue by year (YoY growth) ===")
yoy = con.execute("""
    SELECT dd.year, ROUND(SUM(f.revenue),2) AS revenue
    FROM fact_sales f JOIN dim_date dd ON f.date_key = dd.date_key
    GROUP BY dd.year ORDER BY dd.year
""").fetchdf()
print(yoy)
for i in range(1, len(yoy)):
    growth = (yoy.revenue[i] / yoy.revenue[i-1] - 1) * 100
    print(f"  {yoy.year[i-1]} -> {yoy.year[i]}: {growth:.1f}% growth")

print("\n=== Category share: revenue vs profit vs txn volume ===")
cat = con.execute("""
    SELECT dp.category,
           COUNT(*) AS txns,
           ROUND(SUM(f.revenue),2) AS revenue,
           ROUND(SUM(f.profit),2) AS profit
    FROM fact_sales f JOIN dim_product dp ON f.product_key = dp.product_key
    GROUP BY dp.category ORDER BY profit DESC
""").fetchdf()
cat["txn_share_pct"] = (cat.txns / cat.txns.sum() * 100).round(1)
cat["profit_share_pct"] = (cat.profit / cat.profit.sum() * 100).round(1)
print(cat)

print("\n=== Average order value & margin ===")
print(con.execute("""
    SELECT ROUND(AVG(revenue),2) AS avg_order_value,
           ROUND(SUM(profit)/SUM(revenue)*100, 1) AS overall_margin_pct
    FROM fact_sales
""").fetchdf())

print("\n=== Weekend vs weekday, holiday months ===")
print(con.execute("""
    SELECT dd.is_weekend, COUNT(*) AS txns, ROUND(AVG(f.revenue),2) AS avg_rev
    FROM fact_sales f JOIN dim_date dd ON f.date_key = dd.date_key
    GROUP BY dd.is_weekend
""").fetchdf())
print(con.execute("""
    SELECT dd.month_name, COUNT(*) AS txns
    FROM fact_sales f JOIN dim_date dd ON f.date_key = dd.date_key
    GROUP BY dd.month_name ORDER BY txns DESC LIMIT 3
""").fetchdf())
