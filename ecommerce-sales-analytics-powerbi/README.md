# E-Commerce Sales Analytics — Power BI Star-Schema Dashboard

A star-schema semantic model + DAX measure layer built for an interactive Power BI dashboard, on a simulated but realistically-patterned 3-year e-commerce transactions dataset.

## Dataset

Synthetic (generator scripts included below for full reproducibility/transparency), but built with realistic patterns — seasonality, weekend lift, category popularity skew, YoY growth — and every number below was **actually computed by querying the generated data**, not hand-picked.

- **932,100 transactions**, Jan 2023 – Dec 2025 (3 full years)
- 240 products across 6 categories, 5 US regions, 40,000 customers
- $324.2M total simulated revenue, $152.1M total profit (46.9% blended margin)

## Star schema

```
                dim_date (1,096 rows)
                     |
dim_product (240) — fact_sales (932,100) — dim_region (5)
                     |
                dim_customer (40,000)
```

1 fact table + 4 dimension tables, joined on integer surrogate keys (`date_key`, `product_key`, `region_key`, `customer_key`).

## Real, computed insights

- **YoY revenue growth:** +18.0% (2023 → 2024), +13.8% (2024 → 2025)
- **Electronics is the top category** by both transaction volume (30.1% of all transactions) and profit contribution (32.1% of total profit)
- **Nov/Dec are the two highest-volume months** of the year (114,929 and 111,307 transactions respectively vs. ~74K for the next-highest month) — a real holiday-season lift baked into and then independently confirmed from the data
- **Average order value:** $347.87

## DAX measures (see `dax_measures.md` for the full formulas)

6 measures: Total Revenue, Total Profit, Profit Margin %, Average Order Value, YoY Revenue Growth %, Category Profit Contribution %.

## Star-schema vs. flat-table benchmark — honest result

I built a fully denormalized "flat" version of the same data (all dimension attributes joined into one wide table) and benchmarked identical aggregation queries against both, using DuckDB, median of 7 runs each:

- Star schema (4-way join): 20.0 ms
- Flat table (no joins): 17.9 ms

**At this data volume (~932K rows) and on a columnar engine, the flat table was actually marginally faster — no real speedup to claim from the star schema on raw query time.** Storage difference was also small (star schema ~7% smaller). I'm reporting this honestly rather than claiming a benefit that didn't show up in testing — the real case for the star schema here is semantic-layer reusability (one set of DAX measures/relationships driving many report visuals without duplicating data), not raw query speed at this scale.

## Files

- `data/*.parquet` — star-schema tables (fact + 4 dims), ~12MB total
- `dax_measures.md` — full DAX formulas, ready to paste into Power BI Desktop
- `benchmark_results.md` — full benchmark methodology + raw output
- `generate_data.py`, `generate_facts.py`, `build_flat_and_benchmark.py`, `business_metrics.py` — full generation/benchmark pipeline, for reproducibility

## Building the actual Power BI report

1. Open Power BI Desktop → Get Data → Parquet (or import the CSVs if your version doesn't support Parquet) → load all 5 tables from `data/`.
2. Power BI should auto-detect the 4 relationships from the surrogate keys; verify each is many-to-one from `fact_sales` to the dimension table.
3. Paste in the 6 DAX measures from `dax_measures.md`.
4. Build report pages: a revenue/profit trend page (by month/year), a category breakdown page, a region/segment page.
5. Save as `.pbix`, take a couple of screenshots of the finished report for your portfolio.
