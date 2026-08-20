# Benchmark Results — Star Schema vs. Flat Denormalized Table

## Methodology

- Engine: DuckDB 1.5.5 (embedded columnar OLAP engine), single-threaded default settings, Parquet-backed tables
- Query: monthly revenue & profit by category (`GROUP BY year, month_name, category`) — a representative BI aggregation query
- Star schema version: 3-way join (fact_sales + dim_date + dim_product)
- Flat version: single denormalized table with all dimension attributes pre-joined
- Each query run 7 times back-to-back after warm cache; median reported (to avoid first-run I/O noise)

## Storage

| | Size |
|---|---|
| Star schema (fact + 4 dims, Parquet) | 12.14 MB |
| Flat denormalized table (Parquet) | 13.07 MB |
| Reduction (star vs. flat) | 7.1% |

## Query timing (932,100-row fact table)

| | Median of 7 runs |
|---|---|
| Star schema (3-way join) | 20.0 ms |
| Flat table (no joins) | 17.9 ms |

**Result: the flat table was ~2 ms faster (not the star schema).** At ~932K rows on a modern columnar engine, join elimination doesn't produce a measurable win — DuckDB's vectorized join implementation is cheap enough that the extra I/O/decompression from the wider flat table roughly cancels out the join cost. This is reported as-is; no adjustment was made to produce a more favorable number.

## Takeaway used on the resume/in interviews

The honest conclusion: at this specific scale, the star schema's value isn't raw query speed — it's semantic-layer reuse (one set of relationships + DAX measures serving many report visuals without re-deriving business logic per report, and a normalized model that's easier to maintain/extend). That's a defensible, real engineering tradeoff to discuss if asked, rather than an inflated performance claim.
