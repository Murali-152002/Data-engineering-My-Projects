# Multi-Source Batch ETL & Data Warehouse on Databricks

A PySpark + Delta Lake batch pipeline that ingests daily sales data from two
different source types, cleans it, and loads it into a star-schema data
warehouse using a Bronze / Silver / Gold medallion architecture. Built to
mirror how this would actually run as a scheduled Databricks Job.

## What it does

Two sources land daily:

- **Flat files**: CSV drops in a landing zone (stand-in for ADLS Gen2 / Blob Storage), ~120K-150K sales rows/day by default in `data_generator.py`
- **Relational**: an operational `customers` / `products` source (SQLite standing in for Azure SQL), 5,000 customers / 300 products

The pipeline ingests both, cleans and standardizes the data, and loads it
into a star schema:

```
Bronze (raw, append-only)  →  Silver (cleaned, typed, deduplicated)  →  Gold (star schema)

sample_data/landing/*.csv ─┐
                            ├─→ bronze_sales_transactions ─→ silver_sales_transactions ─→ fact_sales
operational_source.db ─────┘        bronze_customers ─→ silver_customers ─→ dim_customer
                                     bronze_products  ─→ silver_products  ─→ dim_product
                                                                              dim_date (derived)
```

- **Bronze**: lands raw data with minimal processing. Sales CSVs are ingested
  incrementally (only new files, tracked via `_source_file`); reference
  tables are refreshed as full snapshots each run.
- **Silver**: drops rows with a null/empty `CustomerID` (can't be joined to a
  dimension), deduplicates on `TransactionID` (keeping the most recently
  ingested version), coerces types (including a `"3.0"`-style float-string
  quantity bug the synthetic data deliberately injects), and standardizes
  string fields.
- **Gold**: builds the star schema and loads `fact_sales` incrementally via
  a real Delta `MERGE` (upsert on `TransactionID`) — a re-run that corrects
  an already-loaded transaction updates that row instead of duplicating it.

Every stage runs through a `retry_on_failure` decorator (linear backoff,
mirrors ADF/Databricks Jobs retry policy) and raises an alert (logged,
stands in for an Azure Monitor → Action Group email) on unrecoverable
failure, and the orchestrator stops the run rather than letting a bad
Bronze load silently flow into Gold.

## Also demonstrated (and actually run, not just described)

- **Partitioning + Z-ordering benchmark** (`gold_star_schema.py --benchmark`):
  writes `fact_sales` two ways — a single-file baseline and a copy
  partitioned by `OrderDate` and Z-ordered on `CustomerID` — then times the
  same filter+aggregate query against both. Typically 40-60% faster on the
  optimized layout in local runs (varies by data volume/hardware).
- **Delta time travel / rollback**: reads an older version of `fact_sales`
  straight from the Delta transaction log and restores the table to it,
  proving versioning is real and queryable, not just a comment in the code.

## Design decision: Delta Lake writer engine

Delta Lake on actual Azure Databricks works out of the box because the
runtime ships the JVM Delta library. Running Spark locally normally pulls
that same library from Maven Central at startup, which isn't always
available in restricted or offline environments. Rather than fake Delta
behavior with plain Parquet, this project uses [`deltalake`](https://pypi.org/project/deltalake/)
(delta-rs, a Rust-native implementation, pip-installable with zero JVM
dependency) as the **writer engine**. PySpark still does 100% of the actual
transformation logic — reads, joins, window functions, aggregations; `delta-rs`
only handles persistence (read/write/merge) to the identical open Delta Lake
table format Databricks uses (transaction log, Parquet files, ACID commits).
See `delta_io.py` for the full explanation and the three functions
(`read_delta`, `write_delta`, `merge_delta`) every other script calls through.
On Databricks itself, swapping this module out for native
`df.write.format("delta")` calls is a small, mechanical change — the
transformation code above it doesn't change at all.

## Project structure

```
config.py                  Central config (paths, retry policy, alert recipients)
data_generator.py          Generates synthetic source data (with injected data-quality issues)
pipeline_utils.py          get_spark / get_logger / retry_on_failure / send_pipeline_alert
delta_io.py                Delta Lake read/write/merge bridge (see design decision above)
bronze_ingestion.py        Bronze layer
silver_transform.py        Silver layer
gold_star_schema.py        Gold layer (star schema, MERGE, benchmark, time-travel demo)
pipeline_orchestrator.py   Runs Bronze → Silver → Gold, stops + alerts on failure
tests/                     pytest suite (retry logic, Silver data-quality rules, TransactionID generation)
```

## Running it

Requires Python 3.10+ and Java 11/17 (for Spark). Tested with the versions
pinned in `requirements.txt`.

```bash
pip install -r requirements.txt

# Generate a few days of synthetic source data (~120K-150K rows/day, CSV drops + operational DB)
python data_generator.py --days 5

# Run the full pipeline: Bronze -> Silver -> Gold
python pipeline_orchestrator.py

# Also run the partitioning/Z-order benchmark and time-travel demo
python pipeline_orchestrator.py --benchmark

# Run the test suite
pytest tests/ -v
```

**Local-environment note:** on some machines Spark's local hostname
resolution needs a nudge — if you hit a `java.net.UnknownHostException`
on startup, prefix commands with `SPARK_LOCAL_IP=127.0.0.1`. This is a
local-dev-only quirk; it doesn't apply on an actual Databricks cluster.

**A note on data volume and TransactionID generation:** rows default to
~120K-150K per day to reflect realistic daily transaction volume, and each
day's `TransactionID`s are generated as `YYYYMMDD * 1,000,000 + sequence`
so that two different days can never collide on ID at this volume (a
smaller multiplier works fine at toy data sizes but causes IDs to collide
across days once volume scales up, which then silently mis-flags valid
rows as duplicates in the Silver dedup logic — `tests/test_data_generator.py`
has a regression test guarding against this). At this row count, the full
pipeline (including the benchmark, which writes two extra full copies of
`fact_sales` and runs a real Z-order) takes on the order of 1-2 minutes
end-to-end locally, not seconds — that's expected.

Re-running `pipeline_orchestrator.py` is safe: Bronze ingestion is
incremental and idempotent (already-seen files are skipped), and the Gold
`fact_sales` load is a real MERGE, so re-running the same day's data updates
rather than duplicates.
