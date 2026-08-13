# Data Engineering Projects — Muralidhar Reddy Anumandla

Personal projects focused on batch ETL/ELT pipeline design on Azure — ingestion, transformation, incremental loading, and warehouse modeling.

Contact: anumandlamuralidharreddy@gmail.com | [LinkedIn](https://www.linkedin.com/in/anumandla-muralidhar-reddy)

---

## 1. [Multi-Source Batch ETL & Data Warehouse on Databricks](./multi-source-databricks-etl)

**Stack:** PySpark, Delta Lake, Azure Databricks (target), SQL

A medallion-architecture (Bronze/Silver/Gold) pipeline that ingests daily
CSV drops and an operational relational source into a Delta Lake
star-schema warehouse, with incremental `MERGE` loads, a real
partitioning + Z-ordering benchmark, and a Delta time-travel/rollback demo
— all runnable end-to-end locally (`pipeline_orchestrator.py`), with a
`pytest` suite covering the retry logic and the data-cleaning rules.

**Why this design:** star-schema modeling keeps the warehouse easy to query
downstream, and MERGE-based incremental loads avoid reprocessing the full
dataset on every run — the same pattern production ETL systems use at
scale. See the [project README](./multi-source-databricks-etl/README.md)
for the full architecture, the exact commands to run it, and the reasoning
behind one notable design decision: a zero-JVM Delta Lake writer engine
used for local development.

---

## 2. [Cloud-Based Batch ETL Pipeline](./cloud-batch-etl-pipeline)

**Stack:** Azure Data Factory, Azure SQL, T-SQL

A watermark-driven incremental pipeline design: Blob Storage CSV drops →
ADF Copy Activity → Azure SQL staging → a `MERGE`-based stored procedure
that cleans and upserts into a star schema, with nonclustered indexes
tuned for the reporting queries built on top of it.

**What it does:**
- ADF pipeline-as-code (linked services, datasets, pipeline JSON) implementing the standard watermark incremental-copy pattern, with retry policy and a failure-alert branch
- T-SQL star schema, staging/watermark tables, and a `MERGE` stored procedure handling null cleanup, dedup, and type coercion
- Nonclustered indexes on `CustomerID` and `OrderDate` — the two columns the reporting queries actually filter/join on
- A runnable local proof (`local_demo/`, SQLite standing in for Azure SQL) that empirically measures the index performance improvement rather than just asserting it, plus a `pytest` suite for the cleaning logic

See the [project README](./cloud-batch-etl-pipeline/README.md) for the full
design, how to run the local proof, and what the measured "~30% query
performance improvement" figure is based on.

---

## Implementation notes

Both projects were developed and tested locally without live Azure
resources (no Databricks workspace, no ADF instance, no Azure SQL
Database). Where that mattered, each project's README documents the local
substitution used, why, and how it maps back to the real Azure service.
The design decisions themselves — medallion architecture, MERGE-based
incremental loads, watermark patterns, indexing strategy — are the same
ones used in production Azure data platforms.

**3. Orders Lakehouse: ADF + Databricks/Delta Lake + dbt + Airflow**

**Stack: Azure Data Factory, Delta Lake, dbt, Airflow**

A parameterized, incremental ADF pipeline (Lookup + Until-loop pagination) ingesting a REST API into a Delta Lake bronze layer, transformed into a dbt star-schema mart (fct_orders + 3 dimensions, 19 tests, generated lineage docs), with the dbt run orchestrated by a dedicated Airflow DAG.

Why this design: ADF already owns ingestion end-to-end on its own trigger — stacking Airflow on the same job would just duplicate it. Splitting responsibilities (ADF for source-to-lake ingestion, Airflow for orchestrating the downstream dbt run) mirrors how a lot of enterprises actually pair the two tools. See the project orders-lakehouse-adf-dbt-airflow/README.md for the full architecture, the idempotency proof, and the local-substitution table.
