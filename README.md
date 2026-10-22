# Data Engineering Projects: Muralidhar Reddy Anumandla

Three batch data pipelines built around the Azure data stack: incremental loading, medallion layers, star-schema
modeling, dbt, Airflow, and a local-LLM assistant. Each project has an architecture diagram, a test suite and exact
commands to run it.

Contact: anumandlamuralidharreddy@gmail.com | [LinkedIn](https://www.linkedin.com/in/muralidhar-reddy-anumandla-070576241)

> **Read this first:** the data is synthetic or mocked and everything runs locally. Azure services are represented by
> pipeline-as-code (ADF JSON, T-SQL) and local stand-ins, which each project README maps back to the real service.
> No live Azure subscription was used. At work I run production pipelines on Azure Data Factory and Databricks; these
> repos are separate personal projects.

| Project | Stack | What it proves | Checked by |
|---|---|---|---|
| [Orders Lakehouse + LLM Assistant](./orders-lakehouse-adf-dbt-airflow) | ADF, Delta Lake, dbt, Airflow, Ollama | Idempotent paginated API ingestion, dbt star schema, Airflow orchestration, guarded LLM tool-calling | 4 ingestion tests, 19 dbt tests, 15 assistant tests |
| [Multi-Source Batch ETL & Data Warehouse](./multi-source-databricks-etl) | PySpark, Delta Lake | Bronze/Silver/Gold pipeline, Delta MERGE upserts, partition + Z-order benchmark, time travel | `pytest` suite, benchmark script |
| [Cloud-Based Batch ETL Pipeline](./cloud-batch-etl-pipeline) | Azure Data Factory, Azure SQL, T-SQL | Watermark incremental copy, MERGE stored procedure, index tuning | `pytest` suite, local index benchmark |

## How to read this repo

1. Start with the **Orders Lakehouse** README: it has the clearest architecture diagram and the most complete test story.
2. The **Multi-Source** project shows PySpark transformation logic and Delta Lake behavior (MERGE, partitioning, versioning).
3. The **Cloud Batch** project shows the ADF + T-SQL design, including the data-quality handling inside the MERGE procedure.

## Project summaries

**Orders Lakehouse + LLM Assistant.** An ADF pipeline designed from scratch (Lookup, Until-loop pagination, watermark
advanced only on full success) lands orders into Delta bronze. dbt builds `fct_orders` plus three dimensions with 19
tests, and Airflow runs the dbt layer. A local-LLM assistant answers questions about the pipeline through four
read-only tools: schema, SELECT-only SQL, dbt run results and the ingestion watermark.

**Multi-Source Batch ETL.** Daily CSV drops (about 120K-150K rows/day) and a relational source flow through
Bronze, Silver and Gold into a star schema. The fact table loads with a Delta MERGE, every stage retries with backoff,
and a failed stage stops the run instead of passing bad data forward.

**Cloud-Based Batch ETL.** Blob CSV drops are copied by an ADF watermark pipeline into Azure SQL staging, then a MERGE
stored procedure cleans, dedups and upserts into the star schema. Rows with a null `CustomerID` are logged to a
rejected-rows table instead of being silently dropped.

## Limitations

- Synthetic data and mock sources, not production data.
- Local stand-ins replace Databricks, ADF and Azure SQL when running the code.
- Performance figures come from local runs and are labeled as such in each README.
