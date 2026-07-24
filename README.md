# Data-engineering-My-Projects — Muralidhar Reddy Anumandla

Personal projects built during my M.S. in Computer Science at Virginia Commonwealth University. Both focus on batch ETL/ELT pipeline design on Azure — ingestion, transformation, incremental loading, and warehouse modeling.

Contact: anumandlamuralidharreddy@gmail.com | [LinkedIn](https://www.linkedin.com/in/anumandla-muralidhar-reddy)

---

## 1. Multi-Source Batch ETL & Data Warehouse on Databricks

**Stack:** Azure Databricks, PySpark, Delta Lake, Azure Data Factory, SQL

Built a batch ETL/ELT pipeline that ingests data from two different source types — flat files and a relational database — into a single Delta Lake star-schema data warehouse.

**What it does:**
- Ingests multi-source data (flat files + relational DB) into a unified bronze layer
- Transforms and conforms data into a star-schema model (fact + dimension tables) in Delta Lake
- Runs incremental merge/upsert loads rather than full reloads, to keep the pipeline efficient as data grows
- Orchestrated end-to-end via Azure Data Factory, scheduled to run daily
- Includes retry logic and failure alerting so failed runs don't go unnoticed

**Why this design:** star-schema modeling makes the warehouse straightforward to query for downstream reporting, and incremental merge/upsert logic avoids the cost and risk of reprocessing the full dataset on every run — the same pattern used in production ETL systems at scale.

---

## 2. Cloud-Based Batch ETL Pipeline

**Stack:** Azure Data Factory, Azure SQL

A second, more focused pipeline centered on incremental load design and query performance.

**What it does:**
- Designs incremental load logic to avoid reprocessing unchanged data
- Implements an indexing strategy on the target Azure SQL tables
- Achieved ~30% query performance improvement as a result of the indexing work

---

## Note on this repo

Code for both projects is being cleaned up for public release. This README documents the actual architecture and design decisions of each project. Reach out if you'd like to discuss implementation details directly.
