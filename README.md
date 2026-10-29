# Data Engineering Projects: Muralidhar Reddy Anumandla

Portfolio site: https://murali-152002.github.io/

Contact: anumandlamuralidharreddy@gmail.com | [LinkedIn](https://www.linkedin.com/in/muralidhar-reddy-anumandla-070576241)

Two pipelines built on **real public data**, each with tests, CI on GitHub Actions and every number measured on a real run. Older projects below use synthetic data and are labeled that way.

> **Honest scope:** everything runs locally (PySpark local mode, Delta Lake, DuckDB), with no cloud deployment yet. At work I run production pipelines on Azure Data Factory and Databricks; these repos are separate personal projects.

| Project | Data | Stack | Checked by |
|---|---|---|---|
| [NYC Mobility Incremental Pipeline](./nyc-mobility-incremental-pipeline) | 48.7M real NYC TLC taxi trips (12 months) + daily weather | PySpark, Delta Lake MERGE, dbt (DuckDB), Airflow, GitHub Actions | 37 dbt tests, pytest, late-data test, `airflow dags test` |
| [CMS Claims Lakehouse](./claims-lakehouse-cms-delta-dbt) | 1.56M rows of real CMS Medicare inpatient and Part D files | PySpark, Delta Lake MERGE, dbt (DuckDB), GitHub Actions | 32 dbt tests, 6 pytest tests, idempotent re-run |

## NYC Mobility Incremental Pipeline
Loads one month at a time with an idempotent Delta MERGE. Bad rows go to a quarantine table with a reason each: 48,722,602 bronze rows = 47,743,923 silver + 978,679 quarantined (2.0%). A late-arriving and corrected-data test inserted 19,380 rows and updated 4,938, ending identical to a clean load with 0 duplicates. A dbt gold layer passes 37 of 37 tests, an Airflow DAG orchestrates the run, and CI runs the pipeline on a real-data sample.

## CMS Claims Lakehouse
Bronze, silver and gold over two real CMS Medicare files (145,879 inpatient and 1,416,883 Part D rows), with a quarantine table and a check that fails the job if bronze does not equal silver plus quarantine. Silver is written with a Delta MERGE on the business key and a second run leaves counts unchanged. The dbt layer passes 32 data tests.

## Earlier projects (synthetic or mock data)

| Project | Stack | What it shows |
|---|---|---|
| [Orders Lakehouse + LLM Assistant](./orders-lakehouse-adf-dbt-airflow) | ADF, Delta Lake, dbt, Airflow, Ollama | Idempotent paginated API ingestion, dbt star schema, guarded LLM tool-calling |
| [Cloud-Based Batch ETL Pipeline](./cloud-batch-etl-pipeline) | Azure Data Factory, Azure SQL, T-SQL | Watermark incremental copy, MERGE stored procedure, index tuning |
| [E-commerce Sales Analytics (Power BI)](./ecommerce-sales-analytics-powerbi) | Power BI | Dashboard reporting |

## Limitations
- The two real-data projects run locally and the dbt gold layer reads a Parquet snapshot exported from Delta silver (DuckDB cannot read Delta offline here).
- Performance figures come from local runs on a 4-core, 4 GB VM and are labeled as such in each README.
- The earlier projects use synthetic or mock data and local stand-ins for Azure services.
