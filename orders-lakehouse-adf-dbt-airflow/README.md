# Orders Lakehouse: ADF + Databricks/Delta Lake + dbt + Airflow

An incremental, API-sourced ingestion pipeline (Azure Data Factory, designed
from scratch) landing into a Delta Lake bronze layer, transformed into a
star-schema mart entirely in dbt (dbt-databricks-style modeling, real tests,
docs/lineage), with the dbt run orchestrated by Airflow. Built to close a
specific gap: prior projects proved production Databricks/PySpark work and an
ADF-to-SQL pipeline, but never an ADF pipeline **designed from scratch**
(parameterized, incremental, idempotent) or a dbt/Airflow-based
transformation layer - both real, common pieces of a modern data platform
that weren't demonstrated yet.

## Architecture

```
Internal Orders REST API (mock_orders_api.py)
        │  paginated, modified_at watermark filter
        ▼
Azure Data Factory  (adf/pipeline_IncrementalOrdersIngestion.json)
  Lookup last watermark → Until-loop pagination → land JSON pages
  → advance watermark ONLY if every page succeeded (idempotent-by-construction)
        │
        ▼
Landing zone (ADLS Gen2, simulated as ./landing/)
        │
        ▼
Databricks / Delta Lake bronze layer  (ingestion/land_to_bronze.py)
  raw, minimally-typed, append-only, file-tracked idempotent ingestion
        │
        ▼
dbt  (dbt_project/)                        ◄── orchestrated by
  staging: type-cast, dedupe (latest        Airflow (orchestration/dags/
  wins), drop unjoinable rows                dbt_orchestration_dag.py)
  marts: dim_customer, dim_product,          sensor → land_to_bronze → dbt run
  dim_date, fct_orders (star schema)         → dbt test → mark success
  19 dbt tests: not_null / unique /
  relationships, dbt docs generate for
  lineage
```

## Why Airflow orchestrates dbt specifically, not ingestion

ADF already owns ingestion end-to-end on its own Tumbling Window Trigger.
Adding Airflow on top of that same job would just be two orchestrators
doing the same work for no reason. The split here is a genuinely common
real-world pattern instead: ADF for source-to-lake ingestion, Airflow for
orchestrating the downstream dbt transformation run - Airflow triggering
dbt jobs is, in a lot of enterprises, more common than relying on dbt
Cloud's own scheduler. Each tool is doing the job it's actually good at.

## Local substitutes (personal project, no live Azure subscription)

Same honest-substitution pattern as the other two projects in this repo -
real logic, different execution target, clearly labeled:

| Real Azure service | Local stand-in | Why |
|---|---|---|
| Internal REST API | `api_source/mock_orders_api.py` (Flask) | No real internal API to hit; this one has real pagination + a `modified_since` watermark contract, not a toy |
| Azure Data Factory | `local_demo/run_adf_ingestion_simulation.py` | ADF only executes inside Azure; this script runs the *exact* logic described in the pipeline JSON so it's provable, not just described |
| ADLS Gen2 landing zone | `./landing/` folder | Plain filesystem instead of Blob FS |
| Databricks Delta Lake | `deltalake` (delta-rs, zero-JVM) via `ingestion/delta_io.py` | Same substitution used in `multi-source-databricks-etl` |
| Databricks SQL warehouse (dbt target) | DuckDB via `dbt-duckdb`, reading the Delta table's Parquet files directly | A live Databricks SQL warehouse needs an Azure workspace; dbt-duckdb runs the *exact same* dbt models/tests/docs, only the query engine differs |
| Azure Monitor Action Group alert | Log-based alert (`_send_failure_alert` in the DAG) | Same stand-in pattern as the other projects' `send_pipeline_alert` |

The ADF pipeline JSON itself (`adf/`) is the real, deployable artifact - the
same "pipeline-as-code" representation ADF exports via Git integration / ARM
templates, identical in spirit to `cloud-batch-etl-pipeline/adf/`.

## Idempotency & failure-safety, proven not asserted

- The watermark only advances in `AdvanceWatermarkOnSuccess`, which only
  runs if the entire pagination loop succeeded - a failed run never
  advances it, so re-running after a failure safely re-pulls the same
  window instead of silently skipping data.
- `tests/test_ingestion.py` proves this against the real mock API: full
  pagination pulls the correct total, an immediate re-run pulls 0 new rows,
  a simulated mid-pagination failure leaves the watermark untouched, and a
  status-change update (not just a new row) is correctly caught by the
  `modified_at` watermark on the next pull.
- `ingestion/land_to_bronze.py` is separately idempotent on the Bronze side
  via file-tracking (`_source_file`), so calling it twice never double-lands
  the same file - proven by running it back-to-back in the same session.

## Running it

Requires Python 3.10+, Java 11/17 (for Spark, same local-hostname quirk as
the other projects applies: prefix with `SPARK_LOCAL_IP=127.0.0.1` if you
hit a `java.net.UnknownHostException`).

```bash
pip install -r requirements.txt

# 1. Start the mock API (separate terminal, or background it)
cd api_source && python3 mock_orders_api.py &

# 2. Run the ADF pipeline simulation twice - second run proves idempotency
cd ../local_demo && SPARK_LOCAL_IP=127.0.0.1 python3 run_adf_ingestion_simulation.py

# 3. Land Bronze
cd ../ingestion && SPARK_LOCAL_IP=127.0.0.1 python3 land_to_bronze.py

# 4. Run dbt (staging + marts + tests + docs)
cd ../dbt_project
export DBT_PROFILES_DIR=./profiles
export BRONZE_ORDERS_PATH=../warehouse/bronze_orders
dbt run && dbt test && dbt docs generate

# 5. Run the whole thing as one Airflow DAG
cd ..
export AIRFLOW_HOME=./orchestration/airflow_home
airflow db migrate
cp orchestration/dags/dbt_orchestration_dag.py $AIRFLOW_HOME/dags/
airflow dags test orders_dbt_orchestration $(date +%F)

# Test suite
pytest tests/ -v
```

## Star schema

`fct_orders` (grain: one row per order) joins to `dim_customer`,
`dim_product`, and `dim_date`. `dim_customer` and `dim_product` are built
from observed order activity rather than a separate master data source,
since the mock API only exposes orders - a realistic constraint (not every
project has a clean master-data feed to join against), handled honestly
rather than fabricating a customer/product source that doesn't exist here.

19 dbt tests cover primary-key uniqueness/not-null on every model and
referential integrity (`relationships`) from `fct_orders` to all three
dimensions.
