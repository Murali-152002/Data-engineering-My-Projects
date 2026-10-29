"""Monthly NYC taxi + weather pipeline.

One DAG run = one logical month (logical_date). catchup=True means a fresh deployment backfills every month
since start_date in order; re-running a past month is safe because every step is idempotent (replaceWhere + MERGE).

  reference -> bronze_silver (Delta, MERGE) -> verify (reconciliation) -> export_snapshot -> dbt_build (gold + tests)
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ.get("NYC_PROJECT_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
PY = os.environ.get("NYC_PYTHON", "python")
MONTH = "{{ logical_date.strftime('%Y-%m') }}"

with DAG(
    dag_id="nyc_mobility_monthly",
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 12, 31),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,  # months must land in order (silver MERGE is per-partition, but gold reads everything)
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["portfolio", "delta", "dbt"],
) as dag:
    reference = BashOperator(task_id="reference", bash_command=f"cd '{ROOT}/src' && '{PY}' reference.py")
    bronze_silver = BashOperator(task_id="bronze_silver", bash_command=f"cd '{ROOT}/src' && '{PY}' pipeline.py {MONTH}")
    verify = BashOperator(task_id="verify", bash_command=f"cd '{ROOT}/src' && '{PY}' verify.py")
    export = BashOperator(task_id="export_snapshot", bash_command=f"cd '{ROOT}/src' && '{PY}' export_snapshot.py")
    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"cd '{ROOT}/dbt_project' && DBT_PROFILES_DIR=. dbt build")
    reference >> bronze_silver >> verify >> export >> dbt_build
