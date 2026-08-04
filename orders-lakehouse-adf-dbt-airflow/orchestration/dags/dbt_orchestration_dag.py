"""
dbt_orchestration_dag.py
--------------------------
Orchestrates ONLY the dbt transformation layer (staging -> marts), scoped
deliberately: Azure Data Factory already owns ingestion (see adf/
pipeline_IncrementalOrdersIngestion.json) on its own Tumbling Window
Trigger. Stacking Airflow on top of ADF to do the SAME ingestion job would
be redundant tooling for no reason - the honest, common real-world split is
ADF for source-to-lake ingestion, Airflow for orchestrating the downstream
dbt transformation run, which is exactly what this DAG does.

Flow: wait for Bronze to actually have fresh data -> land it (idempotent,
safe to call every run) -> dbt run -> dbt test -> alert on failure.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

PROJECT_ROOT = "/tmp/proj"
DBT_DIR = f"{PROJECT_ROOT}/dbt_project"
DBT_ENV = {
    "PATH": "/sessions/happy-exciting-einstein/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "DBT_PROFILES_DIR": f"{DBT_DIR}/profiles",
    "BRONZE_ORDERS_PATH": f"{PROJECT_ROOT}/warehouse/bronze_orders",
    "SPARK_LOCAL_IP": "127.0.0.1",
}


def _bronze_has_new_landing_files(**context):
    """Sensor check: are there landing files newer than the last time this
    DAG successfully ran dbt? Mirrors a real production pattern - don't
    burn a dbt run on a schedule tick where nothing new actually landed."""
    import glob
    import os

    landing_glob = f"{PROJECT_ROOT}/landing/orders/run_date=*/*.json"
    files = glob.glob(landing_glob)
    if not files:
        return False
    marker = f"{PROJECT_ROOT}/orchestration/.last_dbt_run_marker"
    last_run = os.path.getmtime(marker) if os.path.exists(marker) else 0
    newest_file = max(os.path.getmtime(f) for f in files)
    return newest_file > last_run


def _land_to_bronze(**context):
    """Calls the same land_to_bronze logic used standalone - idempotent,
    safe to call even if nothing new landed since the last run."""
    import subprocess

    result = subprocess.run(
        ["python3", f"{PROJECT_ROOT}/ingestion/land_to_bronze.py"],
        cwd=f"{PROJECT_ROOT}/ingestion",
        env={**DBT_ENV},
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("land_to_bronze failed")


def _touch_success_marker(**context):
    import os
    marker = f"{PROJECT_ROOT}/orchestration/.last_dbt_run_marker"
    with open(marker, "w") as f:
        f.write(str(datetime.now()))


def _send_failure_alert(context):
    # Mirrors the same log-based alert pattern (stand-in for an Azure
    # Monitor Action Group / Slack webhook) used by the other two projects'
    # pipeline_utils.send_pipeline_alert.
    ti = context["task_instance"]
    print(f"[ALERT] dbt orchestration DAG failed at task '{ti.task_id}', run_id={context['run_id']}")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _send_failure_alert,
}

with DAG(
    dag_id="orders_dbt_orchestration",
    description="Orchestrates the dbt transformation layer for the orders lakehouse (ingestion is owned by ADF, not this DAG).",
    default_args=default_args,
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["dbt", "orders", "data-intelligence"],
) as dag:

    wait_for_new_data = PythonSensor(
        task_id="wait_for_new_landing_data",
        python_callable=_bronze_has_new_landing_files,
        poke_interval=30,
        timeout=60 * 5,
        mode="reschedule",
    )

    land_to_bronze = PythonOperator(
        task_id="land_to_bronze",
        python_callable=_land_to_bronze,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run",
        env=DBT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
        env=DBT_ENV,
    )

    mark_success = PythonOperator(
        task_id="mark_success",
        python_callable=_touch_success_marker,
    )

    wait_for_new_data >> land_to_bronze >> dbt_run >> dbt_test >> mark_success
