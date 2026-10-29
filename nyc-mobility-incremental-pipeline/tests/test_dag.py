"""DAG integrity: imports cleanly, one task per stage, in the expected order, backfill enabled."""
import os

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=DAGS, include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.get_dag("nyc_mobility_monthly")


def test_task_order(dag):
    order = ["reference", "bronze_silver", "verify", "export_snapshot", "dbt_build"]
    assert [t.task_id for t in dag.topological_sort()] == order


def test_backfill_settings(dag):
    assert dag.catchup is True
    assert dag.max_active_runs == 1
