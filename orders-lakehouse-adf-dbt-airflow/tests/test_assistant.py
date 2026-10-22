"""
Tests for the Pipeline Run Assistant. No LLM is needed: a scripted fake model
proves the tool-calling loop and, more importantly, that the guardrails hold.
"""
import json
import os
import sys

import duckdb
import pytest

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)
from assistant import agent, tools  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    path = str(tmp_path / "wh.duckdb")
    con = duckdb.connect(path)
    con.execute("create table fct_orders (order_id int, order_status varchar, gross_amount double)")
    con.execute("insert into fct_orders values (1,'shipped',10.5),(2,'shipped',20),(3,'cancelled',5)")
    con.close()
    return path


class FakeLLM:
    """Plays back scripted assistant messages."""

    def __init__(self, script):
        self.script, self.seen = list(script), []

    def chat(self, messages):
        self.seen.append(list(messages))
        return self.script.pop(0)


def tool_call(name, **args):
    return {"role": "assistant", "content": "", "tool_calls": [{"function": {"name": name, "arguments": args}}]}


def test_select_runs_and_is_limited(db):
    out = tools.run_sql("select order_status, count(*) n from fct_orders group by 1 order by 1", db)
    assert out["rows"] == [["cancelled", "1"], ["shipped", "2"]]


@pytest.mark.parametrize("sql", [
    "drop table fct_orders",
    "delete from fct_orders",
    "select 1; drop table fct_orders",
    "insert into fct_orders values (9,'x',1)",
    "with a as (select 1) create table t as select * from a",
    "copy fct_orders to '/tmp/x.csv'",
])
def test_unsafe_sql_is_refused(sql, db):
    with pytest.raises(tools.UnsafeQuery):
        tools.run_sql(sql, db)


def test_data_is_unchanged_after_refused_queries(db):
    with pytest.raises(tools.UnsafeQuery):
        tools.run_sql("delete from fct_orders", db)
    assert tools.run_sql("select count(*) from fct_orders", db)["rows"] == [["3"]]


def test_connection_is_read_only(db):
    con = tools._connect(db)
    with pytest.raises(Exception):
        con.execute("delete from fct_orders")
    con.close()


def test_row_limit(tmp_path):
    path = str(tmp_path / "big.duckdb")
    con = duckdb.connect(path)
    con.execute("create table t as select range as i from range(500)")
    con.close()
    assert len(tools.run_sql("select * from t", path)["rows"]) == tools.MAX_ROWS


def test_last_dbt_run_reports_failures(tmp_path):
    p = tmp_path / "run_results.json"
    p.write_text(json.dumps({"metadata": {"generated_at": "2026-10-22"}, "elapsed_time": 3.2, "results": [
        {"unique_id": "model.x.fct_orders", "status": "success"},
        {"unique_id": "test.x.unique_fct_orders_order_id", "status": "fail", "message": "Got 2 results"}]}))
    out = tools.last_dbt_run(str(p))
    assert out["status_counts"] == {"success": 1, "fail": 1}
    assert out["problems"][0]["node"].endswith("unique_fct_orders_order_id")


def test_missing_files_return_errors_not_crashes(tmp_path):
    assert "error" in tools.last_dbt_run(str(tmp_path / "nope.json"))
    assert "error" in tools.ingestion_status(str(tmp_path / "nope.json"))


def test_agent_loop_answers_from_tool_results(db, monkeypatch):
    monkeypatch.setattr(tools, "DB_PATH", db)
    llm = FakeLLM([
        tool_call("describe_tables"),
        tool_call("run_sql", sql="select count(*) from fct_orders where order_status='shipped'"),
        {"role": "assistant", "content": "2 orders shipped."},
    ])
    assert agent.ask("how many shipped?", llm, verbose=False) == "2 orders shipped."
    last_tool_msg = llm.seen[-1][-1]
    assert last_tool_msg["role"] == "tool" and '"2"' in last_tool_msg["content"]


def test_agent_survives_a_refused_query(db, monkeypatch):
    monkeypatch.setattr(tools, "DB_PATH", db)
    llm = FakeLLM([tool_call("run_sql", sql="drop table fct_orders"),
                   {"role": "assistant", "content": "I can only read data."}])
    assert agent.ask("delete everything", llm, verbose=False) == "I can only read data."
    assert "query refused" in llm.seen[-1][-1]["content"]


def test_agent_stops_at_step_limit(db, monkeypatch):
    monkeypatch.setattr(tools, "DB_PATH", db)
    llm = FakeLLM([tool_call("describe_tables")] * 20)
    assert "step limit" in agent.ask("loop forever", llm, verbose=False)
