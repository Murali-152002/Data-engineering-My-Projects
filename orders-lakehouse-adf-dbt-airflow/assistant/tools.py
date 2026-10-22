"""
Tools the Pipeline Run Assistant is allowed to call. The LLM never touches the
database or the filesystem directly: it can only ask for one of these four
functions, and every function is read-only and bounded.
"""
import json
import os
import re

import duckdb

ROOT = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.environ.get("ORDERS_DB", os.path.join(ROOT, "warehouse", "orders_intelligence.duckdb"))
RUN_RESULTS = os.environ.get(
    "DBT_RUN_RESULTS", os.path.join(ROOT, "dbt_project", "target", "run_results.json"))
WATERMARK = os.environ.get("ORDERS_WATERMARK", os.path.join(ROOT, "control", "watermark.json"))

MAX_ROWS = 50
_READ_ONLY = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|attach|copy|pragma|install|load|export|call)\b",
    re.IGNORECASE)


class UnsafeQuery(ValueError):
    pass


def check_sql(sql: str) -> str:
    """Allow exactly one SELECT/WITH statement. Anything else is refused."""
    stripped = sql.strip().rstrip(";").strip()
    if ";" in stripped:
        raise UnsafeQuery("only one statement is allowed")
    if not _READ_ONLY.match(stripped):
        raise UnsafeQuery("only SELECT queries are allowed")
    if _FORBIDDEN.search(stripped):
        raise UnsafeQuery("query contains a forbidden keyword")
    return stripped


def _connect(db_path=None):
    # read_only=True is a second guard: even a query that slipped past
    # check_sql could not change the warehouse.
    return duckdb.connect(db_path or DB_PATH, read_only=True)


def describe_tables(db_path=None) -> dict:
    con = _connect(db_path)
    try:
        rows = con.execute(
            "select table_name, column_name, data_type from information_schema.columns "
            "where table_schema = 'main' order by table_name, ordinal_position").fetchall()
    finally:
        con.close()
    out = {}
    for table, col, dtype in rows:
        out.setdefault(table, []).append(f"{col} {dtype}")
    return out


def run_sql(sql: str, db_path=None) -> dict:
    safe = check_sql(sql)
    con = _connect(db_path)
    try:
        cur = con.execute(f"select * from ({safe}) limit {MAX_ROWS}")
        cols = [d[0] for d in cur.description]
        rows = [[str(v) if v is not None else None for v in r] for r in cur.fetchall()]
    finally:
        con.close()
    return {"sql": safe, "columns": cols, "rows": rows, "row_limit": MAX_ROWS}


def last_dbt_run(path=None) -> dict:
    path = path or RUN_RESULTS
    if not os.path.exists(path):
        return {"error": "no dbt run results found; run `dbt run && dbt test` first"}
    with open(path) as f:
        data = json.load(f)
    results = data.get("results", [])
    counts, problems = {}, []
    for r in results:
        status = r.get("status")
        counts[status] = counts.get(status, 0) + 1
        if status not in ("success", "pass"):
            problems.append({"node": r.get("unique_id"), "status": status,
                             "message": (r.get("message") or "")[:300]})
    return {"generated_at": data.get("metadata", {}).get("generated_at"),
            "elapsed_seconds": data.get("elapsed_time"),
            "status_counts": counts, "problems": problems}


def ingestion_status(path=None) -> dict:
    path = path or WATERMARK
    if not os.path.exists(path):
        return {"error": "no watermark file found; ingestion has not run yet"}
    with open(path) as f:
        return json.load(f)


TOOLS = {
    "describe_tables": describe_tables,
    "run_sql": run_sql,
    "last_dbt_run": last_dbt_run,
    "ingestion_status": ingestion_status,
}

# Schemas handed to the LLM (Ollama / OpenAI-style function calling).
TOOL_SCHEMAS = [
    {"type": "function", "function": {
        "name": "describe_tables",
        "description": "List the warehouse tables and their columns. Call this before writing SQL.",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "run_sql",
        "description": "Run ONE read-only SELECT query (DuckDB SQL) on the warehouse. Max 50 rows.",
        "parameters": {"type": "object", "properties": {
            "sql": {"type": "string", "description": "a single SELECT or WITH query"}},
            "required": ["sql"]}}},
    {"type": "function", "function": {
        "name": "last_dbt_run",
        "description": "Summarise the latest dbt run/test results, including any failures.",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "ingestion_status",
        "description": "Show the ingestion watermark: how far the orders pull has advanced.",
        "parameters": {"type": "object", "properties": {}}}},
]
