"""
delta_io.py
-----------
Read/write helpers that bridge PySpark DataFrames and Delta Lake tables.

A note on the implementation, since it's a deliberate design choice:

On actual Azure Databricks, Delta Lake read/write is just
`df.write.format("delta")...` because the Databricks runtime ships the
Delta JVM library out of the box. Running Spark *locally* outside of
Databricks normally pulls that same JVM library from Maven Central at
startup - which isn't available in every environment (e.g. restricted
network/CI sandboxes). To keep this project runnable anywhere with zero
external Java dependencies, these helpers do the transformation logic in
PySpark (real distributed DataFrame operations - joins, aggregations,
window functions) and then hand the result to `deltalake` (the delta-rs
Rust engine, pip-installable, zero JVM) at the point of writing to disk.

`deltalake` speaks the exact same open Delta Lake table format Databricks
uses (transaction log, Parquet data files, MERGE, time travel) - it's a
different *writer engine* for the identical *table format*, not a
different technology. This module is the only place that distinction
lives; every pipeline script above it just calls read_delta/write_delta/
merge_delta and doesn't need to know the difference.
"""

import os

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


def write_delta(spark_df, path, mode="append", partition_by=None):
    """Writes a Spark DataFrame to a Delta table at `path`."""
    arrow_table = pa.Table.from_pandas(spark_df.toPandas(), preserve_index=False)
    write_deltalake(path, arrow_table, mode=mode, partition_by=partition_by)


def read_delta(spark, path, version=None):
    """
    Reads a Delta table back into a Spark DataFrame.
    `version=N` reads a historical snapshot (Delta time travel) - used by
    the rollback demo in gold_star_schema.py.
    """
    if not os.path.exists(path):
        return None
    dt = DeltaTable(path, version=version) if version is not None else DeltaTable(path)
    return spark.createDataFrame(dt.to_pandas())


def delta_table_exists(path):
    return os.path.exists(os.path.join(path, "_delta_log"))


def merge_delta(spark_df, path, merge_keys, update_columns=None):
    """
    Incremental MERGE (upsert): update matching rows on `merge_keys`,
    insert new ones. This is the same MERGE INTO semantics as
    `DeltaTable.forPath(spark, path).alias("t").merge(...)` on Databricks -
    delta-rs exposes the identical operation via its Python API.

    If the target table doesn't exist yet, this is just the initial load
    (a plain write).
    """
    pdf = spark_df.toPandas()
    arrow_table = pa.Table.from_pandas(pdf, preserve_index=False)

    if not delta_table_exists(path):
        write_deltalake(path, arrow_table, mode="overwrite")
        return len(pdf), 0  # (rows inserted, rows updated) - initial load, all inserts

    dt = DeltaTable(path)
    predicate = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    update_columns = update_columns or [c for c in pdf.columns if c not in merge_keys]
    update_set = {c: f"source.{c}" for c in update_columns}
    insert_set = {c: f"source.{c}" for c in pdf.columns}

    (
        dt.merge(
            source=arrow_table,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update(updates=update_set)
        .when_not_matched_insert(updates=insert_set)
        .execute()
    )
    return None, None  # delta-rs merge doesn't return row-level counts in this version
