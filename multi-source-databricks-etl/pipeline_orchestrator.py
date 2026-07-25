"""
pipeline_orchestrator.py
-------------------------
Top-level entry point: runs Bronze -> Silver -> Gold in order, stopping and
alerting if any layer fails rather than letting a bad Bronze load silently
produce a bad Gold table.

In production this orchestration logic (order, retries, failure alerts) is
exactly what a Databricks Jobs multi-task pipeline or an ADF pipeline with
dependent activities does declaratively - this script is the same logic
expressed as plain Python so it can also just be run as one Databricks Job
task, or on a schedule via cron/Airflow/whatever the target environment
uses.

Usage:
    python pipeline_orchestrator.py               # bronze -> silver -> gold
    python pipeline_orchestrator.py --benchmark    # also runs the gold-layer
                                                    # partitioning/Z-order
                                                    # benchmark + time-travel demo
"""

import argparse
import sys
import time

from bronze_ingestion import run_bronze
from gold_star_schema import run_gold, run_partitioning_benchmark, run_time_travel_demo
from pipeline_utils import get_logger, get_spark, send_pipeline_alert
from silver_transform import run_silver


def run_pipeline(run_benchmark=False):
    logger, log_path = get_logger("pipeline_run")
    spark = get_spark()
    start = time.time()

    stages = [("bronze", run_bronze), ("silver", run_silver), ("gold", run_gold)]

    for name, stage_fn in stages:
        try:
            stage_fn(spark, logger)
        except Exception as exc:
            send_pipeline_alert(
                subject=f"Pipeline run failed at '{name}' stage",
                message=f"Run aborted after {name} raised: {exc}\nDownstream stages were NOT run.",
                logger=logger,
            )
            logger.error(f"Pipeline aborted at '{name}' stage. See {log_path} for details.")
            sys.exit(1)

    if run_benchmark:
        run_partitioning_benchmark(spark, logger)
        run_time_travel_demo(spark, logger)

    elapsed = time.time() - start
    logger.info(f"=== Full pipeline run complete in {elapsed:.1f}s. Log: {log_path} ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", action="store_true", help="Also run the Gold-layer partitioning/Z-order benchmark and time-travel demo.")
    args = parser.parse_args()
    run_pipeline(run_benchmark=args.benchmark)
