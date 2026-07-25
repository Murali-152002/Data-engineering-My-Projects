"""
pipeline_utils.py
------------------
Shared cross-cutting concerns used by every layer of the pipeline:

  - get_spark(): builds a local SparkSession with Delta Lake enabled. In an
    actual Azure Databricks job this function isn't needed - `spark` is
    already provided by the cluster runtime - but keeping the pipeline
    logic in plain functions (rather than notebook cells) makes it
    testable and portable.
  - retry_on_failure: a decorator implementing the same retry-with-backoff
    behavior configured on the Databricks Jobs / ADF pipeline activities
    in production (transient failures - a locked file, a brief network
    blip to the source DB - shouldn't fail the whole run).
  - send_pipeline_alert: stands in for an Azure Monitor / Log Analytics
    alert firing to an Action Group (email/Teams/PagerDuty). Locally it
    logs what would have been sent so the alerting *path* is still
    exercised and testable.
  - get_logger: consistent run logging to both stdout and a log file per
    run, which is what you'd tail in production when a job fails.
"""

import functools
import logging
import os
import time
from datetime import datetime

from config import LOG_DIR, MAX_RETRIES, RETRY_BACKOFF_SECONDS, ALERT_RECIPIENTS


def get_spark(app_name="multi-source-etl"):
    """
    Builds a local SparkSession for transformations. Delta Lake I/O is
    handled separately by delta_io.py (see that module's docstring for why:
    short version, this keeps the project runnable without a JVM Maven
    dependency for the Delta library, while every transformation below is
    still real, distributed PySpark).
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")  # small local runs don't need 200 shuffle partitions
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_logger(run_name):
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"{run_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger(run_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger, log_path


def retry_on_failure(max_retries=MAX_RETRIES, backoff_seconds=RETRY_BACKOFF_SECONDS):
    """
    Decorator that retries a transient-failure-prone step (file read, DB
    connection, etc.) with linear backoff, mirroring the retry policy set
    on ADF pipeline activities / Databricks Jobs tasks in production.
    Raises the final exception (and lets the caller alert + fail the run)
    only after all attempts are exhausted.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__) or logging
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - intentionally broad; this is a generic retry wrapper
                    last_exc = exc
                    logging.warning(
                        f"[retry] '{func.__name__}' failed on attempt {attempt}/{max_retries}: {exc}"
                    )
                    if attempt < max_retries:
                        time.sleep(backoff_seconds)
            raise last_exc
        return wrapper
    return decorator


def send_pipeline_alert(subject, message, logger=None):
    """
    Stand-in for an Azure Monitor alert -> Action Group email notification.
    In production this would be a call to the Azure Monitor REST API (or
    simply raising the exception and letting a configured Databricks Job
    /ADF alert rule handle notification) - the pipeline code itself just
    needs to make sure failures are surfaced, not silently swallowed.
    """
    log = logger or logging
    log.error(f"[ALERT] To: {ALERT_RECIPIENTS} | Subject: {subject}\n{message}")
