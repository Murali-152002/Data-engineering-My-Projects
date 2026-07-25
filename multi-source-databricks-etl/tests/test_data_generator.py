"""
Regression test for a real bug found while validating this project: the
daily TransactionID scheme is `YYYYMMDD * MULTIPLIER + sequence`. If
MULTIPLIER is smaller than the largest possible daily row count, two
different days' ID ranges overlap and the Silver layer's dedup-on-
TransactionID logic wrongly collapses distinct transactions from
different days into "duplicates" (this actually happened during
development: raising daily volume to 120K+ rows with the old 10,000
multiplier silently discarded ~71% of rows as false-positive dupes).
"""

import csv
import os
import shutil
import sys
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_consecutive_days_do_not_share_transaction_ids():
    import config
    import data_generator

    tmp_dir = tempfile.mkdtemp(prefix="datagen_test_")
    try:
        landing_dir = os.path.join(tmp_dir, "landing")
        config.LANDING_DIR = landing_dir
        data_generator.LANDING_DIR = landing_dir

        day1 = datetime(2026, 7, 20)
        day2 = datetime(2026, 7, 21)

        f1 = data_generator.generate_daily_csv(day1, min_rows=140000, max_rows=140000, inject_dirty_data=False)
        f2 = data_generator.generate_daily_csv(day2, min_rows=140000, max_rows=140000, inject_dirty_data=False)

        ids_day1 = {int(r["TransactionID"]) for r in csv.DictReader(open(f1))}
        ids_day2 = {int(r["TransactionID"]) for r in csv.DictReader(open(f2))}

        assert len(ids_day1 & ids_day2) == 0, (
            "TransactionID ranges for two different days overlap - this reintroduces "
            "the false-duplicate bug in silver_transform.py's dedup logic."
        )
        assert len(ids_day1) == 140000
        assert len(ids_day2) == 140000
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_daily_volume_is_120k_plus():
    """Sanity check that the default generator config actually produces
    120K+ rows/day, the daily volume this project is designed around."""
    import data_generator

    assert data_generator.generate_daily_csv.__defaults__[2] >= 120000  # min_rows default
