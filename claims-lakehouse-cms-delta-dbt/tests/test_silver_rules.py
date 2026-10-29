"""Unit tests for the validation logic, run on tiny hand-made frames (no CMS file needed)."""
import os, sys, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pyspark.sql import SparkSession, functions as F

@pytest.fixture(scope="session")
def sp():
    s = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    yield s; s.stop()

def rules():
    return {
        "bad_ccn_format": ~F.col("ccn").rlike(r"^[0-9A-Z]{6}$"),
        "medicare_exceeds_total": F.col("mdcr") > F.col("tot"),
        "below_floor": F.col("dsch") < 11,
        "payment_exceeds_charge": F.col("tot") > F.col("chg"),
    }

def hits(sp, rows):
    df = sp.createDataFrame(rows, "ccn string, dsch int, chg double, tot double, mdcr double")
    return {n: df.filter(c).count() for n, c in rules().items()}

def test_clean_row_passes_all_rules(sp):
    assert sum(hits(sp, [("010001", 20, 1000.0, 500.0, 400.0)]).values()) == 0

def test_leading_zero_ccn_is_kept_as_string(sp):
    assert hits(sp, [("010001", 20, 1000.0, 500.0, 400.0)])["bad_ccn_format"] == 0

def test_short_ccn_is_caught(sp):
    assert hits(sp, [("1001", 20, 1000.0, 500.0, 400.0)])["bad_ccn_format"] == 1

def test_medicare_above_total_is_caught(sp):
    assert hits(sp, [("010001", 20, 1000.0, 500.0, 600.0)])["medicare_exceeds_total"] == 1

def test_discharges_below_cms_floor_is_caught(sp):
    assert hits(sp, [("010001", 5, 1000.0, 500.0, 400.0)])["below_floor"] == 1

def test_payment_above_charge_is_soft_flag(sp):
    assert hits(sp, [("010001", 20, 100.0, 500.0, 400.0)])["payment_exceeds_charge"] == 1
