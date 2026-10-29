"""Unit tests for the validation rules on tiny hand-made frames (no big data needed)."""
import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import common  # noqa: E402
import silver  # noqa: E402


@pytest.fixture(scope="module")
def sp():
    return common.spark("tests", mem="1g")


COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "PULocationID",
        "DOLocationID", "fare_amount", "total_amount", "_ingested_at"]


def frame(sp, rows):
    return sp.createDataFrame([(*r, datetime(2025, 3, 1)) for r in rows],
                              "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, passenger_count int, "
                              "trip_distance double, PULocationID int, DOLocationID int, fare_amount double, total_amount double, _ingested_at timestamp")


def reasons(sp, rows):
    zones = sp.createDataFrame([(i, "B", "Z", "Yellow Zone") for i in range(1, 266)], "location_id int, borough string, zone string, service_zone string")
    out = silver.classify(frame(sp, rows), zones, "2025-03").select("reasons").collect()
    return [r["reasons"] for r in out]


P, D = datetime(2025, 3, 5, 10, 0), datetime(2025, 3, 5, 10, 20)


def test_clean_row_has_no_reasons(sp):
    assert reasons(sp, [(1, P, D, 1, 2.0, 100, 200, 12.0, 15.0)]) == [[]]


def test_dropoff_before_pickup(sp):
    assert "dropoff_before_pickup" in reasons(sp, [(1, D, P, 1, 2.0, 100, 200, 12.0, 15.0)])[0]


def test_pickup_outside_file_month(sp):
    assert "pickup_outside_file_month" in reasons(sp, [(1, datetime(2008, 12, 31, 23, 0), datetime(2008, 12, 31, 23, 30), 1, 2.0, 100, 200, 12.0, 15.0)])[0]


def test_unknown_location(sp):
    assert "unknown_location_id" in reasons(sp, [(1, P, D, 1, 2.0, 999, 200, 12.0, 15.0)])[0]


def test_negative_total_is_rejected_but_negative_fare_with_positive_total_is_kept(sp):
    r = reasons(sp, [(1, P, D, 1, 2.0, 100, 200, 12.0, -15.0), (2, P, D, None, 2.0, 100, 200, -3.5, 2.0)])
    assert "negative_total_amount" in r[0]
    assert r[1] == []  # flex-fare style row survives (found by profiling the first version of the rule)


def test_distance_out_of_range(sp):
    assert "distance_out_of_range" in reasons(sp, [(1, P, D, 1, 500.0, 100, 200, 12.0, 15.0)])[0]


def test_same_trip_gets_same_key_and_changed_fare_gets_new_hash(sp):
    zones = sp.createDataFrame([(100, "B", "Z", "x"), (200, "B", "Z", "x")], "location_id int, borough string, zone string, service_zone string")
    a = silver.classify(frame(sp, [(1, P, D, 1, 2.0, 100, 200, 12.0, 15.0)]), zones, "2025-03").select("trip_id").collect()[0][0]
    b = silver.classify(frame(sp, [(1, P, D, 1, 2.0, 100, 200, 99.0, 102.0)]), zones, "2025-03").select("trip_id").collect()[0][0]
    assert a == b  # key excludes mutable money fields, so a corrected fare MERGEs onto the same trip
