"""
pytest suite covering the ingestion logic's correctness properties:
pagination completeness, idempotent watermark advancement, and
failure-safety (watermark must NOT advance if any page fails).
Runs against the real mock API + real land_to_bronze code, not mocks -
same "prove it, don't just describe it" standard as the other two projects.
"""
import json
import os
import sys
import time
import subprocess
import shutil

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, "local_demo"))
sys.path.insert(0, os.path.join(ROOT, "api_source"))

API_BASE = "http://127.0.0.1:5055/api/orders"


@pytest.fixture(scope="module", autouse=True)
def api_server():
    proc = subprocess.Popen(
        ["python3", "mock_orders_api.py"],
        cwd=os.path.join(ROOT, "api_source"),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(20):
        try:
            requests.get(API_BASE, params={"page_size": 1}, timeout=1)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)
    yield
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(autouse=True)
def clean_state(tmp_path, monkeypatch):
    """Each test gets a fresh landing/control dir so tests don't interfere."""
    landing = tmp_path / "landing"
    control = tmp_path / "control"
    control.mkdir()
    (control / "watermark.json").write_text('{"last_modified_at": ""}')

    import run_adf_ingestion_simulation as sim
    monkeypatch.setattr(sim, "LANDING_DIR", str(landing))
    monkeypatch.setattr(sim, "CONTROL_PATH", str(control / "watermark.json"))
    yield sim


def test_pagination_pulls_all_matching_rows(clean_state):
    sim = clean_state
    total_rows, page_count = sim.run_pipeline()
    assert total_rows > 0
    assert page_count >= 1
    # cross-check against the API's own reported total
    resp = requests.get(API_BASE, params={"page_size": 1})
    assert resp.json()["total_matching"] == total_rows


def test_rerun_with_same_watermark_is_idempotent(clean_state):
    sim = clean_state
    first_total, _ = sim.run_pipeline()
    second_total, _ = sim.run_pipeline()
    assert second_total == 0, "re-running without new upstream data must pull 0 rows"


def test_watermark_does_not_advance_on_failure(clean_state):
    sim = clean_state
    before = sim.read_watermark()
    with pytest.raises(RuntimeError):
        sim.run_pipeline(simulate_failure_on_page=1)
    after = sim.read_watermark()
    assert before == after, "watermark must not advance when a page fails"


def test_watermark_catches_updates_not_just_inserts(clean_state):
    """An order whose status changes (completed -> returned) gets a new
    modified_at without a new order_id - the watermark must catch that on
    the next incremental pull, not just brand-new order_ids."""
    sim = clean_state
    sim.run_pipeline()  # baseline pull
    resp = requests.post(f"{API_BASE}/_new_batch", params={"n": 50})
    added, updated = resp.json()["added"], resp.json()["updated"]
    second_total, _ = sim.run_pipeline()
    assert second_total >= added + (updated if updated else 0)
