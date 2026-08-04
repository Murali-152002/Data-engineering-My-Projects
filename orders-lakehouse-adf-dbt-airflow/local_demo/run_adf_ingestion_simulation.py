"""
Runs the EXACT logic described in adf/pipeline_IncrementalOrdersIngestion.json
against the local mock API, since Azure Data Factory itself can't execute
outside Azure for a personal project. This proves the pipeline's actual
behavior (pagination, idempotent watermark advance, failure handling) rather
than just describing it in JSON.
"""
import json
import os
import sys
import time
import requests

API_BASE = "http://127.0.0.1:5055/api/orders"
CONTROL_PATH = os.path.join(os.path.dirname(__file__), "..", "control", "watermark.json")
LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "landing")


def read_watermark():
    with open(CONTROL_PATH) as f:
        return json.load(f)["last_modified_at"]


def write_watermark(value):
    with open(CONTROL_PATH, "w") as f:
        json.dump({"last_modified_at": value}, f)


def run_pipeline(simulate_failure_on_page=None):
    watermark = read_watermark()
    cursor = 0
    max_modified_seen = watermark or ""
    run_date = time.strftime("%Y-%m-%d")
    out_dir = os.path.join(LANDING_DIR, "orders", f"run_date={run_date}")
    os.makedirs(out_dir, exist_ok=True)

    page_num = 0
    total_rows = 0
    while True:
        page_num += 1
        if simulate_failure_on_page == page_num:
            print(f"  [SIMULATED FAILURE] page {page_num} - raising, watermark stays at '{watermark}'")
            raise RuntimeError("Simulated CopyOrdersPage failure")

        resp = requests.get(API_BASE, params={
            "modified_since": watermark, "cursor": cursor, "page_size": 200,
        })
        resp.raise_for_status()
        payload = resp.json()
        rows = payload["data"]
        total_rows += len(rows)

        ts = time.strftime("%Y%m%d%H%M%S")
        out_path = os.path.join(out_dir, f"orders_page_{ts}_{cursor}.json")
        with open(out_path, "w") as f:
            json.dump(rows, f)

        for r in rows:
            if r["modified_at"] > max_modified_seen:
                max_modified_seen = r["modified_at"]

        print(f"  page {page_num}: cursor={cursor} rows={len(rows)} -> {os.path.basename(out_path)}")

        if payload["next_cursor"] is None:
            break
        cursor = payload["next_cursor"]

    # Only reached if every page succeeded - this is the idempotency guarantee
    write_watermark(max_modified_seen)
    print(f"  DONE: {total_rows} rows across {page_num} pages. Watermark advanced to {max_modified_seen}")
    return total_rows, page_num


if __name__ == "__main__":
    print("=== Run 1: full incremental pull from empty watermark ===")
    run_pipeline()

    print("\n=== Run 2: immediate re-run, same watermark - should pull ~0 new rows (idempotent) ===")
    run_pipeline()
