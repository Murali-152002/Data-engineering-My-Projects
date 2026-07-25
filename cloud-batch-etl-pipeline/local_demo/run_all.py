"""
run_all.py
-----------
One-shot runner: generate data -> build schema -> load pipeline -> index
benchmark. Convenience wrapper around the individual scripts, which can
also be run on their own.

    python run_all.py --days 40
"""

import argparse

from build_star_schema import build
from generate_data import generate_n_days
from load_pipeline import run as run_load
from run_index_benchmark import run as run_benchmark


def main(days):
    generate_n_days(days)
    build()
    run_load()
    print()
    run_benchmark()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=40, help="More days = larger table = the index benefit shows up more clearly.")
    args = parser.parse_args()
    main(args.days)
