"""
generate_data.py
------------------
Generates synthetic source data at realistic daily volume
(~5K-8K daily sales transaction records), plus reference (customer/product)
data, and deliberately injects the same messy-data problems the merge
procedure has to handle: null CustomerID, "3.0"-style float-string
quantities, and duplicate TransactionIDs.

    python generate_data.py --days 5
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta

from config import LANDING_DIR

random.seed(11)

REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
N_CUSTOMERS = 800
N_PRODUCTS = 200


def generate_reference_data():
    os.makedirs(LANDING_DIR, exist_ok=True)
    customers_path = os.path.join(LANDING_DIR, "customers.csv")
    products_path = os.path.join(LANDING_DIR, "products.csv")

    segments = ["Consumer", "Corporate", "Small Business"]
    with open(customers_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["CustomerID", "CustomerName", "Segment", "SignupDate"])
        for cid in range(1, N_CUSTOMERS + 1):
            signup = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 900))
            w.writerow([cid, f"Customer_{cid:04d}", random.choice(segments), signup.strftime("%Y-%m-%d")])

    categories = ["Electronics", "Home & Kitchen", "Apparel", "Sporting Goods", "Toys"]
    with open(products_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ProductID", "ProductName", "Category", "UnitCost"])
        for pid in range(1, N_PRODUCTS + 1):
            w.writerow([pid, f"Product_{pid:04d}", random.choice(categories), round(random.uniform(5, 250), 2)])

    print(f"Reference data: {N_CUSTOMERS} customers -> {customers_path}, {N_PRODUCTS} products -> {products_path}")


def generate_daily_csv(date, min_rows=5000, max_rows=8000):
    os.makedirs(LANDING_DIR, exist_ok=True)
    filename = os.path.join(LANDING_DIR, f"sales_{date.strftime('%Y%m%d')}.csv")
    n_rows = random.randint(min_rows, max_rows)

    txn_start = int(date.strftime("%Y%m%d")) * 100000
    rows = []
    for i in range(n_rows):
        txn_id = txn_start + i
        customer_id = random.randint(1, N_CUSTOMERS)
        product_id = random.randint(1, N_PRODUCTS)
        order_date = date.strftime("%Y-%m-%d")
        quantity = random.randint(1, 8)
        unit_price = round(random.uniform(5, 300), 2)
        region = random.choice(REGIONS)

        if random.random() < 0.025:
            customer_id = ""
        if random.random() < 0.02:
            quantity = f"{quantity}.0"
        if random.random() < 0.01 and rows:
            txn_id = txn_start + max(i - 1, 0)

        rows.append([txn_id, customer_id, product_id, order_date, quantity, unit_price, region])

    with open(filename, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["TransactionID", "CustomerID", "ProductID", "OrderDate", "Quantity", "UnitPrice", "Region"])
        w.writerows(rows)

    print(f"Generated {n_rows} rows -> {filename}")
    return filename


def generate_n_days(n_days=5, start_date=None):
    start_date = start_date or (datetime.today() - timedelta(days=n_days))
    generate_reference_data()
    for d in range(n_days):
        generate_daily_csv(start_date + timedelta(days=d))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=5)
    args = parser.parse_args()
    generate_n_days(args.days)
