"""
In-memory synthetic 'Orders' event store standing in for an internal enterprise
REST API (e.g. an order-management or e-commerce backend). Real production ADF
pipelines commonly ingest from internal REST APIs like this one, not just flat
files - this mirrors that pattern instead of repeating the CSV-blob source
already used in the existing cloud-batch-etl-pipeline project.
"""
import random
import uuid
from datetime import datetime, timedelta, timezone

random.seed(42)

CUSTOMERS = [f"CUST{i:05d}" for i in range(1, 4001)]
PRODUCTS = [
    (f"PROD{i:04d}", cat, round(random.uniform(5, 500), 2))
    for i, cat in enumerate(
        random.choices(
            ["Electronics", "Home & Kitchen", "Sports", "Beauty", "Toys", "Office"],
            k=600,
        ),
        start=1,
    )
]

_all_orders = []


def _make_order(order_ts):
    cust = random.choice(CUSTOMERS)
    prod_id, category, unit_price = random.choice(PRODUCTS)
    qty = random.randint(1, 5)
    order = {
        "order_id": str(uuid.uuid4()),
        "customer_id": cust if random.random() > 0.02 else None,  # ~2% missing, injected data-quality issue
        "product_id": prod_id,
        "category": category,
        "quantity": str(qty) if random.random() > 0.05 else f"{qty}.0",  # occasional float-string quirk
        "unit_price": unit_price,
        "order_status": random.choices(
            ["completed", "returned", "cancelled"], weights=[0.88, 0.08, 0.04]
        )[0],
        "created_at": order_ts.isoformat(),
        "modified_at": order_ts.isoformat(),
    }
    return order


def seed_history(days=30, orders_per_day=(400, 700)):
    """Seed `days` worth of historical orders ending 'now'."""
    now = datetime.now(timezone.utc)
    for d in range(days, 0, -1):
        day_ts = now - timedelta(days=d)
        n = random.randint(*orders_per_day)
        for _ in range(n):
            jitter = timedelta(seconds=random.randint(0, 86399))
            _all_orders.append(_make_order(day_ts + jitter))
    _all_orders.sort(key=lambda o: o["modified_at"])
    return len(_all_orders)


def add_new_batch(n=150):
    """Simulate new orders landing 'now' - used to demonstrate incremental pulls."""
    now = datetime.now(timezone.utc)
    for _ in range(n):
        _all_orders.append(_make_order(now))
    # Also simulate a few *updates* to already-existing orders (status change:
    # completed -> returned), which is exactly what a `modified_at` watermark
    # is meant to catch that a pure created_at/append-only feed would miss.
    updated = 0
    if len(_all_orders) > 500:
        sample = random.sample(_all_orders[:-n], k=min(20, len(_all_orders) - n))
        for o in sample:
            if o["order_status"] == "completed" and random.random() < 0.5:
                o["order_status"] = "returned"
                o["modified_at"] = now.isoformat()
                updated += 1
    _all_orders.sort(key=lambda o: o["modified_at"])
    return n, updated


def query(modified_since=None, cursor=0, page_size=200):
    """Cursor + watermark based pagination, mirroring a realistic internal API contract."""
    rows = _all_orders
    if modified_since:
        rows = [o for o in rows if o["modified_at"] > modified_since]
    page = rows[cursor: cursor + page_size]
    next_cursor = cursor + page_size if cursor + page_size < len(rows) else None
    return page, next_cursor, len(rows)
