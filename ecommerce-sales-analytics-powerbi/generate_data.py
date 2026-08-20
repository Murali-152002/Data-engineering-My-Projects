import numpy as np
import pandas as pd
from datetime import date, timedelta

rng = np.random.default_rng(42)

# ---- Dimension: Date (3 years) ----
start = date(2023, 1, 1)
end = date(2025, 12, 31)
dates = pd.date_range(start, end, freq="D")
dim_date = pd.DataFrame({
    "date_key": range(1, len(dates) + 1),
    "full_date": dates,
    "year": dates.year,
    "month": dates.month,
    "quarter": dates.quarter,
    "day_of_week": dates.dayofweek,
})
dim_date["is_weekend"] = dim_date["day_of_week"].isin([5, 6])
dim_date["month_name"] = dim_date["full_date"].dt.strftime("%b")

# ---- Dimension: Product / Category ----
categories = {
    "Electronics": ["Headphones", "Laptop Sleeve", "Smart Watch", "Bluetooth Speaker", "Webcam"],
    "Home & Kitchen": ["Air Fryer", "Coffee Maker", "Cookware Set", "Blender", "Vacuum"],
    "Apparel": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Cap"],
    "Beauty": ["Moisturizer", "Shampoo", "Perfume", "Lipstick", "Sunscreen"],
    "Sporting Goods": ["Yoga Mat", "Dumbbell Set", "Running Shoes", "Water Bottle", "Bike Helmet"],
    "Office Supplies": ["Notebook", "Desk Organizer", "Pen Set", "Monitor Stand", "Backpack"],
}
rows = []
pid = 1
for cat, items in categories.items():
    for item in items:
        for variant in range(1, 9):  # 8 SKUs per item
            base_cost = rng.uniform(5, 150)
            markup = rng.uniform(1.25, 2.6)
            rows.append({
                "product_key": pid,
                "product_name": f"{item} {variant}",
                "category": cat,
                "unit_cost": round(base_cost, 2),
                "unit_price": round(base_cost * markup, 2),
            })
            pid += 1
dim_product = pd.DataFrame(rows)

# ---- Dimension: Region/Store ----
regions = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
dim_region = pd.DataFrame({
    "region_key": range(1, len(regions) + 1),
    "region_name": regions,
})

# ---- Dimension: Customer ----
n_customers = 40000
dim_customer = pd.DataFrame({
    "customer_key": range(1, n_customers + 1),
    "segment": rng.choice(["Consumer", "Small Business", "Enterprise"], size=n_customers, p=[0.72, 0.22, 0.06]),
    "signup_year": rng.integers(2019, 2026, size=n_customers),
})

print("dims built:", len(dim_date), len(dim_product), len(dim_region), len(dim_customer))

dim_date.to_parquet("/tmp/ecommerce-bi/data/dim_date.parquet")
dim_product.to_parquet("/tmp/ecommerce-bi/data/dim_product.parquet")
dim_region.to_parquet("/tmp/ecommerce-bi/data/dim_region.parquet")
dim_customer.to_parquet("/tmp/ecommerce-bi/data/dim_customer.parquet")
