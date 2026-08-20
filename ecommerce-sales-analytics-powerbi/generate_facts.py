import numpy as np
import pandas as pd

rng = np.random.default_rng(7)

dim_date = pd.read_parquet("/tmp/ecommerce-bi/data/dim_date.parquet")
dim_product = pd.read_parquet("/tmp/ecommerce-bi/data/dim_product.parquet")
dim_region = pd.read_parquet("/tmp/ecommerce-bi/data/dim_region.parquet")
dim_customer = pd.read_parquet("/tmp/ecommerce-bi/data/dim_customer.parquet")

n_days = len(dim_date)
# Base daily transaction volume with year-over-year growth + weekend lift + Nov/Dec holiday lift
base = 650
year_growth = {2023: 1.0, 2024: 1.18, 2025: 1.34}   # real, deliberately-designed YoY growth baked into the generator
daily_counts = []
for _, row in dim_date.iterrows():
    lam = base * year_growth[row.year]
    if row.is_weekend:
        lam *= 1.15
    if row.month in (11, 12):
        lam *= 1.55
    elif row.month in (1, 2):
        lam *= 0.85
    daily_counts.append(rng.poisson(lam))
dim_date = dim_date.copy()
dim_date["txn_count"] = daily_counts

total_rows = int(sum(daily_counts))
print("planned fact rows:", total_rows)

# category popularity weights (skewed, not uniform, so there's a real "top category" story)
cat_weights_map = {
    "Electronics": 0.30, "Home & Kitchen": 0.20, "Apparel": 0.22,
    "Beauty": 0.12, "Sporting Goods": 0.09, "Office Supplies": 0.07,
}
product_weights = dim_product["category"].map(cat_weights_map).to_numpy()
product_weights = product_weights / product_weights.sum()

date_keys = np.repeat(dim_date["date_key"].to_numpy(), dim_date["txn_count"].to_numpy())
n = len(date_keys)

product_idx = rng.choice(len(dim_product), size=n, p=product_weights)
product_keys = dim_product["product_key"].to_numpy()[product_idx]
unit_price = dim_product["unit_price"].to_numpy()[product_idx]
unit_cost = dim_product["unit_cost"].to_numpy()[product_idx]

customer_keys = rng.integers(1, len(dim_customer) + 1, size=n)
region_keys = rng.choice(dim_region["region_key"].to_numpy(), size=n, p=[0.24, 0.19, 0.20, 0.16, 0.21])
quantity = rng.integers(1, 5, size=n)

# small realistic per-line discount
discount_pct = rng.choice([0.0, 0.05, 0.10, 0.15], size=n, p=[0.55, 0.20, 0.15, 0.10])
gross_revenue = unit_price * quantity
net_revenue = gross_revenue * (1 - discount_pct)
cost = unit_cost * quantity
profit = net_revenue - cost

fact_sales = pd.DataFrame({
    "transaction_id": np.arange(1, n + 1),
    "date_key": date_keys,
    "product_key": product_keys,
    "customer_key": customer_keys,
    "region_key": region_keys,
    "quantity": quantity,
    "discount_pct": discount_pct,
    "revenue": net_revenue.round(2),
    "cost": cost.round(2),
    "profit": profit.round(2),
})

fact_sales.to_parquet("/tmp/ecommerce-bi/data/fact_sales.parquet")
print("actual fact rows:", len(fact_sales))
print(fact_sales.head())
