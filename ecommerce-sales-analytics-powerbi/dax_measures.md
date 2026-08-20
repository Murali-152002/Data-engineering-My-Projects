# DAX Measures

Paste these into Power BI Desktop after loading the 5 tables and confirming relationships (all four are many-to-one from `fact_sales` into the dimension tables, on the surrogate key columns).

```dax
Total Revenue = SUM(fact_sales[revenue])

Total Profit = SUM(fact_sales[profit])

Profit Margin % = DIVIDE([Total Profit], [Total Revenue])

Average Order Value = DIVIDE([Total Revenue], COUNTROWS(fact_sales))

YoY Revenue Growth % =
VAR CurrentRevenue = [Total Revenue]
VAR PriorYearRevenue =
    CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(dim_date[full_date]))
RETURN
    DIVIDE(CurrentRevenue - PriorYearRevenue, PriorYearRevenue)

Category Profit Contribution % =
DIVIDE(
    [Total Profit],
    CALCULATE([Total Profit], ALLSELECTED(dim_product[category]))
)
```

## Notes

- `YoY Revenue Growth %` requires a marked date table in Power BI (Model view → right-click `dim_date` → "Mark as Date Table", using `full_date`).
- `Category Profit Contribution %` is designed to be sliced by `dim_product[category]` on a visual — it recalculates each category's share of whatever's currently in view (respects other slicers/filters via `ALLSELECTED`).
- All six measures were validated against the real computed values in `business_metrics.py` output before being written here — the DAX formulas produce the same numbers reported in the README.
