-- ============================================================================
-- 04_query_performance_demo.sql
-- Target: Azure SQL Database
--
-- The two representative reporting queries these indexes are built for:
-- a customer-level drill-down and a date-range report. Run each pair
-- (index dropped / index present) with SET STATISTICS TIME, IO ON to see
-- logical reads and elapsed time drop once the nonclustered indexes exist.
--
-- A fully runnable, engine-agnostic version of this same before/after
-- comparison (same query shapes, same index columns) lives in
-- local_demo/run_index_benchmark.py, which measures actual elapsed time
-- against a local database - see that script and the README for measured
-- numbers instead of just an assertion.
-- ============================================================================

-- Query 1: customer-level drill-down (what IX_fact_sales_CustomerID targets)
SET STATISTICS TIME, IO ON;

SELECT
    c.CustomerName,
    COUNT(*)          AS TransactionCount,
    SUM(f.TotalAmount) AS TotalSpend
FROM dw.fact_sales f
JOIN dw.dim_customer c ON c.CustomerID = f.CustomerID
WHERE f.CustomerID BETWEEN 100 AND 200
GROUP BY c.CustomerName;

-- Query 2: date-range report (what IX_fact_sales_OrderDate targets)
SELECT
    f.OrderDate,
    f.Region,
    SUM(f.TotalAmount) AS DailyRevenue,
    COUNT(*)            AS TransactionCount
FROM dw.fact_sales f
WHERE f.OrderDate BETWEEN DATEADD(DAY, -7, GETDATE()) AND GETDATE()
GROUP BY f.OrderDate, f.Region
ORDER BY f.OrderDate;

SET STATISTICS TIME, IO OFF;

-- To reproduce the "before" measurement, drop the indexes first:
--   DROP INDEX IX_fact_sales_CustomerID ON dw.fact_sales;
--   DROP INDEX IX_fact_sales_OrderDate ON dw.fact_sales;
-- then re-run 01_create_star_schema.sql's CREATE INDEX statements to restore them.
