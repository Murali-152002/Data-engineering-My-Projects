-- ============================================================================
-- 03_merge_procedure.sql
-- Target: Azure SQL Database
--
-- Called by an ADF Stored Procedure activity immediately after Copy Activity
-- lands new rows into dw.stg_sales_transactions. Does the actual cleaning
-- (null handling, dedup, type casts) and the incremental upsert into
-- dw.fact_sales, mirroring the same data-quality rules Silver applies in
-- the Databricks project (null CustomerID dropped, duplicate TransactionID
-- collapsed, "3.0"-style quantity strings coerced) - same problems, same
-- fixes, different engine.
-- ============================================================================

CREATE OR ALTER PROCEDURE dw.usp_MergeSalesStaging
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Clean + type-cast staging into a working temp table. Rows with a
        -- null/non-numeric CustomerID or ProductID are dropped here (logged
        -- to dw.etl_rejected_rows below) rather than failing the whole batch.
        SELECT
            TRY_CAST(TransactionID AS BIGINT)                          AS TransactionID,
            TRY_CAST(CustomerID AS INT)                                AS CustomerID,
            TRY_CAST(ProductID AS INT)                                 AS ProductID,
            TRY_CAST(OrderDate AS DATE)                                AS OrderDate,
            TRY_CAST(TRY_CAST(Quantity AS FLOAT) AS INT)               AS Quantity,  -- handles "3.0"-style strings
            TRY_CAST(UnitPrice AS DECIMAL(10,2))                       AS UnitPrice,
            LTRIM(RTRIM(Region))                                       AS Region
        INTO #cleaned
        FROM dw.stg_sales_transactions;

        IF OBJECT_ID('dw.etl_rejected_rows', 'U') IS NULL
            CREATE TABLE dw.etl_rejected_rows (
                RejectedAt   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                Reason       NVARCHAR(200) NULL,
                RawRow       NVARCHAR(MAX) NULL
            );

        INSERT INTO dw.etl_rejected_rows (Reason, RawRow)
        SELECT
            CASE
                WHEN TransactionID IS NULL THEN 'unparseable TransactionID'
                WHEN CustomerID IS NULL THEN 'null/unparseable CustomerID'
                WHEN ProductID IS NULL THEN 'null/unparseable ProductID'
                WHEN OrderDate IS NULL THEN 'unparseable OrderDate'
                ELSE 'other'
            END,
            CONCAT_WS(',', s.TransactionID, s.CustomerID, s.ProductID, s.OrderDate, s.Quantity, s.UnitPrice, s.Region)
        FROM #cleaned c
        JOIN dw.stg_sales_transactions s
          ON  (TRY_CAST(s.TransactionID AS BIGINT) = c.TransactionID OR (s.TransactionID IS NULL AND c.TransactionID IS NULL))
        WHERE c.TransactionID IS NULL OR c.CustomerID IS NULL OR c.ProductID IS NULL OR c.OrderDate IS NULL;

        -- Deduplicate on TransactionID within this batch, keeping the last-
        -- staged row for any TransactionID that appears more than once.
        WITH deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY TransactionID ORDER BY (SELECT NULL) DESC) AS rn
            FROM #cleaned
            WHERE TransactionID IS NOT NULL
              AND CustomerID IS NOT NULL
              AND ProductID IS NOT NULL
              AND OrderDate IS NOT NULL
        )
        SELECT
            TransactionID, CustomerID, ProductID, OrderDate, Quantity, UnitPrice,
            CAST(FORMAT(OrderDate, 'yyyyMMdd') AS INT) AS DateKey,
            CAST(ROUND(Quantity * UnitPrice, 2) AS DECIMAL(12,2)) AS TotalAmount,
            Region
        INTO #ready
        FROM deduped
        WHERE rn = 1;

        -- Incremental upsert into fact_sales.
        MERGE dw.fact_sales AS target
        USING #ready AS source
            ON target.TransactionID = source.TransactionID
        WHEN MATCHED THEN
            UPDATE SET
                CustomerID  = source.CustomerID,
                ProductID   = source.ProductID,
                DateKey     = source.DateKey,
                OrderDate   = source.OrderDate,
                Quantity    = source.Quantity,
                UnitPrice   = source.UnitPrice,
                TotalAmount = source.TotalAmount,
                Region      = source.Region,
                LoadedAt    = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (TransactionID, CustomerID, ProductID, DateKey, OrderDate, Quantity, UnitPrice, TotalAmount, Region)
            VALUES (source.TransactionID, source.CustomerID, source.ProductID, source.DateKey,
                    source.OrderDate, source.Quantity, source.UnitPrice, source.TotalAmount, source.Region);

        TRUNCATE TABLE dw.stg_sales_transactions;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO

-- ============================================================================
-- usp_UpdateWatermark
-- Called by the pipeline's final Stored Procedure activity once the merge
-- succeeds, advancing dw.etl_watermark so the next run's LookupOldWatermark
-- only pulls source rows newer than this run.
-- ============================================================================

CREATE OR ALTER PROCEDURE dw.usp_UpdateWatermark
    @TableName NVARCHAR(100),
    @NewWatermarkValue DATETIME2
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dw.etl_watermark
    SET WatermarkValue = @NewWatermarkValue,
        UpdatedAt = SYSUTCDATETIME()
    WHERE TableName = @TableName;
END
GO
