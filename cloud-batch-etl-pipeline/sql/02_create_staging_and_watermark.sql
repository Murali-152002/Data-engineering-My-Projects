-- ============================================================================
-- 02_create_staging_and_watermark.sql
-- Target: Azure SQL Database
--
-- Staging table that ADF's Copy Activity lands raw rows into (no
-- constraints, everything nullable - cleaning happens in the stored
-- procedure that merges staging into the fact table), plus a watermark
-- control table that drives the incremental load pattern:
--
--   1. Lookup activity reads WatermarkValue for 'fact_sales' from this table
--   2. Lookup activity reads MAX(OrderDate) from the source
--   3. Copy activity pulls only source rows > old watermark into staging
--   4. Stored Procedure activity merges staging -> fact_sales
--   5. Stored Procedure activity advances the watermark to the new value
--
-- This is the standard ADF incremental-copy pattern (see Microsoft Learn:
-- "Incrementally load data from a source data store to a destination data
-- store") applied to a Blob Storage -> Azure SQL pipeline.
-- ============================================================================

IF OBJECT_ID('dw.stg_sales_transactions', 'U') IS NULL
CREATE TABLE dw.stg_sales_transactions (
    TransactionID   NVARCHAR(50)  NULL,   -- untyped on landing - see usp_MergeSalesStaging for cleaning
    CustomerID      NVARCHAR(50)  NULL,
    ProductID       NVARCHAR(50)  NULL,
    OrderDate       NVARCHAR(50)  NULL,
    Quantity        NVARCHAR(50)  NULL,
    UnitPrice       NVARCHAR(50)  NULL,
    Region          NVARCHAR(50)  NULL,
    SourceFile      NVARCHAR(500) NULL,
    StagedAt        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('dw.etl_watermark', 'U') IS NULL
CREATE TABLE dw.etl_watermark (
    TableName       NVARCHAR(100) NOT NULL PRIMARY KEY,
    WatermarkValue  DATETIME2     NOT NULL,
    UpdatedAt       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT 1 FROM dw.etl_watermark WHERE TableName = 'fact_sales')
    INSERT INTO dw.etl_watermark (TableName, WatermarkValue) VALUES ('fact_sales', '1900-01-01');
GO
