-- ============================================================================
-- 01_create_star_schema.sql
-- Target: Azure SQL Database
--
-- Star schema for the sales reporting warehouse: one fact table plus three
-- dimensions. Nonclustered indexes are added on CustomerID and OrderDate on
-- fact_sales specifically because those are the two columns every reporting
-- query in this warehouse filters or joins on (customer-level drill-downs
-- and date-range reports) - see 04_query_performance_demo.sql for the
-- before/after measurement that justifies them.
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');
GO

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

IF OBJECT_ID('dw.dim_customer', 'U') IS NULL
CREATE TABLE dw.dim_customer (
    CustomerID    INT           NOT NULL PRIMARY KEY,
    CustomerName  NVARCHAR(200) NOT NULL,
    Segment       NVARCHAR(50)  NULL,
    SignupDate    DATE          NULL,
    UpdatedAt     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('dw.dim_product', 'U') IS NULL
CREATE TABLE dw.dim_product (
    ProductID    INT           NOT NULL PRIMARY KEY,
    ProductName  NVARCHAR(200) NOT NULL,
    Category     NVARCHAR(100) NULL,
    UnitCost     DECIMAL(10,2) NULL,
    UpdatedAt    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('dw.dim_date', 'U') IS NULL
CREATE TABLE dw.dim_date (
    DateKey     INT      NOT NULL PRIMARY KEY,  -- yyyyMMdd
    FullDate    DATE     NOT NULL,
    [Year]      SMALLINT NOT NULL,
    [Month]     TINYINT  NOT NULL,
    [Day]       TINYINT  NOT NULL,
    DayOfWeek   NVARCHAR(10) NOT NULL,
    IsWeekend   BIT      NOT NULL
);
GO

-- ---------------------------------------------------------------------------
-- Fact table
-- ---------------------------------------------------------------------------

IF OBJECT_ID('dw.fact_sales', 'U') IS NULL
CREATE TABLE dw.fact_sales (
    TransactionID   BIGINT        NOT NULL PRIMARY KEY,
    CustomerID      INT           NOT NULL,
    ProductID       INT           NOT NULL,
    DateKey         INT           NOT NULL,
    OrderDate       DATE          NOT NULL,
    Quantity        INT           NOT NULL,
    UnitPrice       DECIMAL(10,2) NOT NULL,
    TotalAmount     DECIMAL(12,2) NOT NULL,
    Region          NVARCHAR(50)  NULL,
    LoadedAt        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_fact_sales_customer FOREIGN KEY (CustomerID) REFERENCES dw.dim_customer(CustomerID),
    CONSTRAINT FK_fact_sales_product  FOREIGN KEY (ProductID)  REFERENCES dw.dim_product(ProductID),
    CONSTRAINT FK_fact_sales_date     FOREIGN KEY (DateKey)    REFERENCES dw.dim_date(DateKey)
);
GO

-- ---------------------------------------------------------------------------
-- Nonclustered indexes - the ~30% reporting query improvement in the README
-- is measured with and without these (see 04_query_performance_demo.sql).
-- ---------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fact_sales_CustomerID')
    CREATE NONCLUSTERED INDEX IX_fact_sales_CustomerID
        ON dw.fact_sales (CustomerID)
        INCLUDE (TotalAmount, OrderDate);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fact_sales_OrderDate')
    CREATE NONCLUSTERED INDEX IX_fact_sales_OrderDate
        ON dw.fact_sales (OrderDate)
        INCLUDE (TotalAmount, Region);
GO
