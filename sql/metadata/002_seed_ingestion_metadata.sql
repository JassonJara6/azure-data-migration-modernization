SET NOCOUNT ON;
SET XACT_ABORT ON;
USE [AdventureWorks];

IF OBJECT_ID(N'ingestion.table_config', N'U') IS NULL
   OR OBJECT_ID(N'ingestion.watermark_state', N'U') IS NULL
    THROW 51000, 'Run 001_create_ingestion_metadata.sql before seeding metadata.', 1;

DECLARE @config table
(
    source_schema        sysname NOT NULL,
    source_table         sysname NOT NULL,
    load_type            varchar(11) NOT NULL,
    primary_key_columns  nvarchar(400) NOT NULL,
    watermark_column     sysname NULL,
    bronze_relative_path nvarchar(400) NOT NULL,
    initial_key_json     nvarchar(1000) NULL
);

INSERT @config
    (source_schema, source_table, load_type, primary_key_columns,
     watermark_column, bronze_relative_path, initial_key_json)
VALUES
    (N'Person', N'Person', N'INCREMENTAL', N'["BusinessEntityID"]', N'ModifiedDate', N'adventureworks/person/person', N'{"BusinessEntityID":0}'),
    (N'Person', N'Address', N'INCREMENTAL', N'["AddressID"]', N'ModifiedDate', N'adventureworks/person/address', N'{"AddressID":0}'),
    (N'Person', N'StateProvince', N'FULL', N'["StateProvinceID"]', NULL, N'adventureworks/person/stateprovince', NULL),
    (N'Person', N'CountryRegion', N'FULL', N'["CountryRegionCode"]', NULL, N'adventureworks/person/countryregion', NULL),
    (N'Production', N'Product', N'INCREMENTAL', N'["ProductID"]', N'ModifiedDate', N'adventureworks/production/product', N'{"ProductID":0}'),
    (N'Production', N'ProductCategory', N'FULL', N'["ProductCategoryID"]', NULL, N'adventureworks/production/productcategory', NULL),
    (N'Production', N'ProductSubcategory', N'FULL', N'["ProductSubcategoryID"]', NULL, N'adventureworks/production/productsubcategory', NULL),
    (N'Sales', N'Customer', N'FULL', N'["CustomerID"]', NULL, N'adventureworks/sales/customer', NULL),
    (N'Sales', N'SalesOrderHeader', N'INCREMENTAL', N'["SalesOrderID"]', N'ModifiedDate', N'adventureworks/sales/salesorderheader', N'{"SalesOrderID":0}'),
    (N'Sales', N'SalesOrderDetail', N'INCREMENTAL', N'["SalesOrderID","SalesOrderDetailID"]', N'ModifiedDate', N'adventureworks/sales/salesorderdetail', N'{"SalesOrderID":0,"SalesOrderDetailID":0}'),
    (N'Sales', N'SalesTerritory', N'FULL', N'["TerritoryID"]', NULL, N'adventureworks/sales/salesterritory', NULL);

BEGIN TRY
BEGIN TRANSACTION;

-- Hold registry and state locks until the contract update commits. Future
-- cursor confirmation must use the same transaction/concurrency protocol.
DECLARE @locked_rows bigint;
SELECT @locked_rows = COUNT_BIG(*) FROM ingestion.table_config WITH (TABLOCKX, HOLDLOCK);
SELECT @locked_rows = COUNT_BIG(*) FROM ingestion.watermark_state WITH (TABLOCKX, HOLDLOCK);

-- Once a cursor has been confirmed, changing its load contract requires an
-- explicit migration that reconciles existing state. Never do it implicitly.
IF EXISTS
(
    SELECT 1
    FROM @config AS source
    JOIN ingestion.table_config AS target
      ON target.source_schema = source.source_schema
     AND target.source_table = source.source_table
    JOIN ingestion.watermark_state AS state ON state.config_id = target.config_id
    WHERE
      (
          state.last_successful_run_id IS NOT NULL
          OR state.last_successful_at_utc IS NOT NULL
          OR state.last_successful_row_count IS NOT NULL
          OR state.watermark_value <> CONVERT(datetime2(7), '19000101', 112)
          OR state.watermark_key_json <> source.initial_key_json
          OR source.initial_key_json IS NULL
      )
      AND
      (
          target.load_type <> source.load_type
          OR target.primary_key_columns <> source.primary_key_columns
          OR ISNULL(target.watermark_column, N'') <> ISNULL(source.watermark_column, N'')
      )
)
    THROW 51001, 'A confirmed cursor has an incompatible contract change; use a controlled migration.', 1;

UPDATE target
SET target.load_type            = source.load_type,
    target.primary_key_columns  = source.primary_key_columns,
    target.watermark_column     = source.watermark_column,
    target.bronze_relative_path = source.bronze_relative_path,
    target.is_active            = 1,
    target.updated_at_utc       = SYSUTCDATETIME()
FROM ingestion.table_config AS target
JOIN @config AS source
  ON source.source_schema = target.source_schema
 AND source.source_table = target.source_table;

INSERT ingestion.table_config
    (source_schema, source_table, load_type, primary_key_columns,
     watermark_column, bronze_relative_path)
SELECT source.source_schema, source.source_table, source.load_type,
       source.primary_key_columns, source.watermark_column, source.bronze_relative_path
FROM @config AS source
WHERE NOT EXISTS
(
    SELECT 1
    FROM ingestion.table_config AS target
    WHERE target.source_schema = source.source_schema
      AND target.source_table = source.source_table
);

-- Seed only missing cursors. Re-running this script never resets a successful watermark.
INSERT ingestion.watermark_state (config_id, watermark_value, watermark_key_json)
SELECT target.config_id, CONVERT(datetime2(7), '19000101', 112), source.initial_key_json
FROM @config AS source
JOIN ingestion.table_config AS target
  ON target.source_schema = source.source_schema
 AND target.source_table = source.source_table
WHERE source.load_type = 'INCREMENTAL'
  AND NOT EXISTS
  (
      SELECT 1
      FROM ingestion.watermark_state AS state
      WHERE state.config_id = target.config_id
  );

COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
