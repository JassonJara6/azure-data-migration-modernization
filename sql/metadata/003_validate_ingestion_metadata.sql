SET NOCOUNT ON;
USE [AdventureWorks];

IF OBJECT_ID(N'ingestion.table_config', N'U') IS NULL
    THROW 51010, 'ingestion.table_config does not exist.', 1;
IF OBJECT_ID(N'ingestion.watermark_state', N'U') IS NULL
    THROW 51011, 'ingestion.watermark_state does not exist.', 1;

IF (SELECT COUNT(*) FROM ingestion.table_config) <> 11
   OR (SELECT COUNT(*) FROM ingestion.table_config WHERE is_active = 1) <> 11
    THROW 51012, 'Expected exactly 11 active ingestion configurations.', 1;
IF (SELECT COUNT(*) FROM ingestion.table_config WHERE is_active = 1 AND load_type = 'INCREMENTAL') <> 5
    THROW 51013, 'Expected exactly 5 active INCREMENTAL configurations.', 1;
IF (SELECT COUNT(*) FROM ingestion.table_config WHERE is_active = 1 AND load_type = 'FULL') <> 6
    THROW 51014, 'Expected exactly 6 active FULL configurations.', 1;

IF EXISTS
(
    SELECT 1
    FROM
    (
        VALUES
            (N'Person', N'Person', 'INCREMENTAL'),
            (N'Person', N'Address', 'INCREMENTAL'),
            (N'Person', N'StateProvince', 'FULL'),
            (N'Person', N'CountryRegion', 'FULL'),
            (N'Production', N'Product', 'INCREMENTAL'),
            (N'Production', N'ProductCategory', 'FULL'),
            (N'Production', N'ProductSubcategory', 'FULL'),
            (N'Sales', N'Customer', 'FULL'),
            (N'Sales', N'SalesOrderHeader', 'INCREMENTAL'),
            (N'Sales', N'SalesOrderDetail', 'INCREMENTAL'),
            (N'Sales', N'SalesTerritory', 'FULL')
    ) AS expected(source_schema, source_table, load_type)
    LEFT JOIN ingestion.table_config AS config
      ON config.source_schema = expected.source_schema
     AND config.source_table = expected.source_table
     AND config.is_active = 1
    WHERE config.config_id IS NULL
       OR config.load_type <> expected.load_type
)
    THROW 51019, 'An expected source is missing, inactive, or has the wrong load type.', 1;

IF EXISTS
(
    SELECT 1
    FROM ingestion.table_config AS config
    LEFT JOIN sys.schemas AS source_schema
      ON source_schema.name = config.source_schema
    LEFT JOIN sys.tables AS source_table
      ON source_table.schema_id = source_schema.schema_id
     AND source_table.name = config.source_table
    WHERE config.is_active = 1
      AND source_table.object_id IS NULL
)
    THROW 51015, 'One or more configured source tables do not exist.', 1;

-- PKs must be nonempty arrays of strings; JSON objects and duplicate keys
-- cannot establish a deterministic ordered cursor.
IF EXISTS
(
    SELECT 1 FROM ingestion.table_config AS config
    WHERE LEFT(LTRIM(config.primary_key_columns), 1) <> N'['
       OR NOT EXISTS (SELECT 1 FROM OPENJSON(config.primary_key_columns))
       OR EXISTS (SELECT 1 FROM OPENJSON(config.primary_key_columns) WHERE [type] <> 1)
       OR EXISTS
          (SELECT [value] FROM OPENJSON(config.primary_key_columns)
           GROUP BY [value] HAVING COUNT(*) > 1)
)
    THROW 51021, 'Primary keys must be nonempty JSON arrays of distinct column names.', 1;

IF EXISTS
(
    SELECT 1
    FROM ingestion.table_config AS config
    CROSS APPLY OPENJSON(config.primary_key_columns) AS configured_key
    JOIN sys.schemas AS source_schema ON source_schema.name = config.source_schema
    JOIN sys.tables AS source_table
      ON source_table.schema_id = source_schema.schema_id
     AND source_table.name = config.source_table
    LEFT JOIN sys.columns AS source_column
      ON source_column.object_id = source_table.object_id
     AND source_column.name = configured_key.[value]
    LEFT JOIN sys.indexes AS primary_index
      ON primary_index.object_id = source_table.object_id
     AND primary_index.is_primary_key = 1
    LEFT JOIN sys.index_columns AS primary_column
      ON primary_column.object_id = primary_index.object_id
     AND primary_column.index_id = primary_index.index_id
     AND primary_column.column_id = source_column.column_id
    WHERE config.is_active = 1
      AND
      (
          source_column.column_id IS NULL
          OR primary_column.column_id IS NULL
          OR TRY_CONVERT(int, configured_key.[key]) IS NULL
          OR primary_column.key_ordinal <> TRY_CONVERT(int, configured_key.[key]) + 1
      )
)
    THROW 51016, 'A configured primary-key column is missing, unordered, or not part of the source primary key.', 1;

IF EXISTS
(
    SELECT 1
    FROM ingestion.table_config AS config
    JOIN sys.schemas AS source_schema ON source_schema.name = config.source_schema
    JOIN sys.tables AS source_table
      ON source_table.schema_id = source_schema.schema_id
     AND source_table.name = config.source_table
    WHERE config.is_active = 1
      AND
      (
          SELECT COUNT(*)
          FROM OPENJSON(config.primary_key_columns)
      ) <>
      (
          SELECT COUNT(*)
          FROM sys.indexes AS primary_index
          JOIN sys.index_columns AS primary_column
            ON primary_column.object_id = primary_index.object_id
           AND primary_column.index_id = primary_index.index_id
          WHERE primary_index.object_id = source_table.object_id
            AND primary_index.is_primary_key = 1
            AND primary_column.key_ordinal > 0
      )
)
    THROW 51020, 'Configured primary-key columns do not match the complete source primary key.', 1;

IF EXISTS
(
    SELECT 1
    FROM ingestion.table_config AS config
    JOIN sys.schemas AS source_schema ON source_schema.name = config.source_schema
    JOIN sys.tables AS source_table
      ON source_table.schema_id = source_schema.schema_id
     AND source_table.name = config.source_table
    LEFT JOIN sys.columns AS watermark_column
      ON watermark_column.object_id = source_table.object_id
     AND watermark_column.name = config.watermark_column
    WHERE config.is_active = 1
      AND config.load_type = 'INCREMENTAL'
      AND (config.watermark_column <> N'ModifiedDate'
        OR watermark_column.column_id IS NULL
        OR watermark_column.system_type_id NOT IN (42, 58, 61)
        OR watermark_column.is_nullable = 1)
)
    THROW 51017, 'INCREMENTAL sources require a non-null temporal ModifiedDate column.', 1;

IF EXISTS
(
    SELECT 1
    FROM ingestion.table_config AS config
    LEFT JOIN ingestion.watermark_state AS state ON state.config_id = config.config_id
    WHERE config.is_active = 1
      AND ((config.load_type = 'INCREMENTAL' AND state.config_id IS NULL)
        OR (config.load_type = 'FULL' AND state.config_id IS NOT NULL))
)
    THROW 51018, 'Watermark state must exist for every INCREMENTAL table and no FULL table.', 1;

-- Incremental PKs in this contract are positive integers. Cursor objects must
-- contain exactly those named keys, once each, with nonnegative integer values.
IF EXISTS
(
    SELECT 1
    FROM ingestion.watermark_state AS state
    JOIN ingestion.table_config AS config ON config.config_id = state.config_id
    WHERE LEFT(LTRIM(state.watermark_key_json), 1) <> N'{'
       OR (SELECT COUNT(*) FROM OPENJSON(state.watermark_key_json)) <>
          (SELECT COUNT(*) FROM OPENJSON(config.primary_key_columns))
       OR EXISTS
       (
           SELECT 1 FROM OPENJSON(state.watermark_key_json) AS cursor_key
           WHERE cursor_key.[type] <> 2
              OR TRY_CONVERT(bigint, cursor_key.[value]) IS NULL
              OR TRY_CONVERT(bigint, cursor_key.[value]) < 0
              OR cursor_key.[value] LIKE N'%[^0-9]%'
              OR NOT EXISTS
                 (SELECT 1 FROM OPENJSON(config.primary_key_columns) AS pk
                  WHERE pk.[value] = cursor_key.[key])
       )
       OR EXISTS
          (SELECT [key] FROM OPENJSON(state.watermark_key_json)
           GROUP BY [key] HAVING COUNT(*) > 1)
       OR state.watermark_value < CONVERT(datetime2(7), '19000101', 112)
       OR
       (
           -- A fresh cursor has no success fields and only zero key values.
           state.last_successful_run_id IS NULL
           AND
           (state.last_successful_at_utc IS NOT NULL
            OR state.last_successful_row_count IS NOT NULL
            OR state.watermark_value <> CONVERT(datetime2(7), '19000101', 112)
            OR EXISTS (SELECT 1 FROM OPENJSON(state.watermark_key_json)
                       WHERE TRY_CONVERT(bigint, [value]) <> 0))
       )
       OR
       (
           state.last_successful_run_id IS NOT NULL
           AND
           (LEN(LTRIM(RTRIM(state.last_successful_run_id))) = 0
            OR state.last_successful_at_utc IS NULL
            OR state.last_successful_row_count IS NULL
            OR state.last_successful_row_count < 0)
       )
)
    THROW 51022, 'Invalid cursor key, initial boundary, or successful-run state.', 1;

PRINT 'Ingestion metadata validation passed.';

SELECT config.source_schema, config.source_table, config.load_type,
       config.primary_key_columns, config.watermark_column,
       state.watermark_value, state.watermark_key_json,
       config.bronze_relative_path
FROM ingestion.table_config AS config
LEFT JOIN ingestion.watermark_state AS state ON state.config_id = config.config_id
WHERE config.is_active = 1
ORDER BY config.source_schema, config.source_table;
