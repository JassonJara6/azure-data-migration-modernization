-- Read-only Lookup batch, not a deployment script or stored procedure.
-- The generator injects only a typed ADF integer configuration ID.
SET NOCOUNT ON;
IF DB_NAME()<>N'AdventureWorks'
    THROW 51110, 'ls_sql_server must select AdventureWorks as its database.', 1;
DECLARE @config_id int = __CONFIG_ID__;
DECLARE @schema sysname, @table sysname, @mode varchar(11), @pk nvarchar(400),
        @watermark sysname, @path nvarchar(400), @lower datetime2(7),
        @keys nvarchar(1000), @version varchar(18), @run nvarchar(100),
        @success_at datetime2(7), @success_count bigint;
SELECT @schema=c.source_schema, @table=c.source_table, @mode=c.load_type,
       @pk=c.primary_key_columns, @watermark=c.watermark_column,
       @path=c.bronze_relative_path, @lower=s.watermark_value,
       @keys=s.watermark_key_json, @version=CONVERT(varchar(18),CONVERT(binary(8),s.row_version),1),
       @run=s.last_successful_run_id, @success_at=s.last_successful_at_utc,
       @success_count=s.last_successful_row_count
FROM ingestion.table_config AS c
LEFT JOIN ingestion.watermark_state AS s ON s.config_id=c.config_id
WHERE c.config_id=@config_id AND c.is_active=1;
IF @schema IS NULL THROW 51100, 'Source configuration is missing or inactive.', 1;

-- Fail closed outside the approved eleven-table contract. Identifiers never
-- come from pipeline strings and are additionally quoted with QUOTENAME.
DECLARE @approved table (s sysname, t sysname, m varchar(11), p nvarchar(400));
INSERT @approved VALUES
(N'Person',N'Person','INCREMENTAL',N'["BusinessEntityID"]'),
(N'Person',N'Address','INCREMENTAL',N'["AddressID"]'),
(N'Person',N'StateProvince','FULL',N'["StateProvinceID"]'),
(N'Person',N'CountryRegion','FULL',N'["CountryRegionCode"]'),
(N'Production',N'Product','INCREMENTAL',N'["ProductID"]'),
(N'Production',N'ProductCategory','FULL',N'["ProductCategoryID"]'),
(N'Production',N'ProductSubcategory','FULL',N'["ProductSubcategoryID"]'),
(N'Sales',N'Customer','FULL',N'["CustomerID"]'),
(N'Sales',N'SalesOrderHeader','INCREMENTAL',N'["SalesOrderID"]'),
(N'Sales',N'SalesOrderDetail','INCREMENTAL',N'["SalesOrderID","SalesOrderDetailID"]'),
(N'Sales',N'SalesTerritory','FULL',N'["TerritoryID"]');
IF NOT EXISTS (SELECT 1 FROM @approved WHERE s=@schema AND t=@table AND m=@mode
               AND p COLLATE Latin1_General_100_BIN2=@pk COLLATE Latin1_General_100_BIN2)
   OR @path COLLATE Latin1_General_100_BIN2 <>
      (N'adventureworks/'+LOWER(@schema)+N'/'+LOWER(@table)) COLLATE Latin1_General_100_BIN2
   OR (@mode='INCREMENTAL' AND ISNULL(@watermark,N'')<>N'ModifiedDate')
   OR (@mode='FULL' AND (@watermark IS NOT NULL OR @version IS NOT NULL))
    THROW 51101, 'Configuration differs from the approved source contract.', 1;
DECLARE @object int=OBJECT_ID(QUOTENAME(@schema)+N'.'+QUOTENAME(@table),N'U');
IF @object IS NULL THROW 51102, 'Source table is not visible to the reader.', 1;
IF (SELECT COUNT(*) FROM OPENJSON(@pk)) <>
   (SELECT COUNT(*) FROM sys.indexes AS i JOIN sys.index_columns AS k
    ON k.object_id=i.object_id AND k.index_id=i.index_id
    WHERE i.object_id=@object AND i.is_primary_key=1 AND k.key_ordinal>0)
 OR EXISTS
 (SELECT 1 FROM OPENJSON(@pk) AS j WHERE NOT EXISTS
  (SELECT 1 FROM sys.indexes AS i JOIN sys.index_columns AS k
   ON k.object_id=i.object_id AND k.index_id=i.index_id
   JOIN sys.columns AS c ON c.object_id=k.object_id AND c.column_id=k.column_id
   WHERE i.object_id=@object AND i.is_primary_key=1
     AND k.key_ordinal=CONVERT(int,j.[key])+1 AND c.name=j.[value]))
    THROW 51103, 'Real source primary key differs from metadata.', 1;

DECLARE @k1 sysname=JSON_VALUE(@pk,'$[0]'), @k2 sysname=JSON_VALUE(@pk,'$[1]');
DECLARE @l1 int, @l2 int, @u1 int, @u2 int, @upper datetime2(7);
DECLARE @qualified nvarchar(520)=QUOTENAME(@schema)+N'.'+QUOTENAME(@table);
DECLARE @order nvarchar(600)=QUOTENAME(@k1)+
       CASE WHEN @k2 IS NULL THEN N'' ELSE N','+QUOTENAME(@k2) END;
DECLARE @where nvarchar(max)=N'1=1', @sql nvarchar(max), @expected bigint;
IF @mode='INCREMENTAL'
BEGIN
    IF @version IS NULL OR @lower IS NULL OR ISJSON(@keys)<>1
        THROW 51104, 'Incremental source has no valid committed cursor.', 1;
    IF LEFT(LTRIM(@keys),1)<>N'{' OR
       (SELECT COUNT(*) FROM OPENJSON(@keys))<>(SELECT COUNT(*) FROM OPENJSON(@pk))
       OR EXISTS (SELECT [key] FROM OPENJSON(@keys) GROUP BY [key] HAVING COUNT(*)>1)
       OR EXISTS
       (SELECT 1 FROM OPENJSON(@keys) AS j WHERE j.[type]<>2
        OR TRY_CONVERT(int,j.[value]) IS NULL OR TRY_CONVERT(int,j.[value])<0
        OR j.[value] LIKE N'%[^0-9]%'
        OR NOT EXISTS (SELECT 1 FROM OPENJSON(@pk) AS p
                       WHERE p.[value] COLLATE Latin1_General_100_BIN2=j.[key] COLLATE Latin1_General_100_BIN2))
        THROW 51105, 'Cursor keys must match the complete integer primary key.', 1;
    SELECT @l1=TRY_CONVERT(int,JSON_VALUE(@keys,N'$."'+@k1+N'"')),
           @l2=CASE WHEN @k2 IS NOT NULL THEN TRY_CONVERT(int,JSON_VALUE(@keys,N'$."'+@k2+N'"')) END;
    IF @l1 IS NULL OR (@k2 IS NOT NULL AND @l2 IS NULL)
       OR @lower<CONVERT(datetime2(7),'19000101',112)
       OR (@run IS NULL AND (@success_at IS NOT NULL OR @success_count IS NOT NULL
           OR @lower<>CONVERT(datetime2(7),'19000101',112) OR @l1<>0 OR ISNULL(@l2,0)<>0))
       OR (@run IS NOT NULL AND (LEN(LTRIM(RTRIM(@run)))=0 OR @success_at IS NULL
           OR @success_count IS NULL OR @success_count<0))
        THROW 51106, 'Cursor success fields or initial boundary are inconsistent.', 1;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=@object
                   AND name=N'ModifiedDate' AND system_type_id IN (42,58,61) AND is_nullable=0)
        THROW 51107, 'ModifiedDate must be a non-null temporal column.', 1;
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id=@object
               AND name IN (@k1,@k2) AND system_type_id<>56)
        THROW 51108, 'This version supports int incremental primary keys only.', 1;

    DECLARE @wm nvarchar(100)=N'CONVERT(datetime2(7),[ModifiedDate])';
    DECLARE @ld nvarchar(100)=N'CONVERT(datetime2(7),N'''+CONVERT(nvarchar(33),@lower,126)+N''',126)';
    DECLARE @lk nvarchar(600)=QUOTENAME(@k1)+N'>'+CONVERT(nvarchar(20),@l1);
    IF @k2 IS NOT NULL SET @lk=N'('+@lk+N' OR ('+QUOTENAME(@k1)+N'='+CONVERT(nvarchar(20),@l1)
                                 +N' AND '+QUOTENAME(@k2)+N'>'+CONVERT(nvarchar(20),@l2)+N'))';
    SET @where=N'('+@wm+N'>'+@ld+N' OR ('+@wm+N'='+@ld+N' AND '+@lk+N'))';
    -- Capture the maximal complete tuple, not independent MAX values.
    SELECT @upper=@lower, @u1=@l1, @u2=@l2;
    SET @sql=N'SELECT TOP (1) @u=CONVERT(datetime2(7),[ModifiedDate]), @a='+QUOTENAME(@k1)
        +CASE WHEN @k2 IS NULL THEN N'' ELSE N', @b='+QUOTENAME(@k2) END
        +N' FROM '+@qualified+N' WHERE '+@where+N' ORDER BY [ModifiedDate] DESC,'
        +QUOTENAME(@k1)+N' DESC'+CASE WHEN @k2 IS NULL THEN N'' ELSE N','+QUOTENAME(@k2)+N' DESC' END+N';';
    EXEC sys.sp_executesql @sql,N'@u datetime2(7) OUTPUT,@a int OUTPUT,@b int OUTPUT',
         @u=@upper OUTPUT,@a=@u1 OUTPUT,@b=@u2 OUTPUT;
    DECLARE @ud nvarchar(100)=N'CONVERT(datetime2(7),N'''+CONVERT(nvarchar(33),@upper,126)+N''',126)';
    -- Inclusive upper PK tuple: first component MUST be strict (<), with
    -- equality permitted only when the second component is also <= its limit.
    DECLARE @uk nvarchar(600);
    IF @k2 IS NULL
        SET @uk=QUOTENAME(@k1)+N'<='+CONVERT(nvarchar(20),@u1);
    ELSE
        SET @uk=N'('+QUOTENAME(@k1)+N'<'+CONVERT(nvarchar(20),@u1)
            +N' OR ('+QUOTENAME(@k1)+N'='+CONVERT(nvarchar(20),@u1)+N' AND '
            +QUOTENAME(@k2)+N'<='+CONVERT(nvarchar(20),@u2)+N'))';
    SET @where=@where+N' AND ('+@wm+N'<'+@ud+N' OR ('+@wm+N'='+@ud+N' AND '+@uk+N'))';
    SET @order=N'[ModifiedDate],'+@order;
END;

-- Explicit projection handles SQL Server types unsupported by Parquet.
-- XML is Unicode XML, hierarchyid is its canonical string, and spatial
-- values retain native serialized bytes (including SRID). Unknown CLR types fail.
IF EXISTS (SELECT 1 FROM sys.columns AS c JOIN sys.types AS t ON t.user_type_id=c.user_type_id
           WHERE c.object_id=@object AND c.system_type_id=240
             AND t.name NOT IN (N'geography',N'geometry',N'hierarchyid'))
    THROW 51109, 'Unsupported CLR source type requires an explicit projection.', 1;
DECLARE @columns nvarchar(max);
SELECT @columns=STUFF((SELECT N','+
    CASE WHEN c.system_type_id=241 THEN N'CONVERT(nvarchar(max),'+QUOTENAME(c.name)+N')'
         WHEN c.system_type_id=240 AND t.name=N'hierarchyid' THEN N'CONVERT(nvarchar(4000),'+QUOTENAME(c.name)+N')'
         WHEN c.system_type_id=240 THEN QUOTENAME(c.name)+N'.Serialize()'
         ELSE QUOTENAME(c.name) END+N' AS '+QUOTENAME(c.name)
    FROM sys.columns AS c JOIN sys.types AS t ON t.user_type_id=c.user_type_id
    WHERE c.object_id=@object ORDER BY c.column_id
    FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,1,N'');
SET @sql=N'SELECT @n=COUNT_BIG(*) FROM '+@qualified+N' WHERE '+@where+N';';
EXEC sys.sp_executesql @sql,N'@n bigint OUTPUT',@n=@expected OUTPUT;
DECLARE @upper_keys nvarchar(1000)=NULL;
IF @mode='INCREMENTAL'
    SET @upper_keys=N'{"'+@k1+N'":'+CONVERT(nvarchar(20),@u1)
        +CASE WHEN @k2 IS NULL THEN N'' ELSE N',"'+@k2+N'":'+CONVERT(nvarchar(20),@u2) END+N'}';
-- Copy receives a single SELECT, not a database-switching batch. Bound the
-- query before Lookup serializes it; its JSON-escaped result stays far below
-- Lookup's 4-MB limit even with worst-case Unicode escaping.
DECLARE @extract_query nvarchar(max)=N'SELECT '+@columns+N' FROM '+@qualified
    +N' WHERE '+@where+N' ORDER BY '+@order+N';';
IF @extract_query IS NULL OR DATALENGTH(@extract_query)>65536
    THROW 51111, 'Extraction query exceeds the 32768 UTF-16-unit plan budget.', 1;
SELECT 1 AS plan_contract_version,
       @config_id AS config_id, @schema AS source_schema, @table AS source_table,
       @mode AS load_type, @path AS bronze_relative_path,
       CONVERT(varchar(20),@expected) AS expected_rows,
       ISNULL(CONVERT(nvarchar(33),@lower,126),N'') AS watermark_lower,
       ISNULL(@keys,N'') AS lower_key_json,
       ISNULL(CONVERT(nvarchar(33),@upper,126),N'') AS watermark_upper,
       ISNULL(@upper_keys,N'') AS upper_key_json,
       ISNULL(@version,'') AS expected_row_version,
       @extract_query AS extract_query;
