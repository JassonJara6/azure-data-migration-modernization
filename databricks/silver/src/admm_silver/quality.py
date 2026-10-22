"""Loss-aware PySpark typing and blocking row/relationship quality rules."""
from pyspark.sql import functions as F, Window, types as T
from .contracts import METADATA,require,cursor


def q(name):return '`'+name.replace('`','``')+'`'


def empty_schema(table,opaque_types=None):
    fields=[T.StructField(c['name'],(opaque_types or {}).get(c['name'],T.BinaryType()) if c['type']=='opaque' else T._parse_datatype_string(c['type']),True) for c in table['columns']]
    return T.StructType(fields+[T.StructField(c,T.StringType(),True) for c in sorted(METADATA)])


def transform(raw,table,manifest,parent,units=None):
    require(len(raw.columns)==len(set(raw.columns)) and set(raw.columns)==METADATA|{c['name'] for c in table['columns']},'SCHEMA_COLUMNS_MISMATCH')
    units=units or {};errors=[];values=[];ticks_columns=[]
    # Raw JSON is retained in restricted quarantine only, never in driver logs.
    raw=raw.withColumn('_silver_raw_json',F.to_json(F.struct(*[F.col(q(c)) for c in raw.columns]),options={'ignoreNullFields':'false'}))
    for column in table['columns']:
        name=column['name'];target=column['type'];original=F.col(q(name));physical=raw.schema[name].dataType
        if target=='opaque':
            require(isinstance(physical,(T.StringType,T.BinaryType)),'OPAQUE_SCHEMA_UNSUPPORTED')
            converted=original
        elif target=='timestamp':
            unit=units.get(name)
            if isinstance(physical,T.LongType):
                require(unit=='ns','TIMESTAMP_INTEGER_UNIT_UNKNOWN')
                exact=F.expr(f'{q(name)} DIV 100')
                converted=F.expr(f'timestamp_micros(({q(name)} - pmod({q(name)},1000)) DIV 1000)')
                errors.append((original%100!=0,'SUB_100NS_TIMESTAMP'))
            else:
                require(isinstance(physical,(T.TimestampType,T.TimestampNTZType,T.StringType)),'TIMESTAMP_SCHEMA_UNSUPPORTED')
                converted=F.expr(f'try_cast({q(name)} AS timestamp)')
                exact=F.unix_micros(converted)*10
                if isinstance(physical,T.StringType):
                    # Preserve the seventh SQL fractional digit independently.
                    fractional=F.regexp_extract(original,r'\.(\d{1,7})(?:Z)?$',1)
                    seventh=F.substring(F.rpad(fractional,7,'0'),7,1).cast('long')
                    exact=exact+F.coalesce(seventh,F.lit(0))
                    errors.append((original.rlike(r'\.\d{8,}'),'SUB_100NS_TIMESTAMP'))
            ticks_columns.append(exact.alias(name+'__ticks_100ns'))
        else:
            if target.startswith('decimal'):
                require(isinstance(physical,(T.DecimalType,T.StringType,T.IntegralType)),'DECIMAL_SCHEMA_UNSUPPORTED')
            elif target in ('int','smallint'):
                require(isinstance(physical,(T.IntegralType,T.StringType)),'INTEGER_SCHEMA_UNSUPPORTED')
            elif target=='boolean':
                require(isinstance(physical,(T.BooleanType,T.IntegralType,T.StringType)),'BOOLEAN_SCHEMA_UNSUPPORTED')
                errors.append((original.isNotNull() & ~original.cast('string').isin('0','1','true','false'),'INVALID_BOOLEAN'))
            else:require(isinstance(physical,T.StringType),'STRING_SCHEMA_UNSUPPORTED')
            converted=F.expr(f'try_cast({q(name)} AS {target})')
            if target.startswith('decimal'):
                # Spark decimal casts round scale; detect rather than hide it.
                errors.append((original.isNotNull() & (original.cast('decimal(38,10)')!=converted.cast('decimal(38,10)')),'LOSSY_DECIMAL_CAST'))
        if column['sql_type']=='tinyint':errors.append((~converted.between(0,255),'SOURCE_INTEGER_RANGE'))
        errors.append((original.isNotNull() & converted.isNull(),'INVALID_TYPE'))
        if not column['nullable']:errors.append((converted.isNull(),'REQUIRED_COLUMN_NULL'))
        values.append(converted.alias(name))
    checks=[F.when(F.coalesce(condition,F.lit(False)),F.lit(code)) for condition,code in errors]
    df=raw.select(*values,*ticks_columns,*[F.col(q(c)) for c in sorted(METADATA)],'_silver_raw_json',
          F.filter(F.array(*checks),lambda x:x.isNotNull()).alias('_quality_errors'))
    def add(condition,code):
        nonlocal df
        df=df.withColumn('_quality_errors',F.when(F.coalesce(condition,F.lit(False)),
             F.array_union('_quality_errors',F.array(F.lit(code)))).otherwise(F.col('_quality_errors')))
    if 'rowguid' in df.columns:add(~F.col('rowguid').rlike(r'^[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$'),'INVALID_UUID')
    add(F.expr(' OR '.join(q(k)+' IS NULL' for k in table['pk'])),'NULL_PK')
    add(F.count('*').over(Window.partitionBy(*table['pk']))>1,'DUPLICATE_PK')
    expected={'_pipeline_run_id':manifest['run_id'],'_parent_pipeline_run_id':parent,
              '_source_system':'adventureworks','_load_type':table['mode'],
              '_watermark_lower':manifest['lower'],'_watermark_upper':manifest['upper'],
              '_watermark_lower_key_json':manifest['lower_key_json'],'_watermark_upper_key_json':manifest['upper_key_json']}
    for name,value in expected.items():add(~F.col(name).eqNullSafe(F.lit(value)),'BRONZE_METADATA_MISMATCH')
    add(F.expr('try_cast(_ingested_at_utc AS timestamp)').isNull(),'INVALID_INGESTION_TIME')
    rules={
      'Person.Person':"EmailPromotion BETWEEN 0 AND 2 AND upper(trim(PersonType)) IN ('SC','VC','IN','EM','SP','GC')",
      'Production.Product':'SafetyStockLevel>0 AND ReorderPoint>0 AND StandardCost>=0 AND ListPrice>=0 AND (Weight IS NULL OR Weight>0) AND DaysToManufacture>=0 AND (SellEndDate IS NULL OR SellEndDate>=SellStartDate)',
      'Sales.SalesOrderDetail':'OrderQty>0 AND UnitPrice>=0 AND UnitPriceDiscount BETWEEN 0 AND 1 AND abs(LineTotal - UnitPrice*(1-UnitPriceDiscount)*OrderQty)<=0.000001',
      'Sales.SalesOrderHeader':'Status BETWEEN 0 AND 8 AND DueDate>=OrderDate AND (ShipDate IS NULL OR ShipDate>=OrderDate) AND SubTotal>=0 AND TaxAmt>=0 AND Freight>=0 AND abs(TotalDue-SubTotal-TaxAmt-Freight)<=0.0001',
      'Sales.SalesTerritory':'SalesYTD>=0 AND SalesLastYear>=0 AND CostYTD>=0 AND CostLastYear>=0'}
    if table['source'] in rules:add(~F.expr(rules[table['source']]),'BUSINESS_RULE_FAILED')
    if table['mode']=='INCREMENTAL':
        low=cursor(manifest['lower'],manifest['lower_key_json'],table['pk']);high=cursor(manifest['upper'],manifest['upper_key_json'],table['pk'])
        value=F.struct(*[F.col(q(k)).cast('long').alias('p'+str(i)) for i,k in enumerate(['ModifiedDate__ticks_100ns']+table['pk'])])
        bound=lambda values:F.struct(*[F.lit(v).cast('long').alias('p'+str(i)) for i,v in enumerate(values)])
        add((value<=bound(low)) | (value>bound(high)),'CURSOR_OUT_OF_BOUNDS')
    # Keep original business values. Normalized display/search fields are additive.
    for name in ('Name','FirstName','MiddleName','LastName','City','PostalCode','PersonType','CountryRegionCode','StateProvinceCode','ProductNumber'):
        if name in df.columns:df=df.withColumn(name+'__normalized',F.trim(F.col(q(name))));
    return df


RELATIONSHIPS=[
 ('Person.Address','StateProvinceID','Person.StateProvince','StateProvinceID'),
 ('Person.StateProvince','CountryRegionCode','Person.CountryRegion','CountryRegionCode'),
 ('Person.StateProvince','TerritoryID','Sales.SalesTerritory','TerritoryID'),
 ('Sales.SalesTerritory','CountryRegionCode','Person.CountryRegion','CountryRegionCode'),
 ('Production.ProductSubcategory','ProductCategoryID','Production.ProductCategory','ProductCategoryID'),
 ('Production.Product','ProductSubcategoryID','Production.ProductSubcategory','ProductSubcategoryID'),
 ('Sales.Customer','PersonID','Person.Person','BusinessEntityID'),
 ('Sales.Customer','TerritoryID','Sales.SalesTerritory','TerritoryID'),
 ('Sales.SalesOrderHeader','CustomerID','Sales.Customer','CustomerID'),
 ('Sales.SalesOrderHeader','TerritoryID','Sales.SalesTerritory','TerritoryID'),
 ('Sales.SalesOrderHeader','BillToAddressID','Person.Address','AddressID'),
 ('Sales.SalesOrderHeader','ShipToAddressID','Person.Address','AddressID'),
 ('Sales.SalesOrderDetail','SalesOrderID','Sales.SalesOrderHeader','SalesOrderID'),
 ('Sales.SalesOrderDetail','ProductID','Production.Product','ProductID')]


def missing_relationships(candidates):
    for child,fk,parent,pk in RELATIONSHIPS:
        df=candidates[child]
        missing=df.filter(F.col(q(fk)).isNotNull()).join(candidates[parent].select(F.col(q(pk)).alias(fk)).distinct(),[fk],'left_anti')
        yield child,missing.withColumn('_quality_errors',F.array(F.lit('FK_'+fk+'_MISSING')))


def business_frame(df,parent,batch_digest):
    columns=[c for c in df.columns if c not in ('_quality_errors','_silver_raw_json')]
    result=df.select(*[F.col(q(c)) for c in columns])
    business=sorted(c for c in columns if not c.startswith('_'))
    return result.withColumn('_silver_row_hash',F.sha2(F.to_json(F.struct(*[F.col(q(c)) for c in business]),options={'ignoreNullFields':'false'}),256))\
        .withColumn('_silver_parent_id',F.lit(parent)).withColumn('_silver_batch_digest',F.lit(batch_digest))
