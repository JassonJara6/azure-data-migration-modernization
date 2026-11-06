"""Current-state (Type 1) dimensions and order-line measures, exact decimals."""
from pyspark.sql import functions as F, types as T, Window
from admm_silver.contracts import TABLES

KEYS={
    'dim_product':['product_key'], 'dim_customer':['customer_key'], 'dim_date':['date_key'],
    'fact_sales':['sales_order_id','sales_order_detail_id'],
    'agg_sales_daily':['date_key','currency_rate_id'],
    'agg_product_month':['month_key','product_key','currency_rate_id'],
    'agg_customer_month':['month_key','customer_key','currency_rate_id']}


class GoldError(RuntimeError):
    def __init__(self,code,metrics=None):
        super().__init__(code);self.metrics=metrics or {}


def require(condition,code,metrics=None):
    if not condition:raise GoldError(code,metrics)


def build(frames):
    """Inputs are pinned Silver DataFrames; preserve optional hierarchy members."""
    require(set(frames)=={table['target'] for table in TABLES},'GOLD_ELEVEN_SILVER_SOURCES_REQUIRED')
    for table in TABLES:
        frame=frames[table['target']]
        require(len(frame.columns)==len(set(frame.columns)),'GOLD_SOURCE_SCHEMA_INVALID')
        for column in table['columns']:
            require(column['name'] in frame.columns,'GOLD_SOURCE_SCHEMA_INVALID')
            actual=frame.schema[column['name']].dataType
            expected=column['type']
            require(isinstance(actual,(T.StringType,T.BinaryType)) if expected=='opaque'
                    else actual==T._parse_datatype_string(expected),'GOLD_SOURCE_TYPE_INVALID')
    products=frames['production_product'].alias('p')
    sub=frames['production_productsubcategory'].alias('s')
    cat=frames['production_productcategory'].alias('c')
    product=(products.join(sub,F.col('p.ProductSubcategoryID')==F.col('s.ProductSubcategoryID'),'left')
        .join(cat,F.col('s.ProductCategoryID')==F.col('c.ProductCategoryID'),'left')
        .select(F.col('p.ProductID').alias('product_key'),F.trim('p.ProductNumber').alias('product_number'),
            F.trim('p.Name').alias('product_name'),F.col('s.ProductSubcategoryID').alias('subcategory_id'),
            F.trim('s.Name').alias('subcategory_name'),F.col('c.ProductCategoryID').alias('category_id'),
            F.trim('c.Name').alias('category_name')))
    customer=(frames['sales_customer'].alias('c').join(frames['person_person'].alias('p'),
        F.col('c.PersonID')==F.col('p.BusinessEntityID'),'left')
        .select(F.col('c.CustomerID').alias('customer_key'),F.col('c.AccountNumber').alias('account_number'),
            F.col('c.PersonID').alias('person_id'),F.col('c.StoreID').alias('store_id'),
            F.when(F.col('c.StoreID').isNotNull(),'STORE').when(F.col('c.PersonID').isNotNull(),'INDIVIDUAL')
                .otherwise('UNCLASSIFIED').alias('customer_type'),
            F.when(F.col('c.PersonID').isNotNull(),F.concat_ws(' ',F.trim('p.FirstName'),F.trim('p.LastName')))
                .alias('person_display_name'),F.col('c.TerritoryID').alias('territory_id')))
    joined=frames['sales_salesorderdetail'].alias('d').join(frames['sales_salesorderheader'].alias('h'),'SalesOrderID')
    gross=(F.col('d.UnitPrice')*F.col('d.OrderQty')).cast('decimal(28,6)')
    fact=joined.select(F.col('SalesOrderID').alias('sales_order_id'),F.col('d.SalesOrderDetailID').alias('sales_order_detail_id'),
        F.col('d.ProductID').alias('product_key'),F.col('h.CustomerID').alias('customer_key'),
        F.date_format('h.OrderDate','yyyyMMdd').cast('int').alias('date_key'),
        F.to_date('h.OrderDate').alias('order_date'),F.col('h.TerritoryID').alias('territory_id'),
        F.col('h.Status').alias('order_status'),F.col('h.CurrencyRateID').alias('currency_rate_id'),
        F.col('d.OrderQty').alias('quantity'),F.col('d.UnitPrice').alias('unit_price'),
        F.col('d.UnitPriceDiscount').alias('discount_rate'),gross.alias('gross_sales'),
        (gross-F.col('d.LineTotal').cast('decimal(28,6)')).cast('decimal(28,6)').alias('discount_amount'),
        F.col('d.LineTotal').alias('net_sales'))
    # A continuous observed sales calendar; no invented facts on dates without orders.
    bounds=fact.agg(F.min('order_date').alias('start'),F.max('order_date').alias('end'))
    dates=(bounds.filter('start IS NOT NULL').select(F.explode(F.sequence('start','end')).alias('date'))
        .select(F.date_format('date','yyyyMMdd').cast('int').alias('date_key'),'date',
            F.year('date').alias('year'),F.month('date').alias('month'),F.dayofmonth('date').alias('day'),
            F.quarter('date').alias('quarter'),F.dayofweek('date').alias('day_of_week')))
    def aggregate(keys):
        return fact.withColumn('month_key',F.date_format('order_date','yyyyMM').cast('int')).groupBy(*keys).agg(
            F.countDistinct('sales_order_id').alias('order_count'),F.count('*').alias('line_count'),
            F.sum('quantity').alias('quantity'),F.sum('gross_sales').alias('gross_sales'),
            F.sum('discount_amount').alias('discount_amount'),F.sum('net_sales').alias('net_sales'))
    return dict(dim_product=product,dim_customer=customer,dim_date=dates,fact_sales=fact,
        agg_sales_daily=aggregate(KEYS['agg_sales_daily']),agg_product_month=aggregate(KEYS['agg_product_month']),
        agg_customer_month=aggregate(KEYS['agg_customer_month']))


def validate(frames,outputs):
    """Fail the whole snapshot before publication; no dedup/drop/fabricated members."""
    metrics={};bad=False
    for name,df in outputs.items():
        # Nullable currency_rate_id is a valid grouping key, never a dimension FK.
        nonnull=[k for k in KEYS[name] if k!='currency_rate_id']
        null_condition=F.expr(' OR '.join('`'+k+'` IS NULL' for k in nonnull))
        checked=df.withColumn('_key_count',F.count('*').over(Window.partitionBy(*KEYS[name])))
        def count_if(condition):return F.coalesce(F.sum(F.when(condition,1).otherwise(0)),F.lit(0))
        stats=checked.agg(F.count('*').alias('rows'),count_if(null_condition).alias('nulls'),
            count_if(F.col('_key_count')>1).alias('duplicates'),
            count_if(null_condition | (F.col('_key_count')>1)).alias('rejected')).first()
        metrics[name]={'rows':stats.rows,'null_key_rows':stats.nulls,'duplicate_rows':stats.duplicates,
                      'rejected_rows':stats.rejected}
        bad=bad or bool(stats.rejected)
    require(not bad,'GOLD_KEY_QUALITY_FAILED',metrics)
    fact=outputs['fact_sales'];fk={}
    invalid=fact.filter('quantity IS NULL OR quantity<=0 OR unit_price IS NULL OR unit_price<0 OR '
        'discount_rate IS NULL OR discount_rate<0 OR discount_rate>1 OR gross_sales IS NULL OR '
        'net_sales IS NULL OR net_sales<0 OR discount_amount IS NULL OR discount_amount<0 OR '
        'order_date IS NULL').count()
    metrics['fact_sales']['rejected_rows']=invalid
    require(not invalid,'GOLD_MEASURE_QUALITY_FAILED',metrics)
    for dim,key in [('dim_product','product_key'),('dim_customer','customer_key'),('dim_date','date_key')]:
        fk[key]=fact.join(outputs[dim].select(key),key,'left_anti').count()
    metrics['referential_integrity']=fk
    if any(fk.values()):
        rejected=None
        for dim,key in [('dim_product','product_key'),('dim_customer','customer_key'),('dim_date','date_key')]:
            missing=fact.join(outputs[dim].select(key),key,'left_anti').select(*KEYS['fact_sales'])
            rejected=missing if rejected is None else rejected.unionByName(missing)
        metrics['fact_sales']['rejected_rows']=rejected.distinct().count()
    require(not any(fk.values()),'GOLD_FOREIGN_KEY_FAILED',metrics)
    # Compare raw Silver keys too: an inner join must never silently discard lines.
    raw=frames['sales_salesorderdetail']
    metrics['sales_reconciliation']={'source_lines':raw.count(),'fact_lines':metrics['fact_sales']['rows']}
    require(raw.count()==metrics['fact_sales']['rows'],'GOLD_LINE_COUNT_MISMATCH',metrics)
    orphan=raw.join(frames['sales_salesorderheader'].select('SalesOrderID'),'SalesOrderID','left_anti').count()
    require(not orphan,'GOLD_HEADER_MISSING',metrics)
    sums=raw.groupBy('SalesOrderID').agg(F.sum('LineTotal').alias('detail_total'))
    detail=F.col('detail_total').cast('decimal(28,6)')
    mismatch=(frames['sales_salesorderheader'].join(sums,'SalesOrderID','left')
        .filter((F.col('detail_total').isNotNull() & detail.isNull()) |
            (F.abs(F.col('SubTotal').cast('decimal(28,6)')-F.coalesce(detail,F.lit(0)))>
             F.lit('0.0001').cast('decimal(28,6)'))).count())
    metrics['sales_reconciliation']['header_subtotal_mismatches']=mismatch
    require(not mismatch,'GOLD_HEADER_SUBTOTAL_MISMATCH',metrics)
    totals=fact.agg(F.sum('net_sales').alias('net'),F.sum('quantity').alias('quantity')).first()
    require(not metrics['fact_sales']['rows'] or (totals.net is not None and totals.quantity is not None),'GOLD_AGGREGATE_OVERFLOW',metrics)
    metrics['sales_reconciliation'].update(net_sales=str(totals.net or 0),quantity=int(totals.quantity or 0))
    for name in ('agg_sales_daily','agg_product_month','agg_customer_month'):
        total=outputs[name].agg(F.sum('net_sales').alias('net'),F.sum('quantity').alias('quantity')).first()
        require(total.net==totals.net and total.quantity==totals.quantity,'GOLD_AGGREGATE_MISMATCH',metrics)
    return metrics
