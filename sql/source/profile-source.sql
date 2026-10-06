SET NOCOUNT ON;
USE [AdventureWorks];

PRINT '=== Candidate table row counts and watermark ranges ===';
SELECT 'Person.Person' AS table_name, COUNT_BIG(*) AS row_count, MIN(ModifiedDate) AS min_modified, MAX(ModifiedDate) AS max_modified FROM Person.Person
UNION ALL SELECT 'Person.Address', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate) FROM Person.Address
UNION ALL SELECT 'Production.Product', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate) FROM Production.Product
UNION ALL SELECT 'Production.ProductCategory', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate) FROM Production.ProductCategory
UNION ALL SELECT 'Production.ProductSubcategory', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate) FROM Production.ProductSubcategory
UNION ALL SELECT 'Sales.Customer', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate) FROM Sales.Customer
UNION ALL SELECT 'Sales.SalesOrderHeader', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate) FROM Sales.SalesOrderHeader
UNION ALL SELECT 'Sales.SalesOrderDetail', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate) FROM Sales.SalesOrderDetail
UNION ALL SELECT 'Sales.SalesTerritory', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate) FROM Sales.SalesTerritory
ORDER BY table_name;

PRINT '=== Candidate primary keys ===';
SELECT CONCAT(s.name, '.', t.name) AS table_name, c.name AS key_column, ic.key_ordinal
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.indexes AS i ON i.object_id = t.object_id AND i.is_primary_key = 1
JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE CONCAT(s.name, '.', t.name) IN
('Person.Person', 'Person.Address', 'Production.Product', 'Production.ProductCategory',
 'Production.ProductSubcategory', 'Sales.Customer', 'Sales.SalesOrderHeader',
 'Sales.SalesOrderDetail', 'Sales.SalesTerritory')
ORDER BY table_name, ic.key_ordinal;

PRINT '=== Candidate foreign-key relationships ===';
SELECT CONCAT(ps.name, '.', pt.name) AS child_table, pc.name AS child_column,
       CONCAT(rs.name, '.', rt.name) AS parent_table, rc.name AS parent_column, fk.name AS constraint_name
FROM sys.foreign_keys AS fk
JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
JOIN sys.tables AS pt ON pt.object_id = fk.parent_object_id
JOIN sys.schemas AS ps ON ps.schema_id = pt.schema_id
JOIN sys.columns AS pc ON pc.object_id = pt.object_id AND pc.column_id = fkc.parent_column_id
JOIN sys.tables AS rt ON rt.object_id = fk.referenced_object_id
JOIN sys.schemas AS rs ON rs.schema_id = rt.schema_id
JOIN sys.columns AS rc ON rc.object_id = rt.object_id AND rc.column_id = fkc.referenced_column_id
WHERE CONCAT(ps.name, '.', pt.name) IN
('Person.Person', 'Person.Address', 'Production.Product', 'Production.ProductCategory',
 'Production.ProductSubcategory', 'Sales.Customer', 'Sales.SalesOrderHeader',
 'Sales.SalesOrderDetail', 'Sales.SalesTerritory')
ORDER BY child_table, fk.name, fkc.constraint_column_id;

PRINT '=== Basic quality observations ===';
SELECT
    SUM(CASE WHEN TotalDue <> SubTotal + TaxAmt + Freight THEN 1 ELSE 0 END) AS invalid_order_totals,
    SUM(CASE WHEN ShipDate < OrderDate THEN 1 ELSE 0 END) AS ship_before_order,
    SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END) AS missing_customer
FROM Sales.SalesOrderHeader;

SELECT
    SUM(CASE WHEN OrderQty <= 0 THEN 1 ELSE 0 END) AS nonpositive_quantity,
    SUM(CASE WHEN UnitPrice < 0 THEN 1 ELSE 0 END) AS negative_unit_price,
    SUM(CASE WHEN UnitPriceDiscount < 0 OR UnitPriceDiscount > 1 THEN 1 ELSE 0 END) AS invalid_discount
FROM Sales.SalesOrderDetail;
