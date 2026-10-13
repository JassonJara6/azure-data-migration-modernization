USE AdventureWorks;
GO

GRANT SELECT ON OBJECT::Person.Person TO admm_ingestion;
GRANT SELECT ON OBJECT::Person.Address TO admm_ingestion;
GRANT SELECT ON OBJECT::Person.StateProvince TO admm_ingestion;
GRANT SELECT ON OBJECT::Person.CountryRegion TO admm_ingestion;

GRANT SELECT ON OBJECT::Production.Product TO admm_ingestion;
GRANT SELECT ON OBJECT::Production.ProductCategory TO admm_ingestion;
GRANT SELECT ON OBJECT::Production.ProductSubcategory TO admm_ingestion;

GRANT SELECT ON OBJECT::Sales.Customer TO admm_ingestion;
GRANT SELECT ON OBJECT::Sales.SalesOrderHeader TO admm_ingestion;
GRANT SELECT ON OBJECT::Sales.SalesOrderDetail TO admm_ingestion;
GRANT SELECT ON OBJECT::Sales.SalesTerritory TO admm_ingestion;
GO