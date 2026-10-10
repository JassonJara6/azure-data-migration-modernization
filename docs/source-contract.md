# Phase 1 source contract

The Phase 1 source scope and initial extraction modes below were approved and validated against the local AdventureWorks source. This contract will become ingestion metadata in Phase 3; it does not implement ingestion yet.

| Source object | Primary key | Extraction mode | Watermark | Rationale / expected use |
|---|---|---|---|---|
| `Person.Person` | `BusinessEntityID` | **INCREMENTAL** | `ModifiedDate` + PK | customer name; enough change distribution to justify state |
| `Person.Address` | `AddressID` | **INCREMENTAL** | `ModifiedDate` + PK | geography attributes; enough change distribution to justify state |
| `Person.StateProvince` | `StateProvinceID` | **FULL** | — | small reference table for geography |
| `Person.CountryRegion` | `CountryRegionCode` | **FULL** | — | small reference table for geography |
| `Production.Product` | `ProductID` | **INCREMENTAL** | `ModifiedDate` + PK | intentional demonstration of reusable master-data incremental ingestion |
| `Production.ProductCategory` | `ProductCategoryID` | **FULL** | — | small product-hierarchy reference table |
| `Production.ProductSubcategory` | `ProductSubcategoryID` | **FULL** | — | small product-hierarchy reference table |
| `Sales.Customer` | `CustomerID` | **FULL** | — | customer dimension; all current rows share one `ModifiedDate` |
| `Sales.SalesOrderHeader` | `SalesOrderID` | **INCREMENTAL** | `ModifiedDate` + PK | order context and header measures |
| `Sales.SalesOrderDetail` | (`SalesOrderID`, `SalesOrderDetailID`) | **INCREMENTAL** | `ModifiedDate` + composite PK | sales fact source |
| `Sales.SalesTerritory` | `TerritoryID` | **FULL** | — | small territory reference table |

`Person.Address` → `Person.StateProvince` → `Person.CountryRegion` supplies the hierarchy for a useful Gold `DimGeography`. `Sales.Store` is intentionally outside the current scope.

## Extraction decisions

The presence of `ModifiedDate` does not automatically make a table a good incremental candidate. A useful watermark must vary with meaningful source changes, have understood precision and semantics, and reduce the extraction enough to justify durable state, retry, and delete-handling complexity. The small reference/dimension tables use FULL loads because their volume does not justify that complexity.

`Sales.Customer` also uses FULL even though it has `ModifiedDate`: profiling found that every current row shares the same value, so it cannot usefully partition changes in this dataset. This is a deliberate data-driven exception, not a missing capability.

`Production.Product` is intentionally INCREMENTAL even though this sample has only 504 rows and two distinct `ModifiedDate` values, for which a FULL load would be operationally reasonable. Keeping it incremental demonstrates that the reusable pattern supports master/reference entities that may be substantially larger and updated more frequently in a production source.

For an INCREMENTAL table, do not use only `ModifiedDate > last_watermark`. Multiple rows can share a timestamp, and a failed or concurrent extraction can otherwise skip rows. Phase 3 should capture a fixed upper boundary and use a deterministic `(ModifiedDate, primary key)` cursor, for example:

```text
(ModifiedDate > previous_date OR
 (ModifiedDate = previous_date AND primary_key > previous_key))
AND ModifiedDate <= batch_upper_date
```

Composite keys require a deterministic lexicographic tie-break. Persist the new cursor only after the landing, validation, reconciliation, and audit steps succeed. The exact ADF query and state schema are deferred to Phase 3.

`ModifiedDate` captures inserts and updates, not hard deletes. The portfolio scope will use periodic full key reconciliation to detect deletion differences. Production systems that require direct delete propagation or a true change feed should evaluate SQL Server Change Tracking or CDC.

## Contract rules to carry forward

- Bronze preserves source values, adds ingestion metadata, and is immutable by run; it does not silently fix records.
- An incremental batch records its lower cursor and upper boundary before extraction. Advance the durable cursor only after landing, validation, reconciliation, and audit completion succeed.
- Replays use the same batch boundaries and deterministic destination path, with an explicit overwrite/idempotency policy.
- Reconciliation compares source row count with landed row count for every batch and records exceptions.
- Silver enforces key presence/uniqueness, relationship checks, type expectations, and business rules. Invalid rows retain raw context plus reason codes in quarantine.
- Schema drift fails closed for incompatible changes; compatible additive columns are detected and reviewed rather than silently promoted to Gold.
- Minimize personal data. Only ingest columns needed by the stated analytical use case, and never log raw sensitive values.

## Initial quality rules to validate in Phase 4

- Primary keys are non-null and unique at their declared grain.
- Order details reference an existing order header and product.
- Product subcategories reference a category; products with a populated subcategory reference an existing subcategory.
- `OrderQty > 0`, `UnitPrice >= 0`, and `UnitPriceDiscount` is between 0 and 1.
- `ShipDate` is null or is not earlier than `OrderDate`.
- Header `TotalDue` reconciles to `SubTotal + TaxAmt + Freight` within the source currency precision.

Detailed thresholds, selected-column projections, and quarantine severity will be designed with the Silver implementation in Phase 4; they are not blockers to closing source discovery.
