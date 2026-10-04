# Draft source contract

This is the Phase 1 working contract. Profiling results and business scope must be reviewed before it becomes ingestion metadata in Phase 3.

| Source object | Primary key | Extraction proposal | Expected Gold use |
|---|---|---|---|
| `Person.Person` | `BusinessEntityID` | full seed, then `ModifiedDate` watermark | customer name |
| `Person.Address` | `AddressID` | full seed, then `ModifiedDate` watermark | order address |
| `Production.Product` | `ProductID` | full seed, then `ModifiedDate` watermark | product dimension |
| `Production.ProductCategory` | `ProductCategoryID` | full seed, then `ModifiedDate` watermark | product hierarchy |
| `Production.ProductSubcategory` | `ProductSubcategoryID` | full seed, then `ModifiedDate` watermark | product hierarchy |
| `Sales.Customer` | `CustomerID` | full seed, then `ModifiedDate` watermark | customer dimension |
| `Sales.SalesOrderHeader` | `SalesOrderID` | full seed, then `ModifiedDate` watermark | order context/measures |
| `Sales.SalesOrderDetail` | (`SalesOrderID`, `SalesOrderDetailID`) | full seed, then `ModifiedDate` watermark | sales fact |
| `Sales.SalesTerritory` | `TerritoryID` | full seed, then `ModifiedDate` watermark | territory dimension |

## Contract rules to carry forward

- Bronze preserves source values, adds ingestion metadata, and is immutable by run; it does not silently fix records.
- A batch records both lower and upper watermark boundaries before extraction. Advance the durable watermark only after landing, validation, and audit completion succeed.
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

Thresholds, nullability, selected columns, and quarantine severity remain deliberately undecided until the Phase 1 profile is reviewed.
