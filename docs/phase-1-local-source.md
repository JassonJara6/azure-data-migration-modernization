# Phase 1: local source and discovery

## Decisions and trade-offs

### Source runtime: SQL Server 2022 in Docker

We use the full AdventureWorks 2022 **OLTP** backup in a pinned SQL Server 2022 container. Docker makes the source reproducible and closer to the eventual SQL source than importing CSV files. The trade-off is a larger download and higher memory usage. SQLite or static CSVs would be lighter, but would hide SQL Server types, schemas, keys, and extraction behavior that ADF must handle.

The Developer edition is free for development but is not licensed for production. The backup is downloaded at setup time rather than committed to Git. The image is pinned to a cumulative-update tag to avoid accidental version drift; upgrades should be explicit and tested.

### Incremental strategy: `ModifiedDate` plus the primary key

Having a `ModifiedDate` column is not enough to justify incremental ingestion. Its values must meaningfully distinguish changes, and the avoided read volume must justify cursor state and recovery complexity. The five larger/change-oriented tables use INCREMENTAL; the six small reference/dimension tables use FULL. `Sales.Customer` intentionally uses FULL because profiling showed all rows currently share one `ModifiedDate`, making it a poor watermark for this dataset.

Incremental extraction will use a fixed batch upper boundary and a deterministic cursor made from `ModifiedDate` plus the primary key, rather than naïvely applying only `ModifiedDate > last_watermark`. This prevents rows that share a timestamp from being skipped across batches. This method captures inserts and updates, but **not hard deletes**. For a production source, SQL Server Change Tracking or CDC is preferable when direct delete propagation or an exact change feed is required. For this portfolio, periodic full key reconciliation can detect deletions without introducing CDC administration in Phase 1.

## Source domains and relationships

| Table | Grain / key | Important relationships | Planned role |
|---|---|---|---|
| `Person.Person` | one person / `BusinessEntityID` | customer person identifier; names | customer enrichment |
| `Person.Address` | one address / `AddressID` | order billing and shipping IDs; geography via `StateProvinceID` | order and geography attributes |
| `Person.StateProvince` | one state/province / `StateProvinceID` | address child; country/region parent | geography hierarchy |
| `Person.CountryRegion` | one country/region / `CountryRegionCode` | state/province child | geography hierarchy |
| `Production.ProductCategory` | one category / `ProductCategoryID` | parent of subcategory | product hierarchy |
| `Production.ProductSubcategory` | one subcategory / `ProductSubcategoryID` | category parent; product child | product hierarchy |
| `Production.Product` | one product / `ProductID` | optional subcategory | product dimension source |
| `Sales.Customer` | one customer / `CustomerID` | optional `PersonID` or `StoreID`; territory | customer dimension bridge |
| `Sales.SalesOrderHeader` | one order / `SalesOrderID` | customer, salesperson, territory, bill/ship address | sales transaction header |
| `Sales.SalesOrderDetail` | one order line / (`SalesOrderID`, `SalesOrderDetailID`) | order header and product | eventual fact grain |
| `Sales.SalesTerritory` | one territory / `TerritoryID` | customer and order territory | territory dimension source |

The finalized source scope contains these 11 tables. `Person.Address` → `Person.StateProvince` → `Person.CountryRegion` will support Gold `DimGeography`. `Sales.Store` is intentionally excluded for now. The eventual Gold sales fact should be at **sales order line** grain. Header measures such as tax and freight cannot be copied to every detail row without double-counting; either allocate them with an explicit rule or model order-level measures separately. Gold design is deferred to Phase 5.

The approved loading split is:

- **INCREMENTAL:** `Person.Person`, `Person.Address`, `Production.Product`, `Sales.SalesOrderHeader`, and `Sales.SalesOrderDetail`.
- **FULL:** `Person.StateProvince`, `Person.CountryRegion`, `Production.ProductCategory`, `Production.ProductSubcategory`, `Sales.Customer`, and `Sales.SalesTerritory`.

The profile was executed successfully against the restored local source. The validated baseline is:

| Table | Rows | Distinct `ModifiedDate` values | Mode |
|---|---:|---:|---|
| `Person.Address` | 19,614 | 1,280 | INCREMENTAL |
| `Person.CountryRegion` | 238 | 1 | FULL |
| `Person.Person` | 19,972 | 1,285 | INCREMENTAL |
| `Person.StateProvince` | 181 | 2 | FULL |
| `Production.Product` | 504 | 2 | INCREMENTAL |
| `Production.ProductCategory` | 4 | 1 | FULL |
| `Production.ProductSubcategory` | 37 | 1 | FULL |
| `Sales.Customer` | 19,820 | 1 | FULL |
| `Sales.SalesOrderDetail` | 121,317 | 1,124 | INCREMENTAL |
| `Sales.SalesOrderHeader` | 31,465 | 1,124 | INCREMENTAL |
| `Sales.SalesTerritory` | 10 | 1 | FULL |

Primary keys and the expected geography, sales, and product foreign keys were confirmed. All six current quality checks returned zero invalid records: invalid order totals, ship-before-order, missing customer, nonpositive quantity, negative unit price, and invalid discount. These are baseline observations, not guarantees that future batches will remain valid.

Although `Production.Product` is small and has only two distinct modification dates in the sample, it intentionally remains INCREMENTAL. FULL would be operationally reasonable for this data, but the incremental choice demonstrates reuse of the same safe ingestion pattern for master/reference entities that may be larger and change more frequently in production.

## Local operation

1. Copy `.env.example` to `.env` and replace the password. The committed example is deliberately not used automatically.
2. Run `make source-up`, then `make source-setup`. Re-running setup is safe: it retains the downloaded backup and skips an existing `AdventureWorks` database.
3. Connect with host `localhost`, port from `MSSQL_PORT` (default `1433`), database `AdventureWorks`, user `sa`, and the local password. Trust the local development certificate when prompted.
4. Run `make source-explore`; inspect `artifacts/source-profile.txt`.
5. Run `make source-down` to stop. `make source-reset` irreversibly removes the database volume, backup, and generated profile.

Do not reuse the `sa` account for Azure ingestion. Before Phase 3, create a least-privilege extraction login/user with `SELECT` only on approved source objects. The local `sa` credential exists solely to restore and administer the developer database.

## Phase 2 entry decisions (not yet approved or implemented)

- Confirm Azure subscription access, tenant, region, naming prefix, and a cost budget/alerts.
- Confirm local tooling for Phase 2: Azure CLI, Terraform, and an Azure identity allowed to create the scoped resources and role assignments.
- Choose how ADF will reach the source. A local Docker SQL Server requires a self-hosted integration runtime and an always-on reachable machine; this is realistic but awkward for a portfolio demo. A small Azure SQL source loaded from AdventureWorks is simpler to demonstrate but incurs cost. Make this choice explicitly in Phase 2/3.
- Select the exact columns to ingest and identify sensitive fields; do not land `Person.Person` demographics XML or other unused personal attributes merely because they exist.
- Record the source timezone convention (AdventureWorks sample dates have no timezone) and treat source values consistently.
- Establish resource tags and separate development configuration from secrets. Later credentials belong in Key Vault, never Terraform variables committed to Git.

## Phase 1 completion checklist

- [x] Docker source is healthy and `AdventureWorks` restores successfully.
- [x] Profiling SQL completes and its counts/`ModifiedDate` distributions have been reviewed.
- [x] The 11-table source scope and FULL/INCREMENTAL split are approved.
- [x] Order-line fact grain and geography/product/customer source scope are agreed.
- [x] Boundary-safe incremental semantics, hard-delete limitation, and reconciliation strategy are documented.
- [x] Primary keys and expected geography, sales, and product relationships are confirmed.
- [x] Baseline quality checks complete with zero current invalid records.
- [x] No credentials, database backups, or generated profiles are tracked by Git.

Phase 1 is formally complete. Phase 2 must not begin until the project owner explicitly approves proceeding and resolves the entry decisions above.
