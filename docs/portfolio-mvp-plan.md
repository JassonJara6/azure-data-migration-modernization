# Portfolio MVP: Bronze -> Silver -> Gold

## Scope
Keep the existing 11-table ADF landing implementation as a **landing prototype**.
Do not publish or merge the untested audit/reconciliation integration from PR #17.
The parent pipeline's `WATERMARK_CONFIRMATION_BLOCKED` is intentional; do not remove
it or treat the five incremental loads as committed.

## Next milestone: Silver
Create one Databricks notebook that reads existing Parquet Bronze landing folders
from ADLS and writes Delta tables into a Unity Catalog `silver` schema.
Start with three entities: `Sales.SalesOrderHeader`, `Sales.SalesOrderDetail`,
`Production.Product`. Normalize types, validate keys, reject duplicate keys
within each batch, and preserve source metadata. For MVP, use **full snapshot
replacement** from a verified complete landing folder only; do not merge
incremental deltas as if they were full snapshots.

## Gold
Join orders, details and products into a sales fact with order date, product,
quantity, unit price and line total; add sales-by-month and top-products
aggregates. Check that summed fact line totals reconcile with the source
detail rows in the selected snapshot.

## Acceptance
- Silver Delta tables readable with correct schema and non-null keys
- Gold fact and two aggregates with sample queries and basic quality checks
- README with architecture, how to run, and honest limitations
- No SQL watermark mutations or live ADF pipeline changes required for MVP

## Current limitations
The existing incremental Bronze folders are bounded *deltas*, not necessarily
complete snapshots. Before Silver sales analytics, produce a controlled full
baseline for the three selected tables or implement a verified initial load
and subsequent idempotent Delta MERGE with stable keys. Do not assume the
latest run folder alone is complete.
