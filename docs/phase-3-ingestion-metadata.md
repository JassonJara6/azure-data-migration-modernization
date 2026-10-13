# Phase 3: ingestion metadata foundation

This increment prepares the SQL control metadata and Bronze contract for future metadata-driven ADF pipelines. It does **not** deploy or modify pipelines, linked services, Azure infrastructure, or secrets.

## SQL control model

The metadata lives in the local `AdventureWorks` database under the dedicated `ingestion` schema:

- `ingestion.table_config` is the declarative source registry. It records source object, load mode, ordered primary-key columns, optional watermark column, target path, and activation flag.
- `ingestion.watermark_state` contains only the last **successfully committed** cursor for each INCREMENTAL source. FULL sources intentionally have no state row.

Keeping the control tables beside the local portfolio source avoids provisioning another control database before the ingestion pattern is proven. In a production environment with a read-only source, independent ownership, or stricter availability requirements, place the same contract in a dedicated control store instead.

Primary keys and cursor keys are JSON so a single contract supports both one-column and composite keys while preserving their declared order. The initial incremental cursor is `1900-01-01T00:00:00` plus a zero-valued key object. This precedes the profiled AdventureWorks data and makes the first incremental execution a complete seed without a separate query shape.

The seed is idempotent: it updates the approved configuration, inserts missing rows, and never overwrites an existing watermark state. If a table already has a confirmed successful cursor, the seed fails rather than changing its load mode, ordered primary key, or watermark column. Such a change requires a reviewed migration that reconciles the existing cursor. ADF must treat `watermark_state` as durable success state—not as in-progress batch state.

## Approved configuration

| Source | Mode | Ordered primary key | Watermark |
|---|---|---|---|
| `Person.Person` | INCREMENTAL | `BusinessEntityID` | `ModifiedDate` |
| `Person.Address` | INCREMENTAL | `AddressID` | `ModifiedDate` |
| `Person.StateProvince` | FULL | `StateProvinceID` | — |
| `Person.CountryRegion` | FULL | `CountryRegionCode` | — |
| `Production.Product` | INCREMENTAL | `ProductID` | `ModifiedDate` |
| `Production.ProductCategory` | FULL | `ProductCategoryID` | — |
| `Production.ProductSubcategory` | FULL | `ProductSubcategoryID` | — |
| `Sales.Customer` | FULL | `CustomerID` | — |
| `Sales.SalesOrderHeader` | INCREMENTAL | `SalesOrderID` | `ModifiedDate` |
| `Sales.SalesOrderDetail` | INCREMENTAL | `SalesOrderID`, `SalesOrderDetailID` | `ModifiedDate` |
| `Sales.SalesTerritory` | FULL | `TerritoryID` | — |

## Future ADF metadata contract

The future parent pipeline will read active `table_config` rows using `ls_sql_server`, then dispatch one child execution per table. It will use the already published linked services `ls_sql_server`, `ls_key_vault`, and `ls_adls_gen2`; their definitions and credentials are outside this increment.

For INCREMENTAL sources, a pipeline must:

1. Read the committed lower cursor from `watermark_state`.
2. Capture a fixed source upper boundary before copying.
3. Extract rows ordered deterministically by `ModifiedDate` and the configured primary-key columns. Timestamp ties must be resolved lexicographically by the entire key; `ModifiedDate > last_watermark` alone is unsafe.
4. Land the batch in its deterministic run directory.
5. Validate copy success and source-to-target row-count reconciliation.
6. Only after all validation succeeds, update `watermark_value`, `watermark_key_json`, `last_successful_run_id`, `last_successful_at_utc`, and `last_successful_row_count` in one operation.

On copy, validation, or audit failure, the cursor must remain unchanged so the same boundary can be replayed. Concurrent executions for the same source must be prevented or use `row_version` for optimistic concurrency. Hard deletes are not represented by `ModifiedDate`; periodic key reconciliation remains the documented detection strategy.

FULL sources read the complete table on every run and do not update `watermark_state`.

## Minimum SQL permissions for future ADF pipelines

Use the dedicated database user `admm_ingestion` mapped to the SQL credential used by ADF; do not use `sa` for ADF. Apply these grants manually after creating the metadata tables. The existing source-table grants in `sql/security/create_ingestion_user.sql` remain unchanged. The metadata reader needs only object-level reads:

```sql
USE [AdventureWorks];
GRANT SELECT ON OBJECT::ingestion.table_config TO [admm_ingestion];
GRANT SELECT ON OBJECT::ingestion.watermark_state TO [admm_ingestion];
```

Do not grant `db_datawriter`, `db_owner`, `CONTROL`, or direct table-wide `UPDATE` merely to advance a cursor. When the success path is implemented, expose a narrowly scoped stored procedure that validates the expected `row_version`, checks the source is active and INCREMENTAL, validates the complete ordered PK object and the reconciled fixed upper boundary, prevents cursor regression, and atomically records the successful run and new cursor. Grant the pipeline identity only:

```sql
GRANT EXECUTE ON OBJECT::ingestion.<future_commit_watermark_procedure>
TO [<adf_watermark_committer>];
```

The procedure must accept the expected lower cursor and `row_version`, run ID, reconciled row count, and proposed upper cursor. It must fail on a stale version or conflicting replay, return success for an identical already-committed run, and use one transaction for the state update and success audit. Failed copy, reconciliation, or audit must leave state unchanged; an empty batch must never invent an upper cursor.

The procedure and grants are deliberately not created in this increment because the ADF success/reconciliation path does not exist yet. The account running these three deployment scripts needs DDL rights to create the `ingestion` schema and tables and DML rights to seed them; those elevated deployment rights must not be reused by ADF.

## Bronze file contract

The container is `bronze`. `bronze_relative_path` supplies the stable source/table prefix, and the future pipeline appends immutable run partitions:

```text
abfss://bronze@<storage-account>.dfs.core.windows.net/
  adventureworks/<schema-lower>/<table-lower>/
    load_type=<full|incremental>/
      ingestion_date=<YYYY-MM-DD>/
        run_id=<adf-pipeline-run-id>/
          part-*.parquet
```

Example:

```text
adventureworks/sales/salesorderdetail/load_type=incremental/
ingestion_date=2026-10-13/run_id=<run-id>/part-*.parquet
```

Each run directory is immutable. Retry behavior must either reuse and atomically replace the failed run directory or use a new run ID and mark the abandoned run in future audit metadata; it must never append ambiguously to a successful run. Bronze preserves source column values and should add `_ingested_at_utc`, `_pipeline_run_id`, `_source_system`, `_load_type`, `_watermark_lower`, and `_watermark_upper` as operational columns when the ADF copy design is implemented.

## Manual deployment

These scripts are not run automatically. With the local SQL Server container running, execute them in order from Git Bash at the repository root using an authorized deployment account. These are manual instructions, not commands executed by this PR:

```bash
set -euo pipefail
set -a
source .env
set +a
export SQLCMDPASSWORD="$MSSQL_SA_PASSWORD"

for script in \
  sql/metadata/001_create_ingestion_metadata.sql \
  sql/metadata/002_seed_ingestion_metadata.sql \
  sql/metadata/003_validate_ingestion_metadata.sql
do
  MSYS_NO_PATHCONV=1 docker exec -e SQLCMDPASSWORD -i adventureworks-sql \
    /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -d AdventureWorks -C -b \
    < "$script"
done

unset SQLCMDPASSWORD
```

`docker exec -e SQLCMDPASSWORD` forwards the already-exported environment variable by name; the password is not included in the `sqlcmd` arguments or printed. Do not enable shell tracing (`set -x`) while handling credentials. Alternatively, execute the files in the same order through SSMS or Azure Data Studio against the local `AdventureWorks` database. Review each script before execution. The first two scripts are safe to rerun: table creation is conditional, seeding is an upsert, existing cursor values are preserved, and incompatible contract changes after a confirmed cursor fail closed.

The validation script fails when the exact 11-table scope, modes, real ordered primary keys, non-null temporal `ModifiedDate` columns, cursor-key shape/types, initial/success state consistency, or state coverage do not match the contract. Its final result set is the deployment evidence to review. Do not grant future ADF metadata-update permissions until the pipeline's atomic success path is implemented.


## Verification scope

The scripts were recovered selectively from PR #14 (commit `e383a89f4eeca2e03fdf93b0689967dd06ab5130`), without its Phase 1/2 files. Static review additionally hardens seed transaction locking/rollback and validation of ordered PK arrays, cursor keys, and success state. No SQL was executed against AdventureWorks and no Azure resources or secrets were changed. Runtime validation remains a manual deployment step; run the validation script with a deployment account that can inspect source catalog metadata.
