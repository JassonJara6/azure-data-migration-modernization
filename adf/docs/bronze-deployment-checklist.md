# Bronze deployment checklist (not yet authorized)

The code in PR #17 is a development draft, **not a deployable complete
solution**. Never remove the published ADF fail gate yet.

## Preconditions
- CI reconciliation unit tests pass; validate SQL DDL and procedure on a
  disposable AdventureWorks database, including rollback and concurrency.
- Confirm the live `ingestion.watermark_state` column names, rowversion type,
  foreign keys and constraints before executing migrations.
- Implement a restricted trusted evidence writer that performs destination
  reconciliation itself; a caller must not be able to forge RECONCILED status.
- Persist audit PLANNED -> COPIED -> RECONCILED before commit, with unique
  run identity, exact Bronze path, plan cursor and Copy metrics.
- Protect audit rows against post-commit edits; configure least privilege.
- Wire ADF to the reconciler and SQL commit with retry/failed-run handling.
- Test empty-batch semantics and immutable Bronze destination path.
- Confirm no source changes escape the bounded extraction snapshot contract.

## Deployment order
1. Review and run audit DDL in AdventureWorks.
2. Deploy tested SQL commit procedure with no broad public execute grants.
3. Provision trusted reconciliation compute/identity and audit permissions.
4. Deploy revised child and parent pipelines in ADF Studio.
5. Run a single controlled FULL and INCREMENTAL test, including forced failures.
6. Check independent Parquet counts, audit rows and exact cursor advancement.
7. Only after all checks pass, remove intentional terminal failure and run all
   eleven tables again.

## Current published state
Landing-only ADF pipelines have copied all 11 tables, but watermarks remain
uncommitted. These earlier runs cannot be retroactively promoted based solely
on Copy activity success.
