# Bronze reconciliation and commit contract (work in progress)

## Current safety state
All eleven landing-only child executions have succeeded in ADF, but this is **not**
independent destination reconciliation. The parent intentionally terminates with
`WATERMARK_CONFIRMATION_BLOCKED`. Do not promote these files to Silver or advance
`ingestion.watermark_state` until the checks below are implemented and tested.

## Run identity and evidence
- ADF parent `@pipeline().RunId` is the correlation ID; the child run ID is the
  unique attempt ID and part of the immutable Bronze folder path.
- `PlanBatch` returns `expected_rows`, `watermark_lower`, `lower_key_json`,
  `watermark_upper`, `upper_key_json`, and `expected_row_version`.
- The SQL audit table `ingestion.bronze_run_audit` stores these values, the
  physical Bronze path, observed row counts, and processing status.
- Persist audit evidence using a **restricted writer identity**; the SQL source
  extraction identity must not gain blanket UPDATE rights to watermark_state.

## Required independent destination reconciliation
1. Read the *exact child run folder*, not a wildcard over historical attempts.
2. Verify one expected Parquet file is readable, with schema and required
   ingestion metadata columns; reject missing/unexpected files.
3. Count records by reading the destination Parquet (not Copy output counters).
4. Verify destination count equals the bounded `PlanBatch.expected_rows` and
   the observed Copy counters. For empty batches, verify the empty-file contract.
5. Record immutable evidence for the exact run ID, destination path, count,
   plan boundaries and verification result. Fail closed on any discrepancy.
6. For production correctness, consider source changes during extraction:
   source consistency/snapshot isolation or a reproducible bounded source
   view is needed beyond simple counts.

## Atomic watermark confirmation
- Implement a SQL stored procedure with transaction + `XACT_ABORT ON`.
- Validate audit row is `RECONCILED` for the same run and configuration;
  reject a replay from another attempt.
- Verify `watermark_state.row_version` matches the value captured by PlanBatch
  (optimistic concurrency).
- Validate lower cursor equals the currently committed cursor, and upper tuple
  is monotonic for incremental loads (timestamp, then full primary key tuple).
- Update watermark and `last_successful_*` together with audit status
  `COMMITTED` in **one transaction**, or roll back both.
- FULL loads have no incremental cursor; still require an audited commit.
- Never accept a caller-supplied boolean such as `reconciled=true` as proof
  of reconciliation without independently verifiable, durable evidence.
- Keep the intentional parent failure until all these gates are wired and
  exercised against failure, replay, and concurrent-run cases.

## Tests before enabling commits
- Successful FULL and INCREMENTAL; composite PK tie-breakers.
- Zero-row incremental, repeated execution, stale rowversion, duplicate run.
- Missing/truncated Parquet, mismatched counts, unexpected file/schema.
- Copy success but reconciliation failure: watermark remains unchanged.
- Commit transaction rollback on error and idempotent retry behavior.

## Deployment
`adf/sql/create_bronze_run_audit.sql` is the initial *schema only* migration.
It is not wired to ADF yet and must not be interpreted as completed auditing.
