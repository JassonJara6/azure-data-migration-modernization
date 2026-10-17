# Bronze security and confirmation contract

The authoritative flow, deployment order and R1/R2 acceptance are in [the rollout runbook](bronze-end-to-end-rollout.md). `WATERMARK_CONFIRMATION_BLOCKED` remains in ADF; no flag enables unverified confirmation.

## SQL identities

Example grants for **existing** correctly mapped database users, applied only after review/approval. This file does not create users, distribute credentials or execute grants.

```sql
-- Existing ADF reader already has SELECT on the eleven source objects.
GRANT SELECT ON OBJECT::ingestion.table_config TO [admm_ingestion];
GRANT SELECT ON OBJECT::ingestion.watermark_state TO [admm_ingestion];
GRANT EXECUTE ON OBJECT::ingestion.register_bronze_copy TO [admm_ingestion];

-- A separate trusted runtime; substitute its existing mapped user.
GRANT SELECT ON OBJECT::ingestion.table_config TO [bronze_reconciler];
GRANT SELECT ON OBJECT::ingestion.bronze_run_audit TO [bronze_reconciler];
GRANT SELECT ON OBJECT::ingestion.bronze_parent_audit TO [bronze_reconciler];
GRANT EXECUTE ON OBJECT::ingestion.attest_bronze_reconciliation TO [bronze_reconciler];
GRANT EXECUTE ON OBJECT::ingestion.commit_bronze_parent TO [bronze_reconciler];
```

Neither identity needs direct INSERT/UPDATE/DELETE on audit/watermark tables. Do not grant schema-wide EXECUTE, db_datawriter or db_owner. ADF must not receive attestation, child commit or parent commit permissions. Normal trusted execution needs no direct child-commit grant: same-owner static procedure calls use ownership chaining, covered by real SQL tests. The deployment identity alone needs DDL privileges and must not be reused as a runtime identity.

Audit/parent receipts are immutable through the runtime API. Registration snapshots the ordered PK and watermark-column contract. Exact registration/attestation/parent retries are idempotent; different receipts/manifests fail. Fresh parent confirmation rejects missing children, partial standalone commits, wrong states, changed contracts, stale versions, malformed/duplicate JSON keys, regressions and invented empty-window progress. The parent procedure rolls back **all eleven** when any child fails. Existing historical parent receipts can be replayed after subsequent runs without changing state.

## External identity and storage

The trusted runtime uses DefaultAzureCredential for ADLS read-only access and ADF run/activity read access. Bind it to the intended existing subscription, factory and storage account in protected runtime settings. No Azure write/publish permission is needed by the coordinator. SQL credentials come from a protected `BRONZE_SQL_CONNECTION_STRING`; never log that value or use `sqlcmd -P`.

The reconciler reads only the exact child directory in `bronze`, validates every file/row, rejects nested/unrelated objects, null/duplicate source PKs, mismatched parent/run/mode/boundaries and incomplete upper tuples. Seven-digit timestamps are compared as integer 100-ns values. Conditional downloads and stable listings bind evidence to file ETags; manifests include file hashes/sizes/row counts, cursor metadata, account/filesystem and the independent row count. Empty windows record an explicit empty file list. The manifest digest is SHA256 over its UTF-16LE JSON representation, matching SQL NVARCHAR HASHBYTES.

SQL verifies that evidence matches its immutable receipt, but cannot independently read ADLS. Protect the attestation identity accordingly. Files must remain immutable after copy; read-only reconciliation cannot impose a storage retention policy or prevent a future authorized writer from changing an object.
