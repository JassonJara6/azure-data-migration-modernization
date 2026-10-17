# Bronze end-to-end rollout (controlled release)

## Implemented in this PR
- SQL audit table and optimistic commit procedure
- `ingestion.register_bronze_copy`: immutable COPIED audit after ADF Copy counter validation
- `ingestion.attest_bronze_reconciliation`: privileged COPIED -> RECONCILED transition
- `adf/tools/commit_reconciled_bronze.py`: independent ADLS Parquet verification, SQL attestation, SQL commit
- Existing `reconcile_bronze.py` validates actual Parquet rows and child run metadata

## Deployment (do not skip)
1. Review and test the SQL procedures against the actual audit table schema.
2. Deploy `register_bronze_copy.sql` and `attest_bronze_reconciliation.sql` to AdventureWorks. The audit table and commit procedure were deployed previously.
3. Review and publish the updated child pipeline `pl_adventureworks_bronze_table` to ADF. The GitHub change does NOT publish to Azure automatically.
4. Ensure ADF SQL identity can EXECUTE **only** `register_bronze_copy` (not `attest_bronze_reconciliation`, not `commit_bronze_run`, not direct audit/watermark UPDATE).
5. Deploy the reconciler on a separate trusted runtime with ADLS read-only access, ODBC Driver 18, `pip install -r adf/tools/requirements-commit.txt`, and `BRONZE_SQL_CONNECTION_STRING` provided securely. Grant that identity EXECUTE on attestation and commit only, not direct watermark UPDATE.
6. Run one controlled child pipeline; inspect the COPIED audit row and confirm watermark unchanged.
7. From the trusted reconciler, run:
   ```bash
   python adf/tools/commit_reconciled_bronze.py --account <storage-account> --filesystem bronze --run-id <child-run-uuid>
   ```
8. Verify COMMITTED audit status, incremental cursor advance, and replay/duplicate rejection. Test failures and empty batches before production.
9. Only after integration tests and deployment, decide whether to replace the parent's terminal `WATERMARK_CONFIRMATION_BLOCKED` with an orchestrated success gate. **The parent still intentionally fails**. Do not remove it yet.

## Important boundaries
- This PR is a staged integration, **not** evidence of a successful Azure end-to-end test.
- The runner uses ODBC SQL credentials and ADLS identity that must be configured outside the repository.
- ADF Script activity support and expression syntax require validation in the target ADF instance before publishing.
- Destination reconciliation validates run ID, source system and counts, but richer column-level invariants and completeness checks may be appropriate for production.
- ADF's copy counter check alone is not reconciliation.
- SQL attestation is a privileged assertion: protect the credential, because SQL cannot itself verify ADLS files.
- Never put SQL passwords in source control.
