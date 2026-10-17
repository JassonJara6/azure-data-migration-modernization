# Bronze completion and controlled rollout

Scope: **Bronze only** (Fase 2 in the current delivery request). Existing repository paths retain their historical `phase3` names. No Databricks, Silver, Gold, Terraform or Azure resource creation is included. The older landing-only runbook is background; this document governs reconciliation and confirmation.

Para la ejecución ya realizada y el diagnóstico Windows/Git Bash, consulte [el cierre controlado](bronze-windows-closeout.md). La reconciliación real de R1 y la prueba R2 en Azure siguen pendientes de evidencia.

## Implemented flow

1. Existing ADF parent performs Lookup / sequential ForEach for the active source configurations. Each child plans a fixed window, copies Parquet and checks source/read/copied counts.
2. `RegisterCopyAudit` invokes `ingestion.register_bronze_copy` with **bound Stored Procedure parameters**, not concatenated Script SQL. It persists an immutable COPIED receipt with source-contract snapshot and complete lower/upper cursor. Exact retries are no-ops; conflicting retries fail. Copy still has no automatic retries.
3. Parent stops at the retained `WATERMARK_CONFIRMATION_BLOCKED` Fail. This is expected and must not be removed as part of this release. The coordinator accepts only that terminal failure, successful parent gates and exactly eleven successful child RunIds matching the SQL receipts. Other failures, running/cancelled activities, missing children or partial prior commits are rejected.
4. On a separate trusted runtime, `commit_bronze_parent.py` reads every exact child folder from ADLS. It scans actual Parquet rows, checks metadata, counts, non-null/unique complete PKs, tuple bounds and the captured upper tuple. It captures file sizes, ETags and SHA256; conditional downloads and two listings detect changes while reading. Empty incremental windows may have no file/directory, but must have identical lower/upper tuples and zero expected/copied rows.
5. The trusted worker attests each result through `ingestion.attest_bronze_reconciliation`, persisting a hashed manifest and writer/time. SQL validates the manifest's identity, path, counts, boundaries, storage and file evidence. A reconciled status alone is not a watermark commit.
6. After all eleven attestations and a final unchanged-ETag check, `ingestion.commit_bronze_parent` confirms all eleven in **one transaction**. It invokes the existing guarded child procedure, compares source contract, expected rowversion and lower cursor, validates full JSON key sets and monotonic tuple progress, then atomically writes watermarks and child/parent receipts. A stale last child rolls back all updates. FULL never creates a watermark. Empty incrementals record success/count zero without moving the cursor.
7. Retrying the same committed parent returns its durable receipt, including after newer parents have advanced. It does not rewrite state, recopy files or require a successful Azure read. A lost attestation acknowledgement can be retried with the same manifest; different evidence is rejected.

`commit_reconciled_bronze.py` remains the single-child diagnostic/attestation entry point. Its CLI does not commit a parent or advance a cursor. Normal deployment grants commit permission only on the parent procedure. Direct child commits must not be mixed with parent confirmation.

## What was verified locally

- 20 offline ADF artifact/expression tests, including bound receipt parameters and FULL null values.
- 63 real-Parquet, safe-diagnostic, connection and mocked SQL/ADF coordination tests, including bad metadata, wrong IDs/mode, count mismatch, duplicate/null PKs, out-of-window rows, absent upper tuple, 100-ns timestamps, empty windows, changed ETags and parent rejection/idempotency.
- 18 tests on **SQL Server 2022 Developer in a disposable isolated Docker container**: procedure deployment/redeployment, exact/conflicting retries, two complete consecutive parents, empty increments, FULL, composite keys, stale-version rollback, malformed evidence, least privilege and a trusted EXECUTE-only parent call. Real pyodbc/ODBC Driver 18 tests additionally verify special-character password authentication, safe login failures, bound manifest attestation, database selection, deferred SQL errors and atomic parent replay.
- The real SQL planner was executed against synthetic source tables. Its thirteen-column single-row result, hexadecimal rowversion and generated Copy SELECT were verified. Copy projects incremental ModifiedDate as datetime2(7), matching the boundary conversion even when the source uses SQL datetime. Rows inserted after planning above the complete upper tuple were excluded.

These are synthetic/local tests, **not** evidence of an Azure/ADLS end-to-end run or a test against the user's AdventureWorks instance. No Azure changes or merge were performed. The confirmation block remains until the manual acceptance below is approved and passed. A successful external SQL receipt does not retroactively change ADF's deliberately Failed status.

## Deployment instructions (approval required before execution)

1. Back up the existing metadata/audit schema and record current watermarks. Freeze the sample AdventureWorks source for this MVP validation. Independently review the eleven-source contract and `sql/metadata/003_validate_ingestion_metadata.sql` output; do not reseed or reset live cursors as part of deployment.
2. With an authorized deployment identity, execute the following in order against **AdventureWorks**, with fail-on-error. The table upgrade is additive and preserves existing rows. Legacy receipts lacking snapshots/manifests cannot be confirmed automatically. Duplicate legacy `(parent_run_id, config_id)` rows stop the unique-index upgrade; inspect and resolve them explicitly, never delete them automatically.

```bash
# Supply SQLCMDPASSWORD securely; never use sqlcmd -P or shell tracing.
for script in \
  adf/sql/preflight_bronze_commit.sql \
  adf/sql/create_bronze_run_audit.sql \
  adf/sql/register_bronze_copy.sql \
  adf/sql/attest_bronze_reconciliation.sql \
  adf/sql/commit_bronze_run.sql \
  adf/sql/commit_bronze_parent.sql
do
  sqlcmd -S "$BRONZE_SQL_SERVER" -U "$BRONZE_DEPLOY_USER" \
    -d AdventureWorks -b -i "$script" || exit 1
done
```

Use the existing trusted TLS configuration. If invoking sqlcmd inside Docker, forward the environment by name (`docker exec -e SQLCMDPASSWORD -i ...`), never put the password in process arguments. No credentials are added by these scripts.

3. Apply the object-level grants in [the security contract](bronze-reconciliation-commit-contract.md). The published ADF SQL user can register copies only; a **different** SQL identity can read receipts/configuration and execute attestation/parent confirmation. Neither receives direct table writes. Do not reuse `sa` or grant db_owner/db_datawriter.
4. On the trusted runtime, install `python -m pip install -r adf/tools/requirements-commit.txt`, ODBC Driver 18, and configure the protected `BRONZE_SQL_CONNECTION_STRING`. Configure a read-only ADLS identity scoped to the existing Bronze container and read-only ADF management access scoped to the existing factory. Pin the intended account/factory settings in that runtime; they are a security binding, not values to obtain from untrusted callers.
5. Validate the generated JSON with `python adf/build_artifacts.py --check`. After approval only, import/Validate All and publish the existing two pipelines/datasets in ADF. Reuse `ls_sql_server`, `ls_adls_gen2`, `ls_key_vault` and SHIR; do not create resources or change credentials. Verify the Stored Procedure activity's null/String/Int mappings and the SQL connection's AdventureWorks database. Preserve secure input/output settings.

## Two consecutive approved acceptance runs

**R1**

1. Execute `pl_adventureworks_bronze` once with `acknowledge_landing_only=true`; record its parent RunId. Wait until all eleven children succeed and the parent fails solely at `WATERMARK_CONFIRMATION_BLOCKED`. Check eleven COPIED receipts and unchanged watermarks. Any other failure stops acceptance; start a new parent after fixing the cause, never rerun Copy into an old run folder.
2. Run on the trusted runtime (this command reads Azure but writes SQL audit/watermarks):

```bash
python adf/tools/commit_bronze_parent.py \
  --account "$BRONZE_ADLS_ACCOUNT" --filesystem bronze \
  --subscription "$AZURE_SUBSCRIPTION_ID" --resource-group "$ADF_RESOURCE_GROUP" \
  --factory "$ADF_FACTORY" --parent-run-id "$PARENT_RUN_ID"
```

3. Expect COMMITTED, a durable parent receipt with eleven committed children, six FULL receipts and five incremental receipts. For nonempty incremental sources, upper tuples must equal the landed maximal tuples. Review the stored manifests and independently read/count the Parquet files. Repeat the coordinator with the **same** parent ID; expect `replayed=true`, with byte-for-byte unchanged watermark rows/rowversions.

**R2 (source unchanged)**

4. Start an entirely new ADF parent, with new child RunIds and folders. The five incremental plans must use R1's committed lower tuples and return zero rows; the six FULL tables reload completely. Parent still stops at the expected block.
5. Confirm the second parent with the same coordinator. Expect eleven new COMMITTED receipts and a second parent receipt. Incremental timestamps/PK tuples stay unchanged; last-success IDs/counts update to the second run with count zero. Their rowversions change once because success metadata is updated. FULL still has no watermark row. Retrying R2 changes nothing.
6. For a separate failure run, corrupt a test-only destination or introduce a stale cursor. Confirm that no parent receipt is created and no watermark in that parent advances. Earlier RECONCILED rows may remain as durable evidence; they are uncommitted and can be retried only with unchanged files and matching versions. Never repair production state by resetting cursors.

**Acceptance to close Bronze:** retain R1/R2 outputs, manifests, before/after SQL evidence, least-privilege checks and the failure test. Only after these pass may an explicitly approved follow-up replace the retained ADF Fail with a verified orchestration success gate. This PR does not bypass it or create a network service/linked service to invoke the external coordinator.

## Remaining operational limits

Fixed bounds are not a source snapshot: planning/counting/Copy are separate reads. This MVP requires the source to remain quiescent through landing and confirmation. Concurrent changes with identical counts can evade row-count checks. A mutable production source requires snapshot/staging or a change-feed strategy and delete/late-change handling; no such architecture change is introduced here.

ADLS files must remain immutable after the terminal child run. ETag checks detect changes during reading and just before confirmation, not arbitrary future writes by an authorized storage writer. Do not resume old Copy activities; use new run folders, protect the writer credentials and review abandoned runs. The verifier currently holds file bytes and a PK set in memory, appropriate for the portfolio sample; larger workloads need bounded-memory reconciliation.

SQL cannot prove an external identity actually read ADLS. Attestation authority is therefore a separate protected credential; the manifest is durable evidence, not a substitute for that trust boundary.

## Reproduce tests (no Azure)

```bash
python -m pip install pytest -r adf/tools/requirements-commit.txt
python adf/build_artifacts.py --check
python -m unittest discover -s tests/phase3 -p 'test_*.py' -v
python -m pytest -q adf/tests
# Requires Docker and 3 GB RAM. Creates/disposes its own SQL Server; never targets an existing instance.
python adf/tests/run_sql_integration.py
```

The normal pytest run skips SQL integration when the isolated-container variables are absent. CI runs both offline and real SQL jobs. CI does not deploy SQL to an existing server or access Azure.
