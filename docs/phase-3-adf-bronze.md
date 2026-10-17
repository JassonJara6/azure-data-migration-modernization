# Phase 3: dynamic ADF Bronze landing (unpublished prototype)

> For the current Bronze audit/reconciliation/parent-confirmation implementation and deployment/R1-R2 checklist, use [the controlled rollout runbook](../adf/docs/bronze-end-to-end-rollout.md). This original document describes the landing foundation; ADF’s confirmation block is still retained.

This increment supplies ADF authoring JSON for all 11 approved AdventureWorks sources: five INCREMENTAL and six FULL. It reuses `ls_sql_server` and `ls_adls_gen2`; the existing SQL linked service continues to use its published Key Vault configuration (`ls_key_vault`). No linked service, SHIR, credential, trigger, Azure resource, metadata table, or permission is created or changed here.

**Status: landing only, NOT committed ingestion.** No artifact updates `ingestion.watermark_state`. The parent deliberately ends in `WATERMARK_CONFIRMATION_BLOCKED` after successful table copies. Destination reconciliation, durable audit and atomic confirmation are not implemented. Do not promote these directories to Silver or treat the run as production success. This is an explicit safety boundary, not a transient error to suppress.

## Components and flow

| Artifact | Responsibility |
|---|---|
| `adf/pipeline/pl_adventureworks_bronze.json` | Acknowledgement gate, Lookup of active configurations, sequential ForEach, synchronous child call, terminal confirmation block |
| `adf/pipeline/pl_adventureworks_bronze_table.json` | Per-source gate, batch planning Lookup, bounded Copy to Parquet, comparison of expected/read/copied counters |
| `adf/dataset/ds_adventureworks_sql.json` | Parameterized SQL Server schema/table, existing `ls_sql_server` |
| `adf/dataset/ds_bronze_parquet.json` | Parameterized folder, fixed `bronze` filesystem, Snappy Parquet, existing `ls_adls_gen2` |
| `adf/sql/plan_bronze_batch.sql` | Read-only SQL Lookup template: validates metadata, captures a boundary, counts rows, returns the extraction query |
| `adf/build_artifacts.py` | Deterministic local JSON generator; embeds the SQL template without deploying it |
| `tests/phase3/test_adf_bronze.py`, `tests/phase3/adf_expression.py` | Offline expression/control-flow evaluation, result budgets, query and cursor regression checks |

```text
Acknowledge landing-only mode
  -> Lookup ingestion.table_config WHERE is_active = 1
  -> ValidateActiveConfigurations (one to eleven rows, complete array)
  -> sequential ForEach config_id
       -> ExecutePipeline, waitOnCompletion = true
            -> acknowledge -> PlanBatch -> ValidatePlan -> CopyToBronze -> CheckCopyCounters
  -> Fail WATERMARK_CONFIRMATION_BLOCKED
```

Both pipelines have concurrency 1. The parent passes only a typed integer `config_id`, its run ID, acknowledgement, and a UTC ingestion date computed once from its trigger time. Each child owns its plan; there are no shared mutable pipeline variables inside ForEach. Child-only String variables hold constant SQL fragments; no SetVariable/AppendVariable activities alter them and callers cannot override them as parameters. The planner rereads the active configuration and committed cursor, checks the approved source/mode/PK/path, and verifies the real PK against SQL catalog metadata. Unknown sources, modes, cursor shapes, incompatible PKs and missing state fail before that table's copy. Disabled sources are omitted; with the supplied seed all eleven are active. There are no schedules or event triggers.

`plan_bronze_batch.sql` is **not** a script to run or a stored procedure to install. Its configuration-ID token is converted to an ADF expression by the generator. Lookup executes a batch containing declarations and SELECTs, with read-only dynamic SQL through `sys.sp_executesql`; it returns exactly one result row with thirteen named scalar fields, including `plan_contract_version=1`. Rowversion is hex text, not a Lookup-unsupported byte array. Copy receives a single SELECT, without USE. Parent Lookup and the planner fail unless `DB_NAME()` is `AdventureWorks`; the published linked service must already select that database. The only interpolated pipeline value is a typed Int, explicitly converted at the parent-to-child boundary and again before SQL concatenation. Source identifiers must match the approved registry and are wrapped with `QUOTENAME`; cursor values are converted to temporal/integer types before becoming SQL literals. No arbitrary query or table-name pipeline parameter is accepted.

## FULL and INCREMENTAL semantics

FULL reads the complete source, ordered by its complete primary key, with no cursor state. Its expected source count is taken during planning.

INCREMENTAL uses `ModifiedDate` and the configured ordered integer PK. `Sales.SalesOrderDetail` uses both `SalesOrderID` and `SalesOrderDetailID`. The planner captures the **actual highest row tuple** above the committed lower tuple using `TOP (1) ... ORDER BY ModifiedDate DESC, PK1 DESC, PK2 DESC`. It never combines independent MAX values into an artificial row.

For the composite case, Copy uses this closed upper / open lower window. The first PK component in the upper tie-break must be strictly `<`; `<=` there would incorrectly admit `(upper_PK1, PK2_above_upper)`:

```text
(ModifiedDate > lower_date OR
 (ModifiedDate = lower_date AND
  (PK1 > lower_pk1 OR (PK1 = lower_pk1 AND PK2 > lower_pk2))))
AND
(ModifiedDate < upper_date OR
 (ModifiedDate = upper_date AND
  (PK1 < upper_pk1 OR (PK1 = upper_pk1 AND PK2 <= upper_pk2))))
ORDER BY ModifiedDate, PK1, PK2
```

The one-key variant uses the same date comparison with a single PK. The planner preserves SQL `datetime2(7)` precision in string literals, avoiding ADF/JavaScript timestamp rounding. It returns the lower cursor, proposed upper cursor, and expected `row_version` as evidence for a future confirmation path. Only the current five integer-key incremental sources are supported; the FULL `CountryRegionCode` string key is ordered without converting it to an integer.

The expected count and Copy query use exactly the same bounded predicate. `CheckCopyCounters` requires both Copy `rowsRead` and `rowsCopied` to equal that count. This is **a preliminary counter check**, not independent verification of the landed Parquet contents. No bad rows may be skipped and truncation is disabled. An empty incremental window retains the original lower tuple as its proposed upper tuple, reads zero rows and never fabricates a cursor. ADF may omit an empty Parquet file; validate that behavior before use.

Planning, counting and Copy use separate SQL reads. A fixed tuple boundary is **not a source snapshot**. Concurrent inserts, updates or deletes can change results, including changes with the same count. Only use a quiescent sample for the first manual validation. A future production path must establish stable extraction semantics (snapshot/staging or a source change mechanism), handle late/backdated updates and define overlap/deduplication. Ordered timestamp ties alone cannot solve those problems. Hard deletes still require periodic key reconciliation or a proper change feed.

## Bronze output and data representations

The existing Bronze contract is preserved:

```text
bronze/adventureworks/<schema-lower>/<table-lower>/
  load_type=<full|incremental>/ingestion_date=<UTC YYYY-MM-DD>/
  run_id=<child-pipeline-run-id>/part-00000.parquet
```

The child RunId is a real ADF pipeline run ID and isolates every table execution; `_parent_pipeline_run_id` links the eleven children to the orchestration run. All children inherit the parent's UTC date. Copy runs with one stream, no partitioned source, no staging and no automatic retries. One named Parquet file suits this small portfolio dataset; larger sources will need partitioned-file design and validation.

Operational columns are `_ingested_at_utc`, `_pipeline_run_id`, `_parent_pipeline_run_id`, `_source_system`, `_load_type`, `_watermark_lower`, `_watermark_upper`, `_watermark_lower_key_json`, and `_watermark_upper_key_json`. FULL boundary fields are empty strings. Nonempty files retain complete lower and upper PK tuples alongside their timestamps. `PlanBatch` additionally captures `expected_row_version`; a durable batch manifest/audit, including evidence for empty windows, remains required before confirmation.

## ADF result and expression budgets

The [Lookup contract](https://learn.microsoft.com/en-us/azure/data-factory/control-flow-lookup-activity) requires exactly one result set, limits output to 4 MB / 5,000 rows, and distinguishes `output.value` arrays from `output.firstRow`. The parent queries only integer config IDs, rejects more than eleven active rows in SQL, and selects at most twelve (the twelfth detects a concurrent scope increase). `ValidateActiveConfigurations` rejects zero/twelve rows or a count/array mismatch before ForEach. No large table data is returned through Lookup.

`PlanBatch` has `firstRowOnly=true`; all intermediate SELECTs assign variables, including the two dynamic SQL calls, and only the final SELECT emits a result set. Its extraction query is limited to 65,536 SQL NVARCHAR bytes (32,768 UTF-16 units) **before** Lookup returns it. Even worst-case six-byte JSON escaping plus metadata remains well below 4 MB. `ValidatePlan` checks required fields, version, config ID, mode, nonnegative expected row count and bounded query length before Copy. Missing fields/type-conversion errors also fail before Copy. FULL cursor fields are empty strings rather than nulls.

The original inline PlanBatch expression was 10,038 characters, exceeding ADF's [8,192-character expression limit](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/azure-subscription-service-limits#azure-data-factory-limits). The generator now stores SQL as internal constant String fragments of at most 3,500 characters and assembles them with a short expression. It preserves the template exactly and enforces expression/object budgets before writing JSON. SQL is not accepted as an overridable pipeline parameter.

All conditions return Bool, not string interpolation. Empty success branches are deliberate no-ops; a selected false branch executes Fail with constant, nonempty message/errorCode. Following activities depend only on Succeeded, so failed acknowledgements, malformed plans and mismatched counters cannot reach Copy or bypass the final confirmation block. Counter checks require both numeric Copy counters; missing counters fail rather than being treated as zero. This follows the documented [IfCondition](https://learn.microsoft.com/en-us/azure/data-factory/control-flow-if-condition-activity) / [Fail](https://learn.microsoft.com/en-us/azure/data-factory/control-flow-fail-activity) model; service execution remains to be validated.

Source columns keep their names. SQL XML is serialized to Unicode XML text, `hierarchyid` to its canonical string, and `geography`/`geometry` to native serialized binary (including SRID), because these SQL Server types are not directly portable to Parquet. Unknown CLR types fail closed. These are explicit transport representations, not business cleansing; downstream consumers must decode them appropriately. Review the exact XML/spatial behavior and decimal/date/Unicode mappings in ADF. Parquet does not promise physical row order, even though extraction uses a deterministic ORDER BY.

Every run directory is uncommitted and immutable from the consumer's perspective. On failure, retain it as abandoned evidence. **Do not rerun a failed Copy activity or resume a child against the same RunId:** a named file may be overwritten after partial failure. Start an entirely new parent run so new child IDs produce new directories. Durable abandoned-run registration and replay of identical persisted boundaries are not implemented yet. Until cursors can be safely confirmed, reruns reread the unchanged lower cursor and can duplicate data across different run directories; consumers must not union those runs as committed batches.

## Permissions and manual ADF validation checklist

No deployment or publishing is performed by this PR or by the build/test commands. Before a later authorized import/debug session:

1. Import the two datasets and two pipelines as authoring artifacts in the **existing** factory. Do not create linked services, overwrite their credentials, or publish automatically. Validate All in ADF, including the embedded Lookup SQL batch and every dynamic expression.
2. Confirm `ls_sql_server` targets AdventureWorks through the configured SHIR and `ls_adls_gen2` uses the existing Bronze-authorized identity. Verify SHIR connectivity, installed drivers/Java as required by its Parquet support, and ADLS access. Key Vault resolution remains the responsibility of the existing linked services; no direct Key Vault activity is added.
3. The SQL identity needs the existing object-level SELECT grants on the eleven sources and SELECT on `ingestion.table_config` and `ingestion.watermark_state` as documented in [the metadata runbook](phase-3-ingestion-metadata.md). It needs catalog visibility for objects it reads. Do not use `sa`, grant UPDATE, or add `db_owner`/`db_datawriter`. No SQL DDL is required for these artifacts. Confirm that the published SQL linked service selects **AdventureWorks** explicitly; these artifacts fail closed on a different database and never switch it with USE.
4. Verify the seed/validation evidence already exists, all eleven configurations are active, and the source is quiescent. Never run metadata seed/deployment SQL automatically from a pipeline.
5. For a deliberate later pilot, set parent `acknowledge_landing_only=true` (default false). Both pipelines reject unacknowledged execution. Expect eleven child plans/copies and then the intentional parent failure. Copy or count failures fail earlier and also leave cursors unchanged.
6. Independently inspect the Parquet files, exact schemas and transport representations; count rows in ADLS, compare key sets/row counts to the bounded source, and test initial, empty, single-key tie and composite-key tie windows, FULL string keys, source mutations, connection failures and partial copy failure. Verify no watermark values, versions or success fields changed. Repeat with new runs and inspect isolation of paths.

Lookup/Copy use secure input/output policies to reduce exposure of cursor/query data. Activity outputs remain available to dependent expressions but are masked in monitoring. A future protected durable audit is required; do not disable masking merely to log raw customer data. No password or secret is embedded in these files.

## What still blocks safe incremental ingestion

- Independent landed-file reconciliation (including zero rows), data-quality checks and durable success/failure manifests with exact immutable boundaries and destination file evidence.
- A narrowly authorized stored procedure that validates expected `row_version` and lower cursor, checks the reconciled upper tuple, prevents regression/conflicting replays, and atomically records success audit plus watermark advancement. Only EXECUTE on that procedure, never broad table write permission.
- Stable source-window semantics, late-change handling, hard-delete reconciliation, concurrency protection across all callers, and replay/idempotency rules backed by durable state.
- ADF runtime validation of JSON, expressions, linked-service references, SHIR/Parquet support, conversions, empty windows and failure paths.

Do not remove the terminal Fail or the acknowledgement gate until these controls are implemented and tested together. There is no switch that enables watermark writes in this version.

## Offline validation

From the repository root (Python standard library only):

```bash
python adf/build_artifacts.py --check
python -m unittest discover -s tests/phase3 -p 'test_*.py' -v
git diff --check
```

To intentionally regenerate authoring JSON after editing the generator or SQL template, run `python adf/build_artifacts.py` locally, then rerun the checks. These commands neither connect to AdventureWorks nor contact Azure. Nineteen tests currently cover typed parameter forwarding, every generated expression evaluated against mocked Lookup/Copy outputs, branch failure propagation, malformed/oversized outputs and expression-budget enforcement. Actual SQL concatenation fragments are interpreted and exercised against synthetic SQLite rows: equal timestamps, lower/upper equality, cross-key order, seven-decimal timestamps, empty windows, real maximal tuple capture, and identical COUNT/Copy windows. Mutation regressions verify that making the first upper PK comparison inclusive, excluding exact upper equality, or comparing PK components independently is detected.

The offline evaluator deliberately implements only the expression subset used here. SQLite adapters remove SQL Server-only assignment/TOP/COUNT_BIG syntax for the synthetic tests. Neither mechanism compiles T-SQL, validates the live linked-service database, or proves ADF execution. ADF Validate All and an authorized later SHIR/Lookup/Copy pilot must still confirm batch support, result shapes/types, expressions, empty branches, Fail propagation, Parquet conversions and failure behavior. Watermark confirmation, source snapshot semantics, durable audit and destination reconciliation remain blocked/pending.
