# Local SQL export validation scope

**Real AdventureWorks acceptance: NOT EXECUTED in this environment.**

The user reports a healthy `adventureworks-sql` container on their Windows machine,
AdventureWorks restored, Product=504 and SalesOrderDetail=121317. These are user
observations and default exporter expectations, NOT counts measured by Codex.

Here, loopback TCP `127.0.0.1:1433` returns ConnectionRefusedError, Docker daemon
socket access is denied, pyodbc/SQL Server ODBC are not installed and no source
credentials are attached. We did not ask for secrets, create another database,
start containers, access Azure or simulate real acceptance.

## What is implemented

The [single operator procedure](../../../local/SQL-SERVER.md) uses a real pyodbc
connection in the operator environment, verifies the existing eleven-table
schema/PK contract and source counts in one SERIALIZABLE read transaction, writes
typed Parquet parts and a neutral LOCAL_EXPORTED_VERIFIED manifest, closes SQL
with ROLLBACK and calls the existing local Silver/Gold runner with an explicit
local-authority adapter. No Azure receipt or remote COMMITTED status is produced.

The final report will be `<chosen-root>/sql-acceptance-<export-id>.json` (the Git Bash procedure uses `~/admm-real`).
Only actual completion on that machine can establish the expected eleven Silver
and seven Gold tables against SQL data. It includes all source/target/quality and
amount checks and the unchanged 22 Delta versions on replay. Generated real
reports, raw data, manifests and credentials are not committed by this PR.

## Automated test evidence

| Validation | Result | Scope |
| --- | --- | --- |
| SQL export/CLI regressions | **26 PASS** | strict mock connection/cursors; real typed Arrow/Parquet |
| Gold + SQL export full suite | **42/42 PASS**, 492.70 s | 16 existing Gold + 26 new export/CLI cases; [JUnit](gold-and-mock-sql-junit.xml) |
| Existing automation | **439/439 PASS**, 16.46 s | offline CLI/provider/wheel checks, no Azure |
| Follow-up SQL diagnostics | **25/25 PASS**, 0.54 s | accurate expected-count stage/table; Spark case deselected in this focused rerun |
| Silver full suite | **72/72 PASS**, 323.83 s | existing Spark/Delta transformations and contracts |
| SQL mock → Bronze → Silver → Gold → replay | **PASS** | real Spark/Delta; 11 Silver, 7 Gold, all 22 histories unchanged |
| Direct SQL Server / ODBC native / real 504 and 121317 counts | **NOT VERIFIED** | unavailable from this environment |
| Windows end-to-end | **NOT VERIFIED** | operator must run the single procedure |
| Azure / cloud execution | **NOT EXECUTED** | explicitly excluded |

The mock end-to-end case uses 25 synthetic rows (3 products and 5 detail rows),
not 504/121317. Actual Parquet SHA-256s, footer counts, 7-digit timestamp precision,
opaque geography bytes and local provenance are verified; the real source
baseline intentionally rejects that mock dataset unless test-specific expectations
are supplied. The SQL mock rejects arbitrary/unrecognized statements, DML/DDL,
commit calls and committing connection context managers. It does NOT validate
T-SQL execution against a live server.

Negative cases cover wrong database/cloud engine edition, source schema/types/PK,
short reads, expected-count mismatch, endpoint/property injection, changed files,
extra parts, modified checksums/provenance, SQLSTATE redaction, delimiter-containing
credentials, unsafe password prompts, fixture/SQL lake mixing and replay without
reconnecting or requiring a password. The full Gold/SQL suite had no failures or skips. The focused follow-up
reran all 25 non-Spark cases after an error-stage annotation fix; the already-passed
Spark case was explicitly deselected for that diagnostic-only check. Only JUnit hostname was removed; results/timings are unmodified.

Runtime: Python 3.11.17, Java 17.0.17, Spark 3.5.3, Delta Lake 3.2.1,
PyArrow 19.0.1 on Linux. SQL responses are mocked; no real acceptance report or
manifest is checked in. Existing PR #43 synthetic evidence remains unchanged.

## What still requires the operator's one command

- Live ODBC 18 login/TLS and SQL Server projection/metadata behavior, including XML,
  datetime2 conversion and geography serialization on the restored database.
- Observed 504/121317 source baselines, all eleven source counts and actual data
  quality (rather than assuming the mocks prove them).
- Eleven Silver/seven Gold results with real AdventureWorks monetary/FK consistency.
- Replay of that real export and its Delta histories, and native Windows Spark IO.

Azure, Databricks cloud, real Bronze watermarks and remote SQL receipts are outside
this local route and are never modified or certified. The SQL manifest checksum
is local integrity evidence, not a signed server attestation.

Static checks: Python compile/AST, documented Bash syntax, Markdown links, staged
output scope and JUnit secret-pattern checks passed. Existing automation ran with
real local CLI schema/loopback/provider-auth/wheel tests enabled; none accessed
Azure or the user's SQL Server. Business transformations and the PR #43 fixture
evidence are unchanged.
