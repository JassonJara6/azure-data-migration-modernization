# AdventureWorks SQL local → Bronze Parquet → Silver → Gold

This route uses the **existing** SQL Server on `127.0.0.1:1433`, database
`AdventureWorks`. It does not start Docker, restore a database, create logins,
change permissions, read/write Bronze SQL watermarks, contact Azure or run cloud
Jobs. The existing eleven-table contract, five INCREMENTAL/six FULL modes, Silver
transformations and seven Gold tables are reused.

## One procedure for Windows / Git Bash

Prerequisites: the environment that can run the PR #43 fixtures, Python **3.11**,
Java **17**, ODBC Driver **18 for SQL Server**, and an existing SQL login with
SELECT on the eleven tables. `admm_ingestion` is the repository's source reader;
set `ADMM_SQL_USER` to another existing authorized login if needed. Metadata
visibility on those objects is needed to check columns and primary keys. No
permission is granted by this procedure.

From the updated repository root, paste this **single block**. No password goes
into shell history or command arguments. An existing `ADMM_SQL_PASSWORD` is used;
otherwise the program prompts with echo disabled. Special characters such as
`!`, `$`, `;`, `{` and `}` are not interpreted by a shell. `winpty` is used when
available so native Windows Python has a hidden-input console.

```bash
(
set -euo pipefail
export PYTHONUTF8=1 PYTHONIOENCODING=utf-8
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
export ADMM_SQL_SERVER='127.0.0.1,1433'
export ADMM_SQL_USER="${ADMM_SQL_USER:-admm_ingestion}"
export ADMM_LOCAL_SQL_ROOT="$(python -c 'from pathlib import Path; print((Path.home()/"admm-real").as_posix())')"
python -m pip install './databricks/silver[test]' './databricks/gold' -r local/requirements-sql.txt
export SILVER_DELTA_JARS="$(python databricks/silver/tests/download_delta_jars.py work/delta-jars)"
if command -v winpty >/dev/null 2>&1; then
  winpty python local/run_sql.py --root "$ADMM_LOCAL_SQL_ROOT" --prompt-password
else
  python local/run_sql.py --root "$ADMM_LOCAL_SQL_ROOT" --prompt-password
fi
)
```

The block uses `~/admm-real`, outside the repository/OneDrive checkout, to keep
Windows paths shorter and raw files outside Git. The program default is the ignored
`work/adventureworks-real` if no root is supplied. It exports, processes all eleven
Silver/seven Gold tables, reconciles and replays them. It writes
`<chosen-root>/sql-acceptance-<export-id>.json` and `validation-status.json`. Only a final `status=SUCCEEDED` with
`replay.verified=true`, the expected table counts and reconciliation metrics
establishes local acceptance **for that SQL snapshot**. Do not interpret the mock
tests in this PR as those results. Azure acceptance remains false.

Credentials are accepted only through environment variables. A hidden prompt
stores the password in the current Python process environment, never on disk.
The password is removed before starting Spark/JVM workers. Settings repr, status
reports and error output omit credentials and raw connection strings. A terminal
that cannot hide input is rejected; there is no echo fallback. ODBC values use
brace escaping, so delimiter-containing credentials cannot inject properties.

For replay, run the same command again. The verified immutable export is reused;
there is **no SQL query or password prompt** for that export. The report explicitly
sets `export_reused=true` and `sql_queried_this_invocation=false`. SQL may have
changed since capture; a replay validates the captured snapshot, not today's DB.

## Snapshot, types and read-only behavior

- Only loopback `localhost` / `127.0.0.1` endpoints are accepted. Database is fixed
  to AdventureWorks and DB_NAME plus SQL Server EngineEdition are checked. Azure
  SQL/Managed Instance engine editions are rejected even through a local forward.
- Driver 18 uses encrypted local transport and trusts the local Docker self-signed
  certificate. This exception is confined to the enforced loopback endpoint.
  `ApplicationIntent=ReadOnly` is a routing hint, not a security boundary: the
  actual code issues only SELECT and session SET commands, and never commit/DML/DDL.
- One connection, autocommit disabled, SERIALIZABLE read transaction. Source counts
  and actual PK/schema metadata are checked before payload reads. No SNAPSHOT
  database setting is enabled. Read/range locks may block writers; timeout,
  deadlock, lost connection or mismatched counts fail the export. Do not run
  concurrent source changes during this initial local acceptance.
- SQL transaction always ends with **ROLLBACK**, including success, releasing locks
  before Spark starts. No SQL receipts, audit tables or cursors are changed.
- Bounded `fetchmany` chunks (default 5000 rows) write multiple typed Parquet parts.
  Integers/bit/decimal use explicit Arrow types; floats are not used for money.
  SQL timestamps are rendered by SQL's `CONVERT(datetime2(7),...)` to ISO strings,
  then exact 100 ns ticks are written as Parquet nanosecond timestamps. This follows
  SQL Server's datetime conversion semantics, not Python microsecond rounding.
  Arrow ns range limits fail rather than truncate unsupported dates.
- XML uses SQL's Unicode text serialization, uniqueidentifier a textual GUID and
  geography native `.Serialize()` binary, retaining its native representation
  including SRID rather than inventing WKT or dropping spatial data. Geography
  remains opaque in the existing Silver contract and is not copied into Gold.
- Every source column and actual ordered primary key must match the existing
  contract. There is no SELECT *, dynamic table option, permissive schema inference
  or silent additive-column promotion. All nine required Bronze metadata columns
  are populated; their historic pipeline-ID names contain **local export UUIDs**,
  not ADF run identifiers. Parquet footer metadata labels this explicitly local.

The first export is a complete initial snapshot. For the five INCREMENTAL tables,
its lower cursor is the existing initial `1900-01-01`/zero-key tuple and its upper
is the captured maximum `(ModifiedDate, PK...)` in that read transaction. SQL PK
ordering is deterministic, including the SalesOrderDetail compound key. This does
not implement ongoing SQL CDC or advance a remote watermark. To capture a NEW
snapshot use a new `--root`; do not edit a completed export or reset Delta controls.

## Neutral local evidence, not a fake Azure receipt

`local-sql-export.json` has format `admm.local.sql-export.v1`, status
`LOCAL_EXPORTED_VERIFIED`, source `SQL_SERVER_LOCAL`, database AdventureWorks,
read isolation/rollback, source and exported counts, real file sizes/SHA-256s,
cursor bounds and a canonical manifest checksum. It has **no remote COMMITTED
status** and `remote_commit_confirmed=false`. It never claims SQL receipt export,
ADF reconciliation or remote watermark confirmation.

`local/sql_manifest.py` translates that verified LOCAL authority **in memory** to
the legacy input dictionary required by the unchanged Silver function. The internal
COMMITTED enum/committed_at field are compatibility names only; that dictionary
is never saved as a receipt or presented as remote confirmation. `localsql` is
an internal contract binding, not an Azure storage account. This explicit adapter
keeps the existing Azure evidence validator/transformations unmodified.

The manifest is published last and exclusively, after all eleven source counts
match exported rows and every Parquet file passes the existing hash/footer checks.
A failed export leaves no valid success manifest. Partial files stay private for
inspection and are never adopted. A crash during final JSON publication fails
closed; use a new root after inspection rather than repairing its checksum.
Checksums detect corruption, not malicious local editing or independently attest
SQL provenance: local files and the operator remain within the trust boundary.

On every reuse, the neutral manifest, contract, exact folders/file sets, byte
hashes, footer counts and captured source counts are checked again, BEFORE the
Silver cached-success shortcut. A root is bound to its export hash and cannot be
mixed with fixture data or another export. The existing exclusive writer lock and
Silver/Gold per-table recovery remain active. There is no multitable transaction.

## Baselines and acceptance report

Default source gates are **504 Production.Product** and **121317
Sales.SalesOrderDetail**, as reported by the user. These are EXPECTATIONS until
observed by the actual exporter, not results from Codex. Intentional source changes
require explicit `--expected-product-rows` / `--expected-detail-rows` overrides and
an appropriate new root, not a generic validation bypass. All eleven actual source
counts must match the initial Silver input AND target counts with zero rejects.

The report includes the eleven observed SQL counts; source/export/Silver counts;
seven Gold row/duplicate/null/reject metrics; FKs, header/line and aggregate amount
reconciliation; environment versions; and the before/after **22 Delta versions**
(eleven Silver, two Silver controls, seven Gold, two Gold controls). Successful
replay writes none of those tables. Existing Gold currency/grain/Type 1 limitations
still apply: no invented FX conversion, tax duplication or accounting revenue.

If it fails, `validation-status.json` and stderr contain only static reason codes,
error type, optional SQLSTATE, stage and contract table name. Do not enable raw
ODBC/Spark credential tracing or publish raw Parquet, SQL exports or quarantine
rows. All default artifacts are under ignored `work/`; outside roots must also
stay outside Git. No passwords, raw rows or real manifests are included in this PR.

## Validation limits and references

The developer environment cannot reach the user's Windows Docker: TCP 1433 is
refused, Docker socket access is denied and no SQL credentials are attached.
The supplied SQL cursor tests are STRICT MOCKS; Arrow/Parquet and Spark/Delta
execution are real locally. ODBC 18/SQL query execution and Windows end-to-end
remain **NOT VERIFIED** until the single procedure above runs successfully.
The implementation does not assume that an operator's Windows localhost is this
cloud environment's localhost. See [test evidence](../docs/validation/local-sql/REPORT.md).

- [SQL Server serializable isolation and locking](https://learn.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql)
- [Native geography/binary serialization](https://learn.microsoft.com/en-us/sql/t-sql/spatial-geography/spatial-types-geography)
- [SqlGeography Serialize](https://learn.microsoft.com/en-us/dotnet/api/microsoft.sqlserver.types.sqlgeography.serialize)
- [ODBC Driver encryption/certificate attributes](https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute)
