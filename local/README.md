# Offline Bronze → Silver → Gold

This is an OSS PySpark/Delta workflow, not a Databricks emulator. No Azure client,
credentials, cloud download, Job, cluster service or Terraform is invoked. The
Spark master is explicitly `local[2]`. Azure Free Trial is left untouched.

## Reproduce with one environment

Use Python **3.11**, Java **17** (`JAVA_HOME` and PATH), and roughly 4 GB free RAM.
Validated on Linux; on Windows prefer WSL2. Native Git Bash is usable only with
an already-working Windows Spark/Hadoop installation and is not certified here.
Run from the repository root. `python` must select
the Python 3.11 virtual environment. No SQL Server is required for fixtures.

```bash
python -m pip install './databricks/silver[test]' './databricks/gold'
export SILVER_DELTA_JARS="$(python databricks/silver/tests/download_delta_jars.py work/delta-jars)"
python local/run.py --fixture --root work/local-lake
python local/run.py --fixture --root work/local-lake
python local/run.py --fixture --fixture-phase 2 --root work/local-lake
python local/run.py --fixture --fixture-phase 2 --root work/local-lake
```

The only downloads are free Python/JVM dependencies from package repositories.
Once cached, the processing itself is offline. `SILVER_DELTA_JARS` contains two
local JAR paths separated by a comma. On Windows use `JAVA_HOME` for a Java 17 JDK
and `PYTHONUTF8=1`; set `MSYS_NO_PATHCONV=1` if your environment converts command
arguments. No `/Workspace` paths, CLI profiles or cloud tokens are used.

Every invocation performs Silver, Gold, and a second no-write replay. The second
command also proves persistence across independent Python/Spark processes. The
phase-2 command must follow phase 1 in the same lake and exercises incremental
updates/inserts, composite keys and FULL dimension deletions. Reports are written
to `work/local-lake/acceptance-<parent>.json`; version comparisons include all
13 Silver and 9 Gold business/control tables. Fixture parents start with
`30000000-...`, deliberately different from the real Azure Bronze parent.

Outputs remain under the ignored `work/` tree. A local exclusive writer lock
prevents two invocations in one lake; different processes must not access these
namespaces concurrently. After a hard process kill, remove `.writer.lock` only
once you have verified its PID no longer exists. A normal exception releases it.
There is no multi-table transaction. Only audit status SUCCEEDED exposes a
complete analytical snapshot; a failed publication must be resumed first.

## Existing real local Bronze data

No real data is downloaded, invented or extracted by this tool. Supply an
**authoritative COMMITTED receipt** and its exact local Parquet mirror; preserve
all manifest-relative folders and original metadata, names, bytes and hashes.

```bash
python local/run.py --root work/real-local-lake \
  --receipt /path/to/authoritative-parent.json \
  --bronze-root /path/to/local-bronze-mirror \
  --account storageaccountfromreceipt
```

Use a new output root for a different provenance/source root. Synthetic and
operator-provided inputs cannot share a lake. Real mode verifies the same eleven
manifest/hash/row/schema/cursor contracts, but does not independently prove who
exported that SQL receipt. A SQL Server `.bak` is not a Parquet mirror; it must be
restored/exported/reconciled by the existing Bronze tooling separately. This
implementation does not change Bronze, create fake real receipts, reset any SQL
watermark or claim Azure validation.

The Spark catalog is rebuilt from each local Delta directory on startup, so no
Hive server or persistent metastore is required. Root/location paths must remain
stable. Do not move Delta directories independently of their local lake.

## Tests

```bash
python -m pytest -q databricks/silver/tests
python -m pytest -q databricks/gold/tests
```

Run these as separate commands/sessions. CI runs both on Python 3.11/Java 17 and
also executes the four CLI invocations above. `pytest` fixtures use temporary
storage; no paid services or real personal records are needed. See the Gold
[model decisions](../databricks/gold/README.md) and the checked-in
[local acceptance report](../docs/validation/local/REPORT.md).
