# Silver receipt access: FUSE failure versus Unity Catalog authorization

## Evidence and limits

The operator reports `SILVER_RECEIPT_FILE_NOT_FOUND` at Python `open()` in
`admm_silver/evidence.py`. The v4 cluster started. The approved receipt exists at
`/Volumes/admm_dev/inputs/receipts/20d65511-8190-41a0-8ad2-23d7568363d2.json`.
Runner `7d5adada-f25c-47f2-ab76-9880b52d5490` reportedly has `READ_VOLUME` and
`USE_SCHEMA`. These reports were not independently queried from this environment:
no authenticated Azure/Databricks identity is attached.

**The exact infrastructure/authorization cause is NOT CONFIRMED.** A Python
FileNotFoundError proves the driver could not resolve that POSIX pathname at that
moment. It does not establish that the remote object was deleted, that FUSE is
unsupported on this compute, or that USE_CATALOG is missing. Operator CLI reads
are not runner-identity access tests. The earlier generic committed_evidence
failure is not evidence for the cause of this new failure.

Official guidance supports Python POSIX access to Volumes and recommends
DataFrame APIs for dedicated compute data access. The Databricks knowledge-base
article about FUSE FileNotFound in Databricks Apps describes **Apps**, not this
notebook Job, and must not be used to claim this Job has the same root cause.

The demonstrable code limitation was its exclusive dependency on the driver FUSE
mount and its lack of a native UC file-reader path. The correction selects Spark
DataFrame `binaryFile` explicitly in the notebook, rather than retrying open() or
substituting operator SDK credentials. It therefore removes this dependency;
it is not a claim that a missing permission or real missing file has been fixed.

## Change and security contract

- Spark reads the exact literal `dbfs:/Volumes/<catalog>/<schema>/<volume>/<parent>.json`
  with the existing Spark session and Job runner. No SDK, PAT, direct ADLS path,
  RDD API, alternate identity, SQL write or filesystem copy is introduced.
- Paths reject glob syntax, traversal, whitespace, schemes and parent/file mismatch
  before Spark. This project's configured UC identifiers and UUID filenames fit
  the deliberately restricted accepted path grammar.
- Exactly one file is required. Bytes are read completely, UTF-8 is strict and
  duplicate JSON keys/parent mismatches retain their existing failures.
- An explicit 10 MiB receipt limit prevents unbounded bytes from being collected
  to the driver. Oversize/empty files fail, never truncate. A conditional content
  projection bounds collected content; Spark may still perform source I/O before
  evaluating it. This is not an object-store streaming/rate limit.
- Local fixture files still use open(); Volume paths require the explicit Spark
  reader. There is no fallback if Spark reports access denied or a missing file.
- Structured Spark PATH_NOT_FOUND/FILE_NOT_FOUND and permission codes get safe
  static diagnostics. Unknown service failures are RECEIPT_VOLUME_READ_FAILED;
  allowlisted structured provider codes and exception types also survive the safe
  notebook/launcher reports; no raw exception body, receipt contents or credentials
  are printed. Stage and
  package source location remain in existing notebook failure reports.
- Existing full COMMITTED/hash/11-source validation in pipeline.run is unchanged,
  and runs before Silver publication. No metadata/watermark/permission changes.

## Permissions: what is and is not established

Required effective privileges for the **runner** are:

| Securable | Required read privilege |
| --- | --- |
| `admm_dev` | `USE_CATALOG` |
| `admm_dev.inputs` | `USE_SCHEMA` |
| `admm_dev.inputs.receipts` | `READ_VOLUME` |

`terraform/databricks/workspace/main.tf` already declares the catalog runner grant.
That is desired configuration, not evidence of deployed/effective privileges.
External-volume storage credential/location and storage identity permissions also
must be valid. They are not changed or declared verified by this PR.

## One read-only permission check, no running compute

Run from Git Bash with the existing operator profile. This only queries effective
UC grants scoped to the runner; it never starts compute or grants permissions.
An authorization error while inspecting grants means UNKNOWN, not missing rights.
Review inherited/group privileges as well as direct grants.

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' PYTHONUTF8=1
for item in 'catalog admm_dev' 'schema admm_dev.inputs' 'volume admm_dev.inputs.receipts'; do
  read -r kind name <<< "$item"
  databricks grants get-effective "$kind" "$name" --principal 7d5adada-f25c-47f2-ab76-9880b52d5490 --profile admm-workspace --output json || break
done
```

No receipt data or tokens are requested by this command. Do not use `--debug`,
export a token, grant ALL PRIVILEGES or impersonate another identity as a workaround.
If USE_CATALOG is absent, report it and request authorization before any grant.
Even a sufficient grant response does not prove runtime data access.

## Controlled real validation (NOT executed here)

1. Merge only after review. Under separate publication authorization, build and
   publish the new content-versioned wheel **and notebook** using the existing
   launcher artifact-only procedure (`--stage silver --uc-mode workspace --deploy
   --deploy-only`). It changes remote Job/artifact metadata and is not run by this
   PR. Require ARTIFACT_BYTES_VERIFIED and exact expected_package_version before
   any run; the old wheel cannot contain this fix.
2. Before authorizing a Job, verify its resolved receipt_file and parent_run_id,
   runner application ID, dedicated cluster identity, DBR version and the effective
   privileges above. Do not assume the failed run used today's Job defaults.
3. Only after separate execution authorization, use the existing single Silver Job.
   A successful committed_evidence read under the actual runner confirms native
   UC access for that execution. If it fails, retain the safe code/stage/location
   and task run ID. Permission/FUSE root cause remains open until such evidence.
4. Require all 11 table metrics and reconciliation, then the existing same-parent
   replay with no additional Delta versions before claiming Azure acceptance.

No Jobs, clusters, remote writes or paid Azure resources were used for this change.
Azure Free Trial remains mandatory. Local Spark tests prove the binaryFile code
path and contracts, not Unity Catalog enforcement, real ADLS availability or FUSE.

## Official references

- [Volume file operations and supported interfaces](https://learn.microsoft.com/en-us/azure/databricks/volumes/volume-files)
- [Dedicated compute limitations: DataFrame file APIs](https://learn.microsoft.com/en-us/azure/databricks/compute/dedicated-limitations)
- [Volume privileges](https://learn.microsoft.com/en-us/azure/databricks/volumes/privileges)
- [Spark binary file source](https://spark.apache.org/docs/3.5.3/sql-data-sources-binaryFile.html)
- [Apps-specific FileNotFound guidance (not a Job diagnosis)](https://kb.databricks.com/unity-catalog/queries-to-access-files-on-a-unity-catalog-volume-fail-with-filenotfound-error)

## Local test results

- Full Silver/Spark/Delta suite: **72 passed**, 147.86 seconds. Python 3.11.17,
  Java 17, PySpark 3.5.3, Delta Lake 3.2.1, PyArrow 19.0.1.
  Includes the existing 11-source FULL/incremental MERGE, interrupted recovery,
  same-parent replay and unchanged Delta-version checks, plus 22 new receipt cases.
- Full automation suite: **439 passed**, 21.13 seconds (four new safe diagnostic
  projection cases). Real local CLI-schema/loopback and provider-auth tests enabled.
- Python compileall, Bash syntax for the documented commands, git diff whitespace
  and scope checks: passed.
- The first development run was **70 passed, 1 failed**: a notebook fixture used
  a synthetic filename unrelated to its requested parent. The fixture was updated
  to the exact parent filename; the final complete run above passed.
- Spark tests use local synthetic files. Volume paths/authorization failures are
  mocked at the boundary; no Unity Catalog/FUSE/ADLS compatibility was tested live.
- No real receipt, tokens, credentials, generated logs or runtime results are
  committed with this PR.

Reproduce after installing the existing test extra and selecting Python 3.11 /
Java 17 (with the existing Delta dependency cache or SILVER_DELTA_JARS):

```bash
python -m pytest -q databricks/silver/tests --tb=short
python -m pytest -q automation/silver/tests --tb=short
```

Optional pinned CLI, wheel-build and provider-auth checks in the automation suite
use the existing `SILVER_DATABRICKS_BIN`, `SILVER_WHEEL_BUILD_TESTS=1`,
`SILVER_PROVIDER_AUTH_TESTS=1` and `SILVER_TERRAFORM_BIN` environment settings.
Without those switches, optional tests can be skipped; report skips explicitly.
