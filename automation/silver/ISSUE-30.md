# Issue #30: Silver preflight and evidence, without paid runs

## Findings

The current `NO_ELIGIBLE_SILVER_FALLBACK_CHECK_EASTUS_SKUS_AND_QUOTAS`
occurs before Job submission. It is separate from earlier run `11937840542482`
(Task `831744328908969`) failing in `committed_evidence` after compute started.
No live Azure/Databricks observations were made for this fix. The actual reason
both fallback candidates fail on the operator's subscription remains unconfirmed.

Reproducible preflight defects corrected:

- Any Azure `Location` restriction previously rejected a candidate, even when
  its restriction scope was another region. Now `restrictionInfo.locations`
  (or `values`) is checked for East US. Missing scope remains conservatively
  blocked. Zone restrictions do not block this non-zonal configuration.
- Quota machine-name matching now strips surrounding whitespace and ignores
  casing for `cores` and the family taken from the SKU. Display/localized names
  are **not** guessed or translated; absent keys block with an explicit reason.
- The launcher previously hid the failed predicate. It now emits the report from
  the same function that selects the bundle alternatives, including failure.
- A JSON-null quota response must not activate the function's unit-test-only
  catalog mode; launcher and diagnostic explicitly require the real quota list.

Each reviewed candidate has `eligible`, static `reasons`, and static `actions`.
Quota entries include `limit`, `used`, `headroom`, or `MISSING`/`NOT_CHECKED`.
The latter is allowed only for pure catalog unit tests, never either CLI route.
No raw API responses, subscription identifiers, provider error messages,
credentials or receipt data are printed. A global policy/schema/data-shape
blocker invalidates the report rather than silently dropping that safeguard.
Malformed evidence still blocks globally, even if another candidate looks valid.

No larger or speculative fallback type was added. The official directional
matrix for D4ds_v5 contains precisely D4ads_v5 and D4ds_v4. DBR 15.4 LTS,
SINGLE_USER runner, 4 vCPU / 16 GiB, zero workers, On Demand, fixed primary policy
and saved-plan/bundle approvals remain unchanged. A policy mismatch is reported,
not automatically reconciled. There are no retries, policy/grant changes,
Terraform changes or cloud writes.

## One read-only operator verification command

After reviewing/merging this PR, with the existing Azure login and OAuth profile:

```bash
python automation/silver/check_capacity.py --profile admm-workspace --job-id 210884055971910
```

This calls only Azure account/SKU/usage reads, Databricks Job/settings/node-type/
policy GETs and local bundle schema generation. It invokes no launcher provision
or deploy, Terraform, SQL export, receipt download, Job run, cluster creation or
cloud write. It does not read/write local untracked config/state/evidence files.
Azure/CLI may perform their normal credential-cache handling. Existing reader
permissions are required; no new authentication/grants are requested by the code.
Unlike the separate unmerged PR #29 diagnostic, this command shares the exact
selection code with the launcher, including policy validation.

Exit 0 means `ELIGIBLE_SNAPSHOT`, **not** proof of physical capacity or approval
to run. Exit 1 prints a blocked report or safe read failure. Interpret reasons:

| Reason | Required review (never automatic) |
| --- | --- |
| `NOT_IN_WORKSPACE_CATALOG` | Check supported workspace node types |
| `NOT_IN_AZURE_SKU_CATALOG` / `NOT_IN_EASTUS` | Check subscription's East US SKU catalog |
| `EASTUS_LOCATION_RESTRICTED` | Review East US subscription restrictions with Azure |
| `REGIONAL_QUOTA_MISSING` / `FAMILY_QUOTA_MISSING` | Check machine keys and reader visibility; no guessed quota |
| `*_QUOTA_INSUFFICIENT` | Compare the reported headroom with 4 vCPU; review quota separately |
| `*_POLICY_*` | Review existing fixed policy; no bypass or edit in this tool |
| `INVALID_*` | Incomplete/malformed input blocks; no capacity assumed |

SKU listing, quota snapshots and physical capacity are distinct. Do not start
paid compute merely to diagnose the current predicate. Read-only operator data
is essential to identify which actual subscription condition is blocking.

## Separate committed evidence review

The receipt's existence, valid UTF-8/JSON and direct runner READ_VOLUME grant are
operator-reported facts, not proof of its accessibility inside the failed task.
The notebook performs a synchronous **Python driver** `open('/Volumes/...',
encoding='utf-8')`, not Scala, a subprocess or a UDF. The documented UC volume
requirements (DBR 13.3+) cover DBR 15.4. Dedicated-mode Scala/FUSE restrictions
are not evidence that this Python access mechanism must be replaced.

Keep the PR #28 reader and safe exception type/package-file/line diagnostic.
An added offline test verifies the exact volume path, UTF-8 and complete reads
beyond 64 KiB; never substitute `dbutils.fs.head`, which can truncate receipts.
Existing tests cover permission failure, E/S, encoding, JSON/identity validation
and notebook handling. No speculative storage API fallback or permission change
was made. READ_VOLUME also requires the applicable USE_CATALOG/USE_SCHEMA privileges;
those prerequisites and the actual runner/FUSE context have not been verified
by these offline tests. The original runtime exception remains unknown without the safe task
output; this PR does not request another paid run to obtain it.

## Official references

- [Flexible compatibility matrix](https://learn.microsoft.com/en-us/azure/databricks/compute/flexible-node-type-instances)
- [Flexible node and policy requirements](https://learn.microsoft.com/en-us/azure/databricks/compute/flexible-node-types)
- [Azure quota versus physical capacity](https://learn.microsoft.com/en-us/azure/virtual-machines/quotas)
- [Azure Resource SKU restriction scope](https://learn.microsoft.com/en-us/rest/api/compute/resource-skus/list)
- [Unity Catalog volume requirements and limitations](https://learn.microsoft.com/en-us/azure/databricks/volumes/)

## PR #32 takeover: bounded reads and offline acceptance

The reported `READ_TIMEOUT` established only that a CLI read exceeded its local
limit, not a VM stockout or quota denial. PR #32's original timeout increase was
untested. This revision keeps its configurable `read_json` timeout (other
consumers still default to 60 seconds), but replaces the opaque full-catalog
CLI enumeration with shared explicit ARM Resource SKU GET pagination in both
`check_capacity.py` and the launcher. Only the documented East US location filter
is sent; unsupported server-side SKU-name filters are not invented.

Catalog rules:

- 60 seconds maximum per page attempt, 180 seconds for the complete catalog,
  at most 100 pages; the smaller remaining budget is enforced on each subprocess.
- One retry of the **same GET page**, only on an identified timeout/connectivity/
  DNS failure. Authentication, malformed JSON and access denial are not retried.
  There are no compute or Job retries.
- Validate every page and follow `nextLink` only on HTTPS ARM, the exact project
  subscription/SKU endpoint and the pinned API version. Reject repeated,
  off-endpoint, malformed or excessive links. Never print pagination tokens.
- A partial catalog is never passed to selection: later-page failure, elapsed
  budget or page-limit exhaustion blocks the check, even if a reviewed VM was
  present on an earlier page.
- Progress JSON goes to stderr with stage/page/attempt/count only. The diagnostic
  retains a single final JSON report on stdout, with static stage-suffixed error
  codes. Other read steps are bounded at 90 seconds, quota usage at 120 seconds.
  These are read limits, not a promise that Azure will respond within them.

Realistic page-envelope fixtures, multiple pages, slow subprocesses, transient
failures, malformed Job/CLI responses, legacy injected readers and unsafe error
payloads are tested offline. A second demonstrable ambiguity was fixed: duplicate
SKU capability names are rejected instead of letting the last value silently
replace an earlier CPU/memory declaration. No live claim follows from mocks.

Official pagination contract:
[Resource SKUs List, API 2021-07-01](https://learn.microsoft.com/en-us/rest/api/compute/resource-skus/list?view=rest-compute-2021-07-01).

### Evidence and remaining gates

- [x] Review PR #32 candidate patch and issue #30; add executable regression tests.
- [x] Keep primary/alternate compatibility, policy restrictions, quota requirements,
  cost envelope, approvals and read-only command behavior.
- [x] Run local automation, Spark/Delta and Terraform provider-mock plan checks.
- [x] Preserve committed-evidence diagnostics and complete UTF-8 driver reads;
  do not replace access methods or grants without evidence.
- [ ] Read the real subscription/workspace catalog and quota snapshot. This Codex
  environment exposes no Azure/Databricks identity, capabilities or authenticated
  workspace connector, so that observation is unavailable here.
- [ ] Inspect the old Databricks task's original exception and prove runner
  USE_CATALOG/USE_SCHEMA, READ_VOLUME and FUSE access in its actual context.
  No authorized authenticated task-output reader is available here. Neither a
  local receipt nor an operator's grant listing proves those runtime conditions.
- [ ] End-to-end Silver success, reconciled metrics and replay acceptance. These
  require a separately authorized real run; this PR never runs or requests one.

The existing single read-only command above remains available if a final live
check is later authorized. No sequence of speculative operator diagnostics is
required to review this PR. No deployments, paid runs, cloud writes, Bronze
changes, identity changes or permission changes were performed.

### Final local verification commands and results

Executed in the Codex Linux workspace with cached Python dependencies, pinned
Delta JARs and Terraform 1.9.8; no cloud credentials were configured:

- `SILVER_WHEEL_BUILD_TESTS=1 SILVER_PROVIDER_AUTH_TESTS=1 SILVER_TERRAFORM_BIN=/workspace/work/infrastructure/bin/terraform PYTHONPATH=/workspace/work/silver/deps python -m pytest -q automation/silver/tests --tb=short` — **233 passed**, no skips.
- `SILVER_DELTA_JARS=/workspace/work/silver/jars/delta-spark_2.12-3.2.1.jar,/workspace/work/silver/jars/delta-storage-3.2.1.jar PYTHONPATH=/workspace/work/silver/deps:/workspace/work/silver/bronze-deps python -m pytest -q databricks/silver/tests --tb=short` — **47 passed**, no skips.
- Terraform `fmt -check -recursive` for the four existing roots; `validate -no-color`
  and `test -no-color` in each — four valid configurations, **12 mock-provider
  plan tests passed**, no `apply` or real backend access.
- `python -m compileall -q automation/silver databricks/silver/src databricks/silver/tools databricks/silver/notebooks` and `git diff --check` — passed.

These commands record local evidence, not instructions to run diagnostics or
paid compute. Linux mocks do not establish native Windows or live FUSE behavior.
