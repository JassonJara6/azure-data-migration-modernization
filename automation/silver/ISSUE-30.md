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
