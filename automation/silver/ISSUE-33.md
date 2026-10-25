# Issue #33: remove the unusable full SKU endpoint

## Change and trust boundary

Both production callers now use `read_scoped_skus`; neither calls
`Microsoft.Compute/skus`, `az vm list-skus`, or an exhaustive pagination helper.
A mock with a permanently unresponsive full-catalog endpoint verifies that the
launcher/checker can complete without touching it. Timeouts were not increased.

The replacement is **Databricks cloud-provider node information**, not a claim
that generic VM listings or Retail Prices prove Azure subscription eligibility:

1. Read exactly the existing Azure Databricks workspace resource (GET through
   `az resource show`). Require the pinned project subscription/resource ID and
   `eastus` location. Read the workspace profile's current metastore assignment
   and require its `workspace_id` to equal ARM `properties.workspaceId`.
2. From the already-required Databricks `list-node-types` response, require
   exactly one record for each of D4ds_v5, D4ads_v5 and D4ds_v4. Validate the
   matching instance ID, actual 4 cores / 16384 MiB, and non-hidden/non-deprecated
   flags. Extra unrelated types do not require enumeration or validation.
3. Require **explicit** `node_info.status` arrays. The documented cloud-provider
   flags `NotEnabledOnSubscription` and `NotAvailableInRegion` reject a candidate.
   An explicit empty list means no such provider restriction is reported in this
   workspace snapshot. Missing/null information is **unknown**, never replaced
   with an empty array. Unknown flags block rather than being ignored.
4. Read actual Azure East US `az vm list-usage` and match quota machine keys:
   `standardDDSv5Family`, `standardDADSv5Family`, `standardDDSv4Family` and `cores`.
   The mapping is pinned to the three reviewed families; it is not a fabricated
   quota value or an inference from localized display labels. Missing keys block.
   Report primary quota too. Known insufficient primary headroom does not make
   a quota-eligible fallback ineligible; missing primary quota evidence blocks.
5. Keep existing fixed-policy validation and exact compatible alternate order,
   DBR 15.4, SINGLE_USER, On Demand, zero workers, 4-vCPU/16-GiB cost envelope,
   run-as delegation and hash approvals. Provider rejection of the primary blocks
   conservatively; no automatic policy or identity changes are made.

`ScopedSkuEvidence` is an explicit contract tagged
`DATABRICKS_CLOUD_PROVIDER_NODE_INFO_V1`: exact requested set, provenance and
region. Selector rejects missing/ambiguous records and wrong scope/family; it
never requires a full ARM catalog. Reports name this source. No empty ARM
`restrictions` arrays are invented in the adapter. Provider flags are checked
as provider flags, not represented as a fetched Azure Resource SKU response.

Reads use 90-second bounded subprocesses (quota 120s). Progress is sanitized on
stderr; one final JSON remains on stdout. No retry of the unusable endpoint,
compute allocation, ARM deployment validation, deployment, or Job execution occurs.

## Supported source limits — do not claim more than observed

This is a cloud-provider status **proxy snapshot** from the actual Databricks
workspace plus direct ARM identity/region and quota reads. It is not direct
exhaustive ARM restriction data or a guarantee of live physical capacity. The
Databricks API marks `node_info`/`status` optional: **if the real workspace omits
those fields, this route must block with `SCOPED_PROVIDER_STATUS_MISSING`.** Local
fixtures cannot establish that the workspace returns them. No live credential
or authenticated workspace connector is available in this Codex environment.

Research found no documented per-VM-name equivalent to Compute Resource SKUs
that supplies its full restriction data: Resource SKUs List supports only a
location filter. Adding a name filter, client-side JMESPath, or changing CLI/SDK
would still rely on the same endpoint or unsupported behavior. Regional VM size
listings and pricing cannot prove subscription restrictions, so are not used.
If explicit provider status is unavailable, there is **no proven safe automatic
bypass** in this PR. It requires authoritative provider evidence/support, not a
paid trial run or an assumption that missing fields mean unrestricted.

A successful report means only `ELIGIBLE_SNAPSHOT` on the declared evidence;
it does not mean Silver succeeded. Exact source and residual uncertainty are
preserved in the report/documentation. Existing stockout/quotas can still race
any preflight, just as with direct ARM snapshots.

## Existing committed evidence failure

Run `11937840542482` / task `831744328908969` is independent of this preflight.
This environment currently exposes no Azure/Databricks identity, capabilities,
or callable Databricks connector, so existing authenticated task output cannot
be inspected here. The original exception remains unknown. Preserve the PR #28
safe type/file/line diagnostic and complete UTF-8 driver `/Volumes` read. No FUSE,
UC permission, Bronze, receipt or storage-method changes were guessed.

## Single optional read-only decision point

The existing command is unchanged; it now uses only the scoped strategy:

```bash
python automation/silver/check_capacity.py --profile admm-workspace --job-id 210884055971910
```

It reads existing metadata only and cannot start compute. It is documented for
a later authorized final decision, not a request for repeated exploratory runs.
No `--deploy`, Jobs, clusters, Terraform apply, ADF, cloud IAM/UC writes, secrets,
or paid resources were used to prepare this PR.

## Official references

- [Databricks List node types: dimensions and cloud-provider node_info.status](https://docs.databricks.com/api/clusters/v2/list-node-types)
- [Databricks SDK cloud-provider status contract](https://github.com/databricks/databricks-sdk-go/blob/main/service/compute/model.go)
- [Azure workspace GET and workspaceId](https://learn.microsoft.com/en-us/rest/api/databricks/workspaces/get?view=rest-databricks-2024-05-01)
- [Resource SKUs: only location filter supported](https://learn.microsoft.com/en-us/rest/api/compute/resource-skus/list?view=rest-compute-2021-07-01)
- [VM family quota machine names](https://learn.microsoft.com/en-us/azure/devops/managed-devops-pools/prerequisites)
- [DDSv4 and DADSv5 family names](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/how-to-request-quota-increase)
- [Compute quota versus capacity](https://learn.microsoft.com/en-us/azure/virtual-machines/quotas)
- [Directional compatible VM types](https://learn.microsoft.com/en-us/azure/databricks/compute/flexible-node-type-instances)

## Local validation evidence

- Automation with `SILVER_WHEEL_BUILD_TESTS=1 SILVER_PROVIDER_AUTH_TESTS=1
  SILVER_TERRAFORM_BIN=/workspace/work/infrastructure/bin/terraform
  PYTHONPATH=/workspace/work/silver/deps python -m pytest -q automation/silver/tests
  --tb=short`: **238 passed**, no skips. Includes real local wheel builds,
  local provider-OAuth mocks, and deterministic absent/ambiguous/scoped data,
  disabled subscription/region, quota denial, bad dimensions, authorization,
  malformed responses and timeout cases. Full-catalog reads are forbidden in the
  injected CLI fixture, so success cannot depend on a responsive catalog.
- `SILVER_DELTA_JARS=/workspace/work/silver/jars/delta-spark_2.12-3.2.1.jar,/workspace/work/silver/jars/delta-storage-3.2.1.jar
  PYTHONPATH=/workspace/work/silver/deps:/workspace/work/silver/bronze-deps
  python -m pytest -q databricks/silver/tests --tb=short`: **47 passed**, no skips
  (170.12s). This is local OSS Spark/Delta, not an Azure or FUSE validation.
- Terraform fmt/validate in the four existing roots; **12 mock-provider plan
  tests passed**. No backend access, real provider writes or apply.
- compileall and git diff --check passed. No code changes to Terraform, Bronze,
  Unity Catalog, business transformations or credentials.

Quota references above establish names only. Actual values are read exclusively
from **Microsoft.Compute** usage via `az vm list-usage`, never from DevOps/MySQL
quotas or documentation defaults.
