# Silver compute capacity (East US)

**Current read path:** [Issue #33](ISSUE-33.md) replaces the full ARM SKU
catalog strategy below with explicitly scoped Databricks provider evidence.
Older catalog/pagination instructions are historical, not the production path.

## Diagnosis and supported route

The operator reported Job `210884055971910`, failed run `698409497747491`,
`CLOUD_PROVIDER_RESOURCE_STOCKOUT / SkuNotAvailable: Standard_D4ds_v5`.
The ephemeral single-node cluster requested only that VM. This is a VM allocation
failure before Silver runs, not evidence of a notebook, Delta or Bronze defect.
An exact-filter `az vm list-skus` returning `[]` alone cannot distinguish a
subscription restriction, catalog/filter issue or transient capacity shortage.
A listed SKU also does not guarantee current physical capacity.

Official Azure Databricks references:

- [Flexible node types](https://learn.microsoft.com/en-us/azure/databricks/compute/flexible-node-types)
- [Directional compatibility matrix](https://learn.microsoft.com/en-us/azure/databricks/compute/flexible-node-type-instances)
- [Bundle fields (added in CLI 0.285.0)](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources)
- [Compute policy requirements](https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition#flexible-node-types)

CLI 0.275.0's official schema lacks the driver flexibility field. Upgrade the
operator's modern Databricks CLI to **0.285.0 or later** before using this launcher.
No OAuth credentials need to be copied or replaced. A local schema capability
check fails closed with `FLEXIBLE_NODES_REQUIRE_DATABRICKS_CLI_0_285_OR_NEWER`.
The source bundle uses an existing cluster, so it cannot select that cluster's
VMs; the launcher overlay is the sole ephemeral-compute configuration.

The overlay sets `driver_node_type_flexibility.alternate_node_type_ids`. The
single node is the driver: worker flexibility alone would not solve this error.
Default primary `Standard_D4ds_v5` has reviewed alternates, in order:
`Standard_D4ads_v5`, `Standard_D4ds_v4`. These are the documented compatible
local-disk/x86/4-vCPU/16-GiB group. Do **not** substitute `Standard_D4as_v5` as an
arbitrary fallback for D4ds_v5: equal CPU/RAM does not establish disk compatibility.
For an already configured primary D4as_v5, the bounded reviewed list is
D4ads_v5, D4ds_v5, D4s_v5 (directional compatibility differs).

Before generating the bundle, read-only preflight:

1. Requires DBR 15.4 LTS and the primary in workspace `list-node-types`.
2. Reads CLI bundle schema; requires driver flexibility support.
3. Lists Azure VM SKUs with `--all --location eastus` in the project subscription,
   then intersects reviewed alternates with the workspace catalog and Azure
   catalog: East US, 4 vCPU, 16 GiB, no subscription-level Location restriction.
   Zone-specific restrictions do not exclude a non-zonal request.
4. Reads regional and VM-family quota usage (`az vm list-usage`). Requires at
   least 4 unallocated vCPU in both regional `cores` and the alternate's family.
   Unknown quota evidence blocks rather than inventing availability.
5. Reads the existing compute policy. Primary/driver fixed rules must agree.
   If a fixed flexibility list exists, both types **and order** must match;
   empty or conflicting lists block. Unresolved policy families block review.
   No policy, Terraform state or workspace setting is changed.

Missing Azure compute-read permissions, restricted SKUs, unavailable quota or
an incompatible policy therefore stop deployment. The operator needs read
access for `Microsoft.Compute/skus/read`, `Microsoft.Compute/locations/usages/read`,
workspace node types and this cluster policy, plus the existing deployment rights.
Catalog/quota snapshots can race other allocations; no check guarantees capacity.
The existing hash approval covers the exact generated alternate list.

## Approved operator procedure

Merge the reviewed PR, upgrade CLI if needed, then use the existing command:

```bash
winpty python automation/silver/launch.py --stage silver --uc-mode workspace --workspace-profile admm-workspace --metastore-id b67792ab-c53b-48b4-ae57-fc45b869ed7a --config work/silver-dev.json --deploy
```

This is a **real deployment and Job execution**, only to be run with authorization.
Review saved Terraform plans and bundle hash prompts as usual. Keep primary
`node_type_id` unchanged; the existing fixed-primary policy is reused. The launcher
never enables a global workspace feature. Official documentation permits a
per-compute custom list even with workspace automatic flexibility disabled.
There are no real workspace capability/quota/capacity observations from local tests.
If this workspace rejects the API field despite a current CLI, stop and request
workspace administrator/Databricks support review; do not bypass the compute policy.

If stockout persists, inspect failed run/cluster events and the actual VM type
acquired (cluster details JSON / Clusters API). Check subscription restrictions,
regional and family quota, and Azure support/service health. Wait for capacity
and authorize a new attempt; do not blindly loop Job retries. The launcher emits
`SILVER_COMPUTE_STOCKOUT_CHECK_CAPACITY_GUIDE_NO_AUTOMATIC_RETRY` when the terminal
run state identifies stockout. Raw run messages are not printed.

Legacy alternative when upgrading/enabling flexible nodes is impossible: select
another reviewed 4-vCPU/16-GiB **primary**, such as the already supported D4as_v5,
only after checking SKU/quota and reviewing the corresponding fixed policy plan.
That policy change requires separate approval; this PR does not perform it.
Do not bypass the policy, edit the deployed Job manually or enlarge the machine.
The current launcher intentionally requires a CLI capable of the safe explicit
fallback route rather than silently reverting to a single fragile VM choice.

## Costs and validation limits

DBR 15.4, Dedicated/SINGLE_USER runner, zero workers, On Demand, job-only compute,
2-hour Job timeout, no schedules, and no additional resources remain unchanged.
Only equal-size reviewed alternatives are possible; Azure VM and Databricks DBU
rates can differ by SKU, so verify regional prices rather than assuming identical
cost. No permanent cluster, instance pool, extra node or Spot VM is introduced.
No automatic application-level retry is added (it could rerun completed writes).
Cancel an active Job through the existing launcher stop procedure if needed.

Local tests validate schema support, list selection, Azure restrictions, quota
snapshots, fixed-policy compatibility, driver-only configuration and sanitized
failure diagnostics. Actual allocation and processing of the 11 sources remain
pending an authorized real run. No Terraform, Unity Catalog, Bronze, credentials,
ADF or R2 changes are part of this fix.

## Safe diagnostics for local preflight failures

The reported Windows `TypeError` after Terraform `changes: []` has **not been
reproduced with production Azure/Databricks responses**. The official CLI v1.22.1
binary was exercised locally: `bundle schema` returns valid JSON and the existing
flexibility detector succeeds on that complete schema. A CLI downgrade is not
justified by that evidence. No authenticated Azure/Databricks queries were run.

The previous implementation assumed collection fields were always non-null,
quota counts were Python numbers, and policy definition was always a JSON string.
Synthetic variants with null schema properties, null memory/capability fields,
string quota counts or an already-decoded policy object reproduce exceptions.
These are regression inputs, **not a claim about the failing operator response**.
The preflight now validates shapes and numerical values explicitly, accepts
numeric strings and decoded policy objects, and blocks incomplete/malformed
capacity evidence with field-specific codes. It never substitutes missing quotas
with an assumption of capacity.

Subprocesses explicitly decode UTF-8 with strict errors; Python-based CLIs inherit
`PYTHONUTF8=1` and `PYTHONIOENCODING=utf-8`. This avoids dependence on Windows
cp1252 defaults without requiring changes to the shell or login profiles. Invalid
UTF-8 produces `CLI_OUTPUT_NOT_UTF8`, never replacement-decoded JSON.

For unexpected exceptions, the final JSON includes `preflight_step` plus
repository-relative `locations` containing file and line numbers. It deliberately
omits exception messages, source code lines, local variables, absolute paths,
command arguments, raw CLI responses and exception chaining. A subsequent failure
can therefore identify its exact code location without printing a traceback.
Remove the operator's temporary `traceback.print_exc()` modification when adopting
this version; it is unnecessary and can expose sensitive response details. No
such temporary modification was present in the clean checkout used for this PR.

After merging, the existing approved launcher command requires no extra encoding
variables. Its preflight remains read-only, but `--deploy` still proceeds into
saved-plan application and real Job execution after the existing approvals. This
PR does not authorize or perform those operations. If preflight still fails, the
safe JSON location is the evidence needed to investigate the actual failing line.
