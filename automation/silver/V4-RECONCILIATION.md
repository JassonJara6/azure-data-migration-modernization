# Recover the completed v4 migration without repeating it

## Confirmed operator evidence

Job `210884055971910` and policy `0004947D67FAA646` already use
`Standard_D4ds_v4` for both node types, zero workers and disabled alternatives.
The original transition filename SHA-256 was verified by the operator.
The post-update GET differed from the approved settings in exactly three places:

| Field | Approved request/before | Reported GET | Treatment |
| --- | --- | --- | --- |
| `driver_node_type_flexibility` | `{"alternate_node_type_ids":[]}` | `{}` | Normalize only under verified fixed empty policy |
| `worker_node_type_flexibility` | `{"alternate_node_type_ids":[]}` | `{}` | Same |
| `edit_mode` | `UI_LOCKED` | `EDITABLE` | Functional difference; block |

The old comparison treated all three as generic settings mismatch. Its mocked
response copied the request exactly, so it did not exercise empty list omission.
This PR adds reported response fixtures, not evidence of live API execution.
The original update sent only `job_clusters`; it did not explicitly preserve
`edit_mode`. The operator's GET proves the mode changed during that operation;
without server logs we cannot prove which server-side default caused it.
Future migration requests explicitly preserve a supported existing edit mode.
`UI_LOCKED` and `EDITABLE` are never treated as equivalent or globally ignored.

## Narrow normalization

Only the two present, completely empty flexibility objects normalize to explicit
empty arrays. Missing containers, nulls, alternate arrays with nodes, malformed
arrays and unknown keys still block. The effective policy must independently
pass all existing v4 fixed-node/zero-worker/runtime/identity/no-fallback checks.
No-fallback requests and local bundle YAML retain explicit empty arrays.
The same contextual normalization is used by the read-only preflight and artifact
publication guard, avoiding a second failure after local reconciliation.
Neither capacity eligibility nor physical availability is inferred from this.
All unrelated settings, runner identities, permissions and approval hashes remain
protected; this PR changes no Terraform or transformation code.

Sources:
- [Azure compute policy reference](https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition): the fixed empty policy string maps to an empty alternate list.
- [SDK NodeTypeFlexibility serialization](https://github.com/databricks/databricks-sdk-go/blob/main/service/compute/model.go): optional list serialization (`omitempty`) explains why a CLI/API representation can omit an empty member. This is not proof of this workspace's server implementation.
- [Jobs update API](https://docs.databricks.com/api/workspace/jobs/update): `UI_LOCKED` and `EDITABLE` have different editability semantics.
- [DAB UI locking](https://kb.databricks.com/jobs/cant-edit-jobs-created-using-databricks-asset-bundles-dabs-using-the-ui): bundles use `UI_LOCKED`.

## Recovery: no repeated Terraform apply or v4 Job migration

Azure Free Trial remains mandatory. Do not rerun `migrate_v4.py --apply` to recover
local files. The new tool only issues Workspace GETs, never Terraform, Azure
writes, DAB deployment, Job updates, Jobs execution or permission changes.
Use the exact original transition, not a regenerated one.

From the repository root, in Windows Git Bash:

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' PYTHONUTF8=1 PYTHONIOENCODING=utf-8
python automation/silver/reconcile_v4_local.py --config work/silver-dev.json --transition work/silver-deployment/v4-job-transition-1d4b6e1a042109ee67a6d41c55b32cc024fa98d7ada55cc013cc4ed1a4e70e27.json
```

**Current expected outcome: `V4_EDIT_MODE_CHANGED`.** It writes no local files
and correctly refuses to certify the unlocked Job as the approved locked one.
There is no `--ignore-edit-mode` or general mismatch override.

### Separate remote approval still required

The sole remaining intended remote metadata correction is restoring the existing
Job's `edit_mode` to `UI_LOCKED`. It changes UI editability, not compute, identity,
policy, tasks, wheel, notebook or data. It neither runs a Job nor creates compute.
It is deliberately NOT executed by this PR or the reconciliation tool.

After explicit authorization, and only if a fresh GET still shows the exact three
reported differences and there are no active runs, an authorized operator can use:

```bash
databricks api post /api/2.1/jobs/update --profile admm-workspace --json '{"job_id":210884055971910,"new_settings":{"edit_mode":"UI_LOCKED"}}'
```

The API may still normalize fields: require the reconciliation preview to pass
against the original complete settings. Do not widen the comparison if it fails.
Do not execute this write merely because it is documented here.

### Local reconciliation after the mode has been restored

Run the preview above, then the same command with `--write-local` to request an
interactive hash approval. The hash binds original transition, live settings and
policy, and all three local file hashes. It rechecks identity/metastore, Job ID,
no active runs, policy and full settings before writing. The Job ownership record
must match the original host and ID. It recomputes the intended node transition;
filename hash alone does not authorize arbitrary settings.

Only the staged bundle `job_clusters` and config `node_type_id` are replaced.
Other bundle/config values remain semantically intact. The YAML serializer may
change formatting. Both files independently accept only the reviewed old/target
cluster state or old/target node value, enabling recovery after an interrupted
local write. A completed replay leaves bytes unchanged and needs no remote writes.
The original transition stays untouched. Do not edit config or bundle concurrently.
No claim of a cross-file atomic filesystem transaction is made.

No real migration, Windows OAuth call, remote restore or Silver run was performed
for this PR. Physical capacity, actual eleven-table results and real idempotence
remain pending separate authorization and Azure execution. Local synthetic tests
do not close Silver acceptance.

## Local verification for this change

- Full automation suite: **435 passed** (15.17 seconds), including **30 new cases**.
- Focused migration/normalization suites: **45 passed** (0.30 seconds).
- Python compileall and `git diff --check`: passed.
- Full automation invocation enabled real CLI-schema/loopback contract checks,
  wheel builds and provider-auth tests using locally installed CLI 1.22.1 and
  Terraform 1.9.8. All cloud responses for migration/reconciliation were simulated.
- No Azure/Databricks remote validation or writes. No Spark transformation changes;
  the Spark/Delta suite was not rerun for this automation-only correction.
