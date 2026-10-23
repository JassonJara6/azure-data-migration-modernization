# Dev Unity Catalog root

New, separate state key `dev/databricks-uc.tfstate` in the **existing backend**. Does not migrate or modify `dev/platform.tfstate`. Databricks provider pinned/locked at 1.95.0, account OAuth profile + Azure CLI workspace auth. No tokens or keys in variables.

Receives Azure outputs after the workspace exists. Reuses a discovered East US metastore by default; creation requires explicit permission and a reviewed plan. Preserve automatic workspace assignments without adopting or replacing them. `create_metastore`/`manage_assignment` choices must be kept consistent with owned state on retries (the launcher does this).

Creates only dedicated project UC resources and a secretless Job SP/policy. No persistent compute. Input volumes READ_VOLUME only for the Job. Existing shared metastore privileges and unrelated grants are never broadened by this root. All runtime identifiers are supplied by the [launcher/runbook](../../../automation/silver/README.md).
