# Optional development Databricks workspace module

Called only by `terraform/environments/dev` with `enable_databricks=true`. Inputs reuse the existing RG, region, tags and Bronze/Silver/audit container ARM IDs. No storage, Key Vault, ADF or custom network resources are created. Premium workspace, system-assigned Access Connector and three container-only RBAC assignments; Bronze is always Reader. Optional explicit Microsoft.Databricks RP registration is used only when missing, never provider auto-registration.

Workspace/connector/RP removal is guarded. Outputs expose workspace host/ARM ID/numeric ID and connector/MI IDs, never keys. No cluster is created here: the Bundle uses ephemeral job compute. See [the deployment procedure](../../../automation/silver/README.md) for account-admin/UC dependencies, staged state, costs and approvals.
