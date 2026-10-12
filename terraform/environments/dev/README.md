# Dev platform Terraform root

This directory is the Terraform root for the `admm` development platform in East US. It currently manages the platform resource group, an ADLS Gen2 account with five filesystem boundaries, an empty Azure Data Factory with least-privilege access to Bronze, the Azure-side Self-hosted Integration Runtime (SHIR), and a Key Vault for future SQL connection secrets. It does not create pipelines, datasets, linked services, triggers, Databricks, or networking resources.

## Remote backend and authentication

State remains in the already-deployed bootstrap backend:

| Setting | Value |
|---|---|
| Resource group | `rg-admm-dev-tfstate` |
| Storage account | `admmdevtfstatef6d99ba4` |
| Container | `tfstate` |
| State key | `dev/platform.tfstate` |
| Authentication | Microsoft Entra ID through Azure CLI |

The backend uses `use_azuread_auth = true`. The AzureRM provider uses the existing Azure CLI login, a runtime `TF_VAR_subscription_id`, and `storage_use_azuread = true`; no subscription ID, tenant ID, credential, storage key, or secret is committed.

## Managed resources

Terraform will manage:

1. Resource group `rg-admm-dev-platform` in East US.
2. One Standard LRS, Hot-tier StorageV2 account with hierarchical namespace enabled.
3. Five private containers—`bronze`, `silver`, `gold`, `quarantine`, and `audit`. In an HNS-enabled account, a blob container is an ADLS Gen2 filesystem. The configuration uses `azurerm_storage_container` with `storage_account_id`, which creates the filesystems through Azure Resource Manager without requiring temporary data-plane access for the deploying identity.
4. One Azure Data Factory with a system-assigned managed identity.
5. One `Storage Blob Data Contributor` role assignment for that identity, scoped to the existing `bronze` container only.
6. One Self-hosted Integration Runtime named `shir-admm-dev` in the existing Data Factory.
7. One Standard-tier, RBAC-enabled Azure Key Vault with soft delete and purge protection.
8. One `Key Vault Secrets User` role assignment for the Data Factory managed identity, scoped only to that vault.

The storage name is `st` + `admm` + `dev` + `dl` + the first eight characters of an MD5 hash of the selected subscription ID, capped at 24 characters. This is deterministic, lowercase, compliant with Storage naming rules, and unlikely to collide globally. The subscription ID is used only to derive a stable non-secret suffix.

The Data Factory name is `adf-admm-dev-` plus the same stable subscription-derived suffix. The suffix makes the globally scoped factory name deterministic without committing account-specific identifiers.

The Key Vault name is `kv-admm-dev-` plus the same stable suffix. It is deterministic, globally unique in practice, and within the 24-character Key Vault name limit.

### Filesystem purposes

| Filesystem | Purpose |
|---|---|
| `bronze` | Immutable-as-landed source extracts and ingestion metadata. |
| `silver` | Validated, standardized, and conformed data. |
| `gold` | Curated dimensional data products for analytics. |
| `quarantine` | Invalid records with data-quality reason metadata. |
| `audit` | Pipeline audit, reconciliation, and operational records. |

## Security and cost decisions

- Anonymous blob/container access and shared-key authentication are disabled; Azure identities will be granted RBAC in a later approved step.
- HTTPS is mandatory, TLS 1.2 is the minimum, and cross-tenant replication is disabled.
- Public network access remains enabled because networking/private endpoints are explicitly outside this increment. Access still requires Microsoft Entra authorization; this is not anonymous public data access.
- Standard LRS is the cost-conscious dev choice. It stores three copies in one region but does not provide zone or cross-region resilience.
- Hot tier favors the frequent reads/writes expected while building the platform. Azure charges for stored capacity, transactions, retrieval where applicable, and data transfer. HNS directory/ACL operations also generate transactions. The five empty filesystems have negligible capacity cost; costs begin as data and operations accumulate.
- Versioning, soft delete, lifecycle rules, diagnostic destinations, and reserved capacity are intentionally deferred so this step does not create unreviewed retention or observability costs.

## Data Factory identity and Bronze access

Data Factory uses a system-assigned managed identity, so Azure creates and rotates its credentials; no principal ID or credential is hardcoded. Terraform reads the generated principal ID directly from the factory and assigns `Storage Blob Data Contributor` at `azurerm_storage_container.data_lake["bronze"].id`. For the existing AzureRM v4 container resource, the plan confirms that this value is the Azure Resource Manager container ID—not a data-plane URL—and limits access to Bronze rather than the entire Storage Account or Resource Group.

The deployment identity must be able to create role assignments at the Bronze scope, normally through `Owner`, `User Access Administrator`, or an equivalent custom role. `skip_service_principal_aad_check = true` avoids a transient lookup failure while the new managed identity replicates through Microsoft Entra ID; it does not broaden the assigned role or scope. RBAC propagation after deployment can still take several minutes.

An empty Data Factory has no pipelines to execute, but future activity orchestration, data movement through SHIR, data-flow compute, and monitoring volume can incur charges. This step adds no pipeline runs, Azure-hosted integration-runtime compute, or managed virtual network. Review the current Azure Data Factory pricing before enabling workloads and use budgets/alerts for the dev subscription.

## Self-hosted Integration Runtime

The Terraform resource creates only the **Azure-side registration** named `shir-admm-dev`. It does not install or configure the SHIR agent on Windows. The Windows service is the runtime that will open outbound connections to Azure Data Factory and connect locally to SQL Server.

For local-to-Azure ingestion to work, the Windows host, Docker Desktop, the `adventureworks-sql` container, SQL Server on `127.0.0.1:1433`, and the SHIR Windows service must all be running. Because the agent and Docker-published SQL port are on the same Windows host, the future SQL linked service can reach that local endpoint through SHIR; no linked service is created in this increment.

After an approved Terraform deployment, complete registration manually at a high level:

1. In Azure Data Factory Studio, open **Manage → Integration runtimes** and select `shir-admm-dev`.
2. Download the supported SHIR installer from the Azure-provided link and install it on the Windows machine hosting Docker and SQL Server.
3. Retrieve one registration key interactively from the Azure portal and enter it directly into the Microsoft Integration Runtime Configuration Manager.
4. Confirm the node reports **Running/Online** in both the Windows configuration manager and Data Factory Studio.

Never paste a registration key into Terraform configuration, outputs, shell history, documentation, source control, logs, or issue screenshots. AzureRM 4.x exposes the primary and secondary keys as sensitive computed attributes of `azurerm_data_factory_integration_runtime_self_hosted`; under the explicitly approved approach, Terraform will therefore retain them in the access-controlled remote state even though this project never references or outputs them. Access to the state container must remain restricted.

## Key Vault and SQL secrets

The vault uses the current Azure tenant from `data.azurerm_client_config.current.tenant_id` and Azure RBAC rather than legacy access policies. Data Factory receives only `Key Vault Secrets User` on this vault, allowing its managed identity to read secret values without granting secret creation, deletion, role administration, or broader subscription access. No `azurerm_key_vault_secret` resource is declared, so SQL usernames, passwords, and connection strings do not pass through Terraform variables, plans, outputs, or state.

When the SQL linked service is designed, create its secrets manually in the Azure portal/CLI or through an approved secure secret-management process. Use stable secret names, enter values interactively or through a protected CI secret, and never place values in `.tfvars`, shell history, documentation, logs, screenshots, or source control. The developer or automation identity performing this task needs a data-plane role such as `Key Vault Secrets Officer` on this vault; Azure subscription `Owner` or `Contributor` alone should not be assumed to grant secret data access under the RBAC model. Grant that operational role separately to the appropriate identity rather than widening Data Factory's read-only role.

Public network access remains enabled so the current developer workstation and future Azure services can reach the vault without adding private endpoints in this increment. Authentication and authorization are still required. The Standard tier is the cost-conscious choice; charges are primarily operation-based and will begin as secrets are read or managed.

Soft delete uses the minimum seven-day retention period, and purge protection is enabled. A Terraform destroy would soft-delete the vault but cannot purge it while retention applies; the protected name cannot be recreated during that period, and purge protection cannot be disabled after it is enabled. Treat vault destruction as an exceptional operation, and use a different deterministic suffix only if an intentionally deleted vault must be replaced before retention expires.

## Review and validation

Run from the repository root:

```bash
az login
az account set --subscription "<subscription-name-or-id>"
az account show --query '{name:name, id:id, tenantId:tenantId}' --output table
export TF_VAR_subscription_id="$(az account show --query id --output tsv)"

terraform -chdir=terraform/environments/dev fmt -check -recursive
terraform -chdir=terraform/environments/dev init
terraform -chdir=terraform/environments/dev validate
terraform -chdir=terraform/environments/dev plan -out=dev.tfplan
terraform -chdir=terraform/environments/dev show dev.tfplan
```

With the existing platform and SHIR already deployed, review that the plan adds exactly one `azurerm_key_vault` and one vault-scoped `azurerm_role_assignment`, with no changes to Data Factory, SHIR, Storage, containers, or existing Bronze RBAC. Confirm the role is `Key Vault Secrets User`, its principal is the existing Data Factory identity, and its scope is the new vault ID. The plan must contain no secret resources or values, bootstrap changes, or unrelated services. Do not run `apply` until the plan is approved.

After a future approved deployment, the outputs can be inspected with `terraform -chdir=terraform/environments/dev output`. Generated state, plans, local variable files, and `.terraform/` content remain ignored by Git; `.terraform.lock.hcl` should remain committed.
