# Dev platform Terraform root

This directory is the Terraform root for the `admm` development platform in East US. This increment manages only the platform resource group and an ADLS Gen2 account with five filesystem boundaries. It does not create Data Factory, Databricks, Key Vault, networking, SHIR, or ingestion resources.

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

The storage name is `st` + `admm` + `dev` + `dl` + the first eight characters of an MD5 hash of the selected subscription ID, capped at 24 characters. This is deterministic, lowercase, compliant with Storage naming rules, and unlikely to collide globally. The subscription ID is used only to derive a stable non-secret suffix.

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

Review that the plan contains exactly one resource group, one storage account, and five private containers/filesystems. It must not contain bootstrap changes or any unrelated service. Do not run `apply` until the plan is approved.

After a future approved deployment, the outputs can be inspected with `terraform -chdir=terraform/environments/dev output`. Generated state, plans, local variable files, and `.terraform/` content remain ignored by Git; `.terraform.lock.hcl` should remain committed.
