# Dev platform Terraform root

This directory is the Terraform root for the future `admm` development platform in East US. At this step it contains only provider, environment, and remote-backend configuration—there are **no managed platform resources or module calls**. Initializing or planning this root must not change the bootstrap resources or create ADLS Gen2, Data Factory, Key Vault, Databricks, networking, SHIR, or ingestion resources.

## Remote backend

The backend uses the already-deployed bootstrap resources:

| Setting | Value |
|---|---|
| Resource group | `rg-admm-dev-tfstate` |
| Storage account | `admmdevtfstatef6d99ba4` |
| Container | `tfstate` |
| State key | `dev/platform.tfstate` |
| Authentication | Microsoft Entra ID through Azure CLI |

These names are non-secret, environment-specific backend coordinates, so they are intentionally declared in `backend.tf`. `use_azuread_auth = true` prevents Terraform from retrieving or using a storage account key. The signed-in identity therefore needs `Storage Blob Data Contributor` on the state account, which the deployed bootstrap assigned.

The AzureRM provider also uses the existing Azure CLI login. AzureRM v4 requires a subscription ID, so it is read at runtime through `TF_VAR_subscription_id`; no subscription ID, tenant ID, credential, or secret is committed.

## Initialize and verify

Run these commands from the repository root after reviewing the configuration:

```bash
az login
az account set --subscription "<subscription-name-or-id>"
az account show --query '{name:name, id:id, tenantId:tenantId}' --output table
export TF_VAR_subscription_id="$(az account show --query id --output tsv)"

terraform -chdir=terraform/environments/dev fmt -check -recursive
terraform -chdir=terraform/environments/dev init
terraform -chdir=terraform/environments/dev validate
terraform -chdir=terraform/environments/dev plan -refresh=false -out=dev.tfplan
terraform -chdir=terraform/environments/dev show dev.tfplan
```

Expected results:

1. `init` reports that the `azurerm` backend was configured successfully and creates only local `.terraform/` metadata plus `.terraform.lock.hcl`.
2. `validate` succeeds.
3. The plan reports **no changes** because this root currently declares no resources.
4. The `tfstate` container may remain empty until Terraform first writes state; do not run `apply` merely to create an empty state blob.

To independently verify Entra-authorized access to the existing container without modifying it:

```bash
az storage blob list \
  --account-name admmdevtfstatef6d99ba4 \
  --container-name tfstate \
  --auth-mode login \
  --output table
```

The command may return an empty list before the main state is first written. An authorization error usually means the role assignment has not propagated yet or the active Azure CLI identity/subscription is not the one used by the bootstrap.

Do not run `apply`, `destroy`, state migration, or any command from `terraform/bootstrap/` as part of this initialization. Review and commit the generated `.terraform.lock.hcl`; all other generated state, plans, local variable files, and `.terraform/` content remain ignored by Git.
