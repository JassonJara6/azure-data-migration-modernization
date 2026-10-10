# Terraform remote-state bootstrap

This isolated configuration creates only the foundation needed to store remote Terraform state:

- `rg-admm-dev-tfstate`, a dedicated resource group in East US;
- a globally unique Standard LRS StorageV2 account;
- a private `tfstate` blob container; and
- a `Storage Blob Data Contributor` assignment for the identity currently authenticated with Azure CLI.

It does **not** create ADLS Gen2, Data Factory, Key Vault, Databricks, a self-hosted integration runtime, or any ingestion resources.

## Why bootstrap is separate

The main platform configuration cannot initialize its Azure Storage backend until that storage exists. This bootstrap therefore keeps its own small state file locally and creates the backend first. Storing the bootstrap state in the backend it creates would introduce a circular dependency. Bootstrap state and plan files are ignored by Git; retain the local state securely because it remains the ownership record for these resources.

The storage account name is `admmdevtfstate` plus the first eight characters of an MD5 hash of the selected subscription ID (and is capped at 24 characters). The hash is used only as a stable, non-secret uniqueness suffix; no subscription or tenant ID is committed. AzureRM authenticates with the existing Azure CLI session, while the subscription ID is supplied at runtime because AzureRM provider v4 requires it.

The account disables public blob access, shared-key authorization, cross-tenant replication, and non-HTTPS traffic. It requires TLS 1.2, enables infrastructure encryption, blob versioning, and seven-day blob/container soft deletion. Public network access remains enabled so a developer workstation can reach the backend; authorization uses Microsoft Entra ID and least-privilege blob data access rather than account keys.

## Review and deploy

From the repository root, select the intended Azure CLI subscription and export it for Terraform:

```bash
az login
az account set --subscription "<subscription-name-or-id>"
az account show --query '{name:name, id:id, tenantId:tenantId}' --output table
export TF_VAR_subscription_id="$(az account show --query id --output tsv)"

terraform -chdir=terraform/bootstrap fmt -check
terraform -chdir=terraform/bootstrap init
terraform -chdir=terraform/bootstrap validate
terraform -chdir=terraform/bootstrap plan -out=bootstrap.tfplan
terraform -chdir=terraform/bootstrap show bootstrap.tfplan
```

Review the plan before running any apply. When approved, the project owner—not automation in this step—can run:

```bash
terraform -chdir=terraform/bootstrap apply bootstrap.tfplan
terraform -chdir=terraform/bootstrap output backend_configuration
```

Role assignments require the signed-in identity to have permission to create them. Azure role propagation can also take several minutes after apply.

## Later main-backend configuration

After bootstrap is applied, its output supplies the main configuration's backend values. The future root configuration will use an `azurerm` backend with a distinct key such as `dev/platform.tfstate` and `use_azuread_auth=true`. Backend values will be passed during `terraform init`; they will not be hardcoded into reusable infrastructure modules. Do not migrate this bootstrap's own state into that backend.
