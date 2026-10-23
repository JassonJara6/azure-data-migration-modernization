terraform {
  required_version = ">= 1.9.0"
  required_providers {
    databricks = { source = "databricks/databricks", version = "1.95.0" }
  }
}
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.account_id
  profile    = var.account_profile
}
provider "databricks" {
  alias                       = "workspace"
  host                        = var.workspace_host
  azure_workspace_resource_id = var.workspace_resource_id
  auth_type                   = "azure-cli"
}
