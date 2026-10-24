terraform {
  required_version = ">= 1.9.0"
  required_providers {
    databricks = { source = "databricks/databricks", version = "1.95.0" }
  }
}
provider "databricks" {
  alias     = "workspace"
  host      = var.workspace_host
  profile   = var.workspace_profile
  auth_type = "databricks-cli"
}
