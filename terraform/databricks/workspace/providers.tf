terraform {
  required_version = ">= 1.9.0"
  # 1.110.0 embeds SDK 0.113.0: OAuth delegates to the named CLI profile.
  # 1.95.0 reads a legacy file cache and cannot see Windows secure storage.
  required_providers {
    databricks = { source = "databricks/databricks", version = "1.110.0" }
  }
}
provider "databricks" {
  alias     = "workspace"
  host      = var.workspace_host
  profile   = var.workspace_profile
  auth_type = "databricks-cli"
}
