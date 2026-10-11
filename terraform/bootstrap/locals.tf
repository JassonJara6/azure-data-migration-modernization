locals {
  resource_group_name = "rg-${var.project}-${var.environment}-tfstate"

  # Storage account names must be globally unique and contain only 3-24 lowercase
  # alphanumeric characters. A stable subscription-derived suffix avoids a
  # hardcoded tenant-specific name without introducing a random resource.
  storage_account_name = substr(
    lower("${var.project}${var.environment}tfstate${substr(md5(data.azurerm_client_config.current.subscription_id), 0, 8)}"),
    0,
    24
  )

  common_tags = {
    environment = var.environment
    managed-by  = "terraform"
    project     = "adventureworks-azure-data-platform"
    purpose     = "terraform-remote-state"
  }
}
