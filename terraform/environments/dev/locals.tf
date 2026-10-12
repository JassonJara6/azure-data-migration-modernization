locals {
  resource_group_name = "rg-${var.name_prefix}-${var.environment}-platform"

  # Storage account names must be globally unique and use only 3-24 lowercase
  # alphanumeric characters. The subscription-derived suffix is stable and
  # avoids introducing a random provider or a manually chosen global name.
  storage_account_name = substr(
    lower("st${var.name_prefix}${var.environment}dl${substr(md5(data.azurerm_client_config.current.subscription_id), 0, 8)}"),
    0,
    24
  )

  data_factory_name = lower(
    "adf-${var.name_prefix}-${var.environment}-${substr(md5(data.azurerm_client_config.current.subscription_id), 0, 8)}"
  )

  self_hosted_integration_runtime_name = "shir-${var.name_prefix}-${var.environment}"

  key_vault_name = lower(
    "kv-${var.name_prefix}-${var.environment}-${substr(md5(data.azurerm_client_config.current.subscription_id), 0, 8)}"
  )

  filesystem_names = toset([
    "audit",
    "bronze",
    "gold",
    "quarantine",
    "silver",
  ])

  common_tags = {
    environment = var.environment
    managed-by  = "terraform"
    project     = "adventureworks-azure-data-platform"
    purpose     = "data-platform"
  }
}
