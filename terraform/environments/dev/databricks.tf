# Opt-in extension; all existing resource addresses/backend remain unchanged.
variable "enable_databricks" {
  description = "Add the Silver workspace/connector only after reviewing a saved plan."
  type        = bool
  default     = false
}
variable "register_databricks_provider" {
  description = "Explicitly manage Databricks RP registration only if not already registered."
  type        = bool
  default     = false
}
module "databricks" {
  register_provider   = var.register_databricks_provider
  count               = var.enable_databricks ? 1 : 0
  source              = "../../modules/databricks_workspace"
  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location
  prefix              = "${var.name_prefix}-${var.environment}-${substr(md5(data.azurerm_client_config.current.subscription_id), 0, 8)}"
  container_ids       = { for name in ["bronze", "silver", "audit"] : name => azurerm_storage_container.data_lake[name].id }
  tags                = local.common_tags
}
output "silver_workspace" {
  value = var.enable_databricks ? merge(module.databricks[0].connection, {
    storage_account_name = azurerm_storage_account.data_lake.name
    resource_group_name  = azurerm_resource_group.platform.name
  }) : null
}
