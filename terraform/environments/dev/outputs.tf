output "resource_group_name" {
  description = "Name of the development platform resource group."
  value       = azurerm_resource_group.platform.name
}

output "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account."
  value       = azurerm_storage_account.data_lake.name
}

output "filesystem_names" {
  description = "Names of the private ADLS Gen2 filesystems."
  value       = sort(keys(azurerm_storage_container.data_lake))
}

output "data_factory_name" {
  description = "Name of the development Azure Data Factory instance."
  value       = azurerm_data_factory.platform.name
}

output "data_factory_principal_id" {
  description = "Principal ID of the Data Factory system-assigned managed identity."
  value       = azurerm_data_factory.platform.identity[0].principal_id
}
