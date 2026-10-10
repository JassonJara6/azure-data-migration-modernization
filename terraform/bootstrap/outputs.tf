output "backend_configuration" {
  description = "Non-secret values needed to configure the main Terraform azurerm backend."
  value = {
    resource_group_name  = azurerm_resource_group.state.name
    storage_account_name = azurerm_storage_account.state.name
    container_name       = azurerm_storage_container.state.name
    key                  = "dev/platform.tfstate"
    use_azuread_auth     = true
  }
}
