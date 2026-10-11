provider "azurerm" {
  features {}

  subscription_id     = var.subscription_id
  storage_use_azuread = true
}

data "azurerm_client_config" "current" {}
