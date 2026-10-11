provider "azurerm" {
  features {}

  storage_use_azuread = true
  subscription_id     = var.subscription_id
}

data "azurerm_client_config" "current" {}
