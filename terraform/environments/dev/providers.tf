provider "azurerm" {
  features {}

  # Planning must not register Azure resource providers implicitly.
  resource_provider_registrations = "none"

  storage_use_azuread = true
  subscription_id     = var.subscription_id
}

data "azurerm_client_config" "current" {}
