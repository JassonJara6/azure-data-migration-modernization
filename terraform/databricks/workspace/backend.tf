# Same UC backend/key as dev root. Roots are mutually exclusive, never concurrent.
# Never migrate/overwrite dev/platform.tfstate.
terraform {
  backend "azurerm" {
    resource_group_name  = "rg-admm-dev-tfstate"
    storage_account_name = "admmdevtfstatef6d99ba4"
    container_name       = "tfstate"
    key                  = "dev/databricks-uc.tfstate"
    use_azuread_auth     = true
  }
}
