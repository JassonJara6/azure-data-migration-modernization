terraform {
  required_version = ">= 1.9.0"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 4.0" }
  }
}
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "prefix" { type = string }
variable "container_ids" { type = map(string) }
variable "tags" { type = map(string) }

variable "register_provider" {
  type    = bool
  default = false
}
resource "azurerm_resource_provider_registration" "databricks" {
  count = var.register_provider ? 1 : 0
  name  = "Microsoft.Databricks"
  lifecycle { prevent_destroy = true }
}
resource "azurerm_databricks_workspace" "silver" {
  depends_on                    = [azurerm_resource_provider_registration.databricks]
  name                          = "dbw-${var.prefix}"
  resource_group_name           = var.resource_group_name
  location                      = var.location
  sku                           = "premium"
  managed_resource_group_name   = "rg-${var.prefix}-databricks-managed"
  public_network_access_enabled = true
  # Dev cost choice: public-IP job nodes with Databricks-managed NSGs. Avoid
  # implicitly provisioning a continuously billed managed NAT gateway for SCC.
  # This is not a private/SCC production network design.
  custom_parameters { no_public_ip = false }
  tags = var.tags
  lifecycle { prevent_destroy = true }
}
resource "azurerm_databricks_access_connector" "silver" {
  depends_on          = [azurerm_resource_provider_registration.databricks]
  name                = "ac-${var.prefix}-silver"
  resource_group_name = var.resource_group_name
  location            = var.location
  identity { type = "SystemAssigned" }
  tags = var.tags
  lifecycle { prevent_destroy = true }
}
resource "azurerm_role_assignment" "storage" {
  for_each = {
    bronze = "Storage Blob Data Reader"
    silver = "Storage Blob Data Contributor"
    audit  = "Storage Blob Data Contributor"
  }
  scope                            = var.container_ids[each.key]
  role_definition_name             = each.value
  principal_id                     = azurerm_databricks_access_connector.silver.identity[0].principal_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}
output "connection" {
  value = {
    workspace_id          = azurerm_databricks_workspace.silver.workspace_id
    workspace_resource_id = azurerm_databricks_workspace.silver.id
    workspace_host        = "https://${azurerm_databricks_workspace.silver.workspace_url}"
    access_connector_id   = azurerm_databricks_access_connector.silver.id
    managed_identity_id   = azurerm_databricks_access_connector.silver.identity[0].principal_id
  }
}
