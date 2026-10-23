mock_provider "azurerm" {}
variables {
  resource_group_name = "rg-admm-dev-platform"
  location            = "eastus"
  prefix              = "admm-dev-f6d99ba4"
  tags                = { environment = "dev" }
  container_ids = {
    bronze = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Storage/storageAccounts/stadmmdevdlf6d99ba4/blobServices/default/containers/bronze"
    silver = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Storage/storageAccounts/stadmmdevdlf6d99ba4/blobServices/default/containers/silver"
    audit  = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Storage/storageAccounts/stadmmdevdlf6d99ba4/blobServices/default/containers/audit"
  }
}
run "workspace_and_container_permissions" {
  command = plan
  assert {
    condition     = azurerm_databricks_workspace.silver.sku == "premium" && !azurerm_databricks_workspace.silver.custom_parameters[0].no_public_ip
    error_message = "Unity Catalog requires Premium workspace."
  }
  assert {
    condition     = azurerm_role_assignment.storage["bronze"].role_definition_name == "Storage Blob Data Reader"
    error_message = "Bronze must never receive writer RBAC."
  }
  assert {
    condition     = length(azurerm_role_assignment.storage) == 3 && azurerm_role_assignment.storage["audit"].scope == var.container_ids["audit"]
    error_message = "Scope all three assignments to existing containers."
  }
}
run "register_provider_only_if_needed" {
  command = plan
  variables { register_provider = true }
  assert {
    condition     = length(azurerm_resource_provider_registration.databricks) == 1 && azurerm_resource_provider_registration.databricks[0].name == "Microsoft.Databricks"
    error_message = "Only the explicitly approved Databricks RP may be registered."
  }
}
