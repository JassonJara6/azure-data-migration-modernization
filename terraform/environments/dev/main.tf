resource "azurerm_resource_group" "platform" {
  name     = local.resource_group_name
  location = var.location
  tags     = local.common_tags
}

resource "azurerm_storage_account" "data_lake" {
  name                = local.storage_account_name
  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location

  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  access_tier              = "Hot"
  is_hns_enabled           = true

  allow_nested_items_to_be_public  = false
  cross_tenant_replication_enabled = false
  default_to_oauth_authentication  = true
  https_traffic_only_enabled       = true
  min_tls_version                  = "TLS1_2"
  public_network_access_enabled    = true
  shared_access_key_enabled        = false

  tags = local.common_tags
}

# In a hierarchical-namespace account, each private blob container is an ADLS
# Gen2 filesystem. Using the Resource Manager API avoids requiring temporary
# data-plane permissions merely to create these empty filesystem boundaries.
resource "azurerm_storage_container" "data_lake" {
  for_each = local.filesystem_names

  name                  = each.value
  storage_account_id    = azurerm_storage_account.data_lake.id
  container_access_type = "private"
}

resource "azurerm_data_factory" "platform" {
  name                = local.data_factory_name
  location            = azurerm_resource_group.platform.location
  resource_group_name = azurerm_resource_group.platform.name

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

resource "azurerm_role_assignment" "data_factory_bronze_contributor" {
  scope                = azurerm_storage_container.data_lake["bronze"].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.platform.identity[0].principal_id
  principal_type       = "ServicePrincipal"

  # The system-assigned service principal is created in the same operation;
  # skipping the directory lookup avoids transient Microsoft Entra replication failures.
  skip_service_principal_aad_check = true
}

resource "azurerm_data_factory_integration_runtime_self_hosted" "local_sql" {
  name            = local.self_hosted_integration_runtime_name
  data_factory_id = azurerm_data_factory.platform.id
}

resource "azurerm_key_vault" "platform" {
  name                = local.key_vault_name
  location            = azurerm_resource_group.platform.location
  resource_group_name = azurerm_resource_group.platform.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  enable_rbac_authorization     = true
  purge_protection_enabled      = true
  public_network_access_enabled = true
  soft_delete_retention_days    = 7

  tags = local.common_tags
}

resource "azurerm_role_assignment" "data_factory_key_vault_secrets_user" {
  scope                = azurerm_key_vault.platform.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.platform.identity[0].principal_id
  principal_type       = "ServicePrincipal"

  skip_service_principal_aad_check = true
}
