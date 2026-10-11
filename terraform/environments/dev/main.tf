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
