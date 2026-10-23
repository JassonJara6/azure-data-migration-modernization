mock_provider "azurerm" {
  mock_data "azurerm_client_config" {
    defaults = {
      subscription_id = "00000000-0000-0000-0000-000000000001"
      tenant_id       = "00000000-0000-0000-0000-000000000002"
      client_id       = "00000000-0000-0000-0000-000000000003"
      object_id       = "00000000-0000-0000-0000-000000000004"
    }
  }
}
variables { subscription_id = "00000000-0000-0000-0000-000000000001" }
run "platform_extension_is_opt_in" {
  command = plan
  assert {
    condition     = length(module.databricks) == 0 && output.silver_workspace == null
    error_message = "Existing platform remains opt-in; no workspace by default."
  }
}
run "workspace_enabled_explicitly" {
  command = plan
  variables { enable_databricks = true }
  assert {
    condition     = length(module.databricks) == 1
    error_message = "Workspace requires explicit opt-in."
  }
}
