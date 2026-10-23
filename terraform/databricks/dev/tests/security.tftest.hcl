mock_provider "databricks" {
  alias = "account"
  mock_data "databricks_user" {
    defaults = { id = "12345", acl_principal_id = "users/12345" }
  }
  mock_resource "databricks_service_principal" {
    defaults = { id = "67890", application_id = "00000000-0000-0000-0000-000000000003" }
  }
}
mock_provider "databricks" { alias = "workspace" }
variables {
  account_id            = "00000000-0000-0000-0000-000000000001"
  workspace_id          = "123456789"
  workspace_host        = "https://adb-123456789.1.azuredatabricks.net"
  workspace_resource_id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Databricks/workspaces/dbw-admm-dev-f6d99ba4"
  access_connector_id   = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Databricks/accessConnectors/ac-admm-dev-silver"
  storage_account_name  = "stadmmdevdlf6d99ba4"
  operator_user_name    = "operator@example.invalid"
  metastore_id          = "00000000-0000-0000-0000-000000000002"
}
run "reuse_and_least_privilege" {
  command = plan
  assert {
    condition     = length(databricks_metastore.regional) == 0 && databricks_metastore_assignment.workspace[0].metastore_id == var.metastore_id
    error_message = "Reuse the existing regional metastore."
  }
  assert {
    condition     = databricks_storage_credential.storage["bronze"].read_only && databricks_external_location.storage["bronze"].read_only
    error_message = "Both credential and location must be read-only."
  }
  assert {
    condition     = toset(databricks_grant.receipts_runner.privileges) == toset(["READ_VOLUME"]) && toset(databricks_grant.bronze_runner.privileges) == toset(["READ_VOLUME"])
    error_message = "Runner cannot write inputs."
  }
  assert {
    condition     = databricks_volume.receipts.storage_location == "abfss://audit@stadmmdevdlf6d99ba4.dfs.core.windows.net/admm-silver-receipts"
    error_message = "Receipts use the existing audit container."
  }

}
run "preserve_automatic_assignment" {
  command = plan
  variables { manage_assignment = false }
  assert {
    condition     = length(databricks_metastore_assignment.workspace) == 0
    error_message = "Do not take over an existing assignment."
  }
}
run "explicit_create_only_when_approved" {
  command = plan
  variables {
    create_metastore = true
    metastore_id     = null
  }
  assert {
    condition     = length(databricks_metastore.regional) == 1
    error_message = "Explicitly approved fresh regional metastore is supported."
  }
}
run "missing_regional_choice_is_blocked" {
  command = plan
  variables {
    create_metastore  = false
    metastore_id      = null
    manage_assignment = false
  }
  expect_failures = [var.create_metastore]
}
