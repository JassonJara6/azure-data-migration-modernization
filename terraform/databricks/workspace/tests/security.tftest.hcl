mock_provider "databricks" {
  alias = "workspace"
  mock_data "databricks_current_metastore" {
    defaults = { id = "b67792ab-c53b-48b4-ae57-fc45b869ed7a" }
  }
  mock_data "databricks_service_principal" {
    defaults = { id = "12345", application_id = "00000000-0000-0000-0000-000000000003", active = true }
  }
}
variables {
  workspace_id          = "7405617270738925"
  workspace_host        = "https://adb-7405617270738925.1.azuredatabricks.net"
  workspace_resource_id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Databricks/workspaces/dbw-admm-dev-f6d99ba4"
  access_connector_id   = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-admm-dev-platform/providers/Microsoft.Databricks/accessConnectors/ac-existing-synthetic"
  storage_account_name  = "stadmmdevdlf6d99ba4"
  operator_user_name    = "operator@example.invalid"
  metastore_id          = "b67792ab-c53b-48b4-ae57-fc45b869ed7a"
}
run "foundation_without_account_id_or_runner" {
  command = plan
  assert {
    condition     = length(databricks_cluster_policy.silver) == 0 && length(databricks_grant.bronze_runner) == 0 && length(data.databricks_service_principal.runner) == 0
    error_message = "Foundation does not create a runner, cluster, policy or input grants."
  }
  assert {
    condition     = databricks_storage_credential.storage["bronze"].read_only && databricks_external_location.storage["bronze"].read_only
    error_message = "Bronze credential/location remain read-only."
  }
  assert {
    condition     = output.deployment.created_metastore == false && output.deployment.manages_assignment == false && output.deployment.runner_application_id == null
    error_message = "An existing metastore is reused without personal run-as fallback."
  }
}
run "existing_runner_receives_read_only_input_permissions" {
  command = plan
  variables {
    runner_application_id = "00000000-0000-0000-0000-000000000003"
  }
  assert {
    condition     = toset(databricks_grant.bronze_runner[0].privileges) == toset(["READ_VOLUME"]) && toset(databricks_grant.receipts_runner[0].privileges) == toset(["READ_VOLUME"])
    error_message = "Existing runner never receives input writes."
  }
  assert {
    condition     = length(databricks_cluster_policy.silver) == 1 && output.deployment.runner_application_id == var.runner_application_id
    error_message = "Only a policy for an existing service principal is configured; no permanent cluster."
  }
}
run "wrong_metastore_is_blocked" {
  command = plan
  variables {
    metastore_id = "00000000-0000-0000-0000-000000000099"
  }
  expect_failures = [data.databricks_current_metastore.assigned]
}
run "no_assumed_metastore" {
  command = plan
  variables {
    metastore_id = null
  }
  expect_failures = [var.metastore_id]
}
run "v4_single_node_policy_disables_all_fallback" {
  command = plan
  variables {
    runner_application_id = "00000000-0000-0000-0000-000000000003"
    node_type_id          = "Standard_D4ds_v4"
  }
  assert {
    condition = (
      jsondecode(databricks_cluster_policy.silver[0].definition).node_type_id.value == "Standard_D4ds_v4" &&
      jsondecode(databricks_cluster_policy.silver[0].definition).driver_node_type_id.value == "Standard_D4ds_v4" &&
      jsondecode(databricks_cluster_policy.silver[0].definition).num_workers.value == 0 &&
      jsondecode(databricks_cluster_policy.silver[0].definition)["driver_node_type_flexibility.alternate_node_type_ids"].value == "" &&
      jsondecode(databricks_cluster_policy.silver[0].definition)["worker_node_type_flexibility.alternate_node_type_ids"].value == "" &&
      output.deployment.node_type_id == "Standard_D4ds_v4"
    )
    error_message = "V4 uses one fixed driver, zero workers, no implicit alternate nodes or new compute resources."
  }
}
