# Shared regional objects are reused, never owned/destroyed by default.
resource "databricks_metastore" "regional" {
  provider = databricks.account
  count    = var.create_metastore ? 1 : 0
  name     = "admm-eastus"
  region   = var.region
  lifecycle { prevent_destroy = true }
  owner = var.operator_user_name
}
locals {
  metastore_id = var.create_metastore ? databricks_metastore.regional[0].id : var.metastore_id
  bronze_url   = "abfss://bronze@${var.storage_account_name}.dfs.core.windows.net/"
  silver_url   = "abfss://silver@${var.storage_account_name}.dfs.core.windows.net/admm-silver"
  audit_url    = "abfss://audit@${var.storage_account_name}.dfs.core.windows.net/admm-silver-receipts"
}
resource "databricks_metastore_assignment" "workspace" {
  provider     = databricks.account
  count        = var.manage_assignment ? 1 : 0
  workspace_id = var.workspace_id
  metastore_id = local.metastore_id
  lifecycle {
    prevent_destroy = true
    precondition {
      condition     = var.create_metastore != (var.metastore_id != null)
      error_message = "Choose an existing regional metastore OR explicitly create one, never both/neither."
    }
  }
}
data "databricks_user" "operator" {
  provider  = databricks.account
  user_name = var.operator_user_name
}
resource "databricks_service_principal" "runner" {
  provider     = databricks.account
  display_name = "${var.catalog_name}-silver-job"
}
resource "databricks_mws_permission_assignment" "runner" {
  provider     = databricks.account
  workspace_id = var.workspace_id
  principal_id = databricks_service_principal.runner.id
  permissions  = ["USER"]
}
resource "databricks_mws_permission_assignment" "operator" {
  provider     = databricks.account
  workspace_id = var.workspace_id
  principal_id = data.databricks_user.operator.id
  permissions  = ["ADMIN"]
}
resource "databricks_access_control_rule_set" "run_as" {
  provider = databricks.account
  name     = "accounts/${var.account_id}/servicePrincipals/${databricks_service_principal.runner.application_id}/ruleSets/default"
  grant_rules {
    role       = "roles/servicePrincipal.user"
    principals = [data.databricks_user.operator.acl_principal_id]
  }
}
resource "databricks_storage_credential" "storage" {
  provider = databricks.workspace
  for_each = toset(["bronze", "silver", "audit"])
  name     = "${var.catalog_name}_${each.key}"
  azure_managed_identity { access_connector_id = var.access_connector_id }
  read_only      = each.key == "bronze"
  owner          = var.operator_user_name
  isolation_mode = "ISOLATION_MODE_ISOLATED"
  depends_on     = [databricks_metastore_assignment.workspace, databricks_mws_permission_assignment.operator]
  lifecycle { prevent_destroy = true }
}
resource "databricks_external_location" "storage" {
  provider        = databricks.workspace
  for_each        = { bronze = local.bronze_url, silver = local.silver_url, audit = local.audit_url }
  name            = "${var.catalog_name}_${each.key}"
  url             = each.value
  credential_name = databricks_storage_credential.storage[each.key].name
  owner           = var.operator_user_name
  read_only       = each.key == "bronze"
  isolation_mode  = "ISOLATION_MODE_ISOLATED"
  skip_validation = false
  lifecycle { prevent_destroy = true }
}
resource "databricks_catalog" "silver" {
  provider       = databricks.workspace
  name           = var.catalog_name
  owner          = var.operator_user_name
  storage_root   = "${local.silver_url}/managed"
  isolation_mode = "ISOLATED"
  depends_on     = [databricks_external_location.storage]
  lifecycle { prevent_destroy = true }
}
resource "databricks_schema" "silver" {
  provider     = databricks.workspace
  catalog_name = databricks_catalog.silver.name
  name         = "silver"
  owner        = var.operator_user_name
  lifecycle { prevent_destroy = true }
}
resource "databricks_schema" "inputs" {
  provider     = databricks.workspace
  catalog_name = databricks_catalog.silver.name
  name         = "inputs"
  owner        = var.operator_user_name
  lifecycle { prevent_destroy = true }
}
resource "databricks_volume" "bronze" {
  provider         = databricks.workspace
  catalog_name     = databricks_catalog.silver.name
  schema_name      = databricks_schema.inputs.name
  name             = "bronze"
  volume_type      = "EXTERNAL"
  storage_location = local.bronze_url
  owner            = var.operator_user_name
  depends_on       = [databricks_external_location.storage]
  lifecycle { prevent_destroy = true }
}
resource "databricks_volume" "receipts" {
  provider         = databricks.workspace
  catalog_name     = databricks_catalog.silver.name
  schema_name      = databricks_schema.inputs.name
  name             = "receipts"
  volume_type      = "EXTERNAL"
  storage_location = local.audit_url
  owner            = var.operator_user_name
  depends_on       = [databricks_external_location.storage]
  lifecycle { prevent_destroy = true }
}
resource "databricks_grant" "catalog_runner" {
  provider   = databricks.workspace
  catalog    = databricks_catalog.silver.name
  principal  = databricks_service_principal.runner.application_id
  privileges = ["USE_CATALOG"]
}
resource "databricks_grant" "silver_runner" {
  provider   = databricks.workspace
  schema     = databricks_schema.silver.id
  principal  = databricks_service_principal.runner.application_id
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
}
resource "databricks_grant" "inputs_runner" {
  provider   = databricks.workspace
  schema     = databricks_schema.inputs.id
  principal  = databricks_service_principal.runner.application_id
  privileges = ["USE_SCHEMA"]
}
resource "databricks_grant" "bronze_runner" {
  provider   = databricks.workspace
  volume     = databricks_volume.bronze.id
  principal  = databricks_service_principal.runner.application_id
  privileges = ["READ_VOLUME"]
}
resource "databricks_grant" "receipts_runner" {
  provider   = databricks.workspace
  volume     = databricks_volume.receipts.id
  principal  = databricks_service_principal.runner.application_id
  privileges = ["READ_VOLUME"]
}
# The authorized operator OWNS the input volumes/catalog; the Job receives no
# WRITE_VOLUME, CREATE_VOLUME, credential grants, external-location grants, or Azure RBAC.
resource "databricks_cluster_policy" "silver" {
  provider = databricks.workspace
  name     = "${var.catalog_name}-silver-ephemeral"
  definition = jsonencode({
    spark_version                                 = { type = "fixed", value = "15.4.x-scala2.12" }
    node_type_id                                  = { type = "fixed", value = var.node_type_id }
    num_workers                                   = { type = "fixed", value = 0 }
    data_security_mode                            = { type = "fixed", value = "SINGLE_USER" }
    single_user_name                              = { type = "fixed", value = databricks_service_principal.runner.application_id }
    "spark_conf.spark.databricks.cluster.profile" = { type = "fixed", value = "singleNode" }
    "spark_conf.spark.master"                     = { type = "fixed", value = "local[*]" }
    "custom_tags.ResourceClass"                   = { type = "fixed", value = "SingleNode" }
    "azure_attributes.availability"               = { type = "fixed", value = "ON_DEMAND_AZURE" }
    "cluster_type"                                = { type = "fixed", value = "job" }
  })
  depends_on = [databricks_mws_permission_assignment.runner]
}
resource "databricks_permissions" "policy_runner" {
  provider          = databricks.workspace
  cluster_policy_id = databricks_cluster_policy.silver.id
  access_control {
    service_principal_name = databricks_service_principal.runner.application_id
    permission_level       = "CAN_USE"
  }
  access_control {
    user_name        = var.operator_user_name
    permission_level = "CAN_USE"
  }
}
output "deployment" {
  value = {
    created_metastore     = var.create_metastore
    manages_assignment    = var.manage_assignment
    catalog               = databricks_catalog.silver.name
    schema                = databricks_schema.silver.name
    bronze_volume         = "/Volumes/${databricks_catalog.silver.name}/inputs/bronze"
    receipt_volume        = "/Volumes/${databricks_catalog.silver.name}/inputs/receipts"
    runner_application_id = databricks_service_principal.runner.application_id
    policy_id             = databricks_cluster_policy.silver.id
    node_type_id          = var.node_type_id
    operator              = var.operator_user_name
    metastore_id          = local.metastore_id
  }
}

resource "databricks_entitlements" "runner" {
  provider                   = databricks.workspace
  service_principal_id       = databricks_service_principal.runner.id
  workspace_access           = true
  allow_cluster_create       = false
  allow_instance_pool_create = false
  databricks_sql_access      = false
  depends_on                 = [databricks_mws_permission_assignment.runner]
}
