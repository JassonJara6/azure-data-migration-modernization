# Workspace-only root. No account provider, identity creation or metastore assignment.
# UC object addresses/security contract match the legacy dev root. Use only ONE
# configuration for dev/databricks-uc.tfstate; launcher rejects account-owned state.
locals {
  bronze_url = "abfss://bronze@${var.storage_account_name}.dfs.core.windows.net/"
  silver_url = "abfss://silver@${var.storage_account_name}.dfs.core.windows.net/admm-silver"
  audit_url  = "abfss://audit@${var.storage_account_name}.dfs.core.windows.net/admm-silver-receipts"
}
data "databricks_current_metastore" "assigned" {
  provider = databricks.workspace
  lifecycle {
    postcondition {
      condition     = self.id == var.metastore_id
      error_message = "Existing workspace metastore must match the explicitly selected metastore. No reassignment is allowed."
    }
  }
}
data "databricks_service_principal" "runner" {
  provider       = databricks.workspace
  count          = var.runner_application_id == null ? 0 : 1
  application_id = var.runner_application_id
  lifecycle {
    postcondition {
      condition     = self.active
      error_message = "The runner must already exist and be active in this workspace."
    }
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
  depends_on     = [data.databricks_current_metastore.assigned]
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
  count      = var.runner_application_id == null ? 0 : 1
  provider   = databricks.workspace
  catalog    = databricks_catalog.silver.name
  depends_on = [data.databricks_service_principal.runner]
  principal  = var.runner_application_id
  privileges = ["USE_CATALOG"]
}
resource "databricks_grant" "silver_runner" {
  count      = var.runner_application_id == null ? 0 : 1
  provider   = databricks.workspace
  schema     = databricks_schema.silver.id
  depends_on = [data.databricks_service_principal.runner]
  principal  = var.runner_application_id
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
}
resource "databricks_grant" "inputs_runner" {
  count      = var.runner_application_id == null ? 0 : 1
  provider   = databricks.workspace
  schema     = databricks_schema.inputs.id
  depends_on = [data.databricks_service_principal.runner]
  principal  = var.runner_application_id
  privileges = ["USE_SCHEMA"]
}
resource "databricks_grant" "bronze_runner" {
  count      = var.runner_application_id == null ? 0 : 1
  provider   = databricks.workspace
  volume     = databricks_volume.bronze.id
  depends_on = [data.databricks_service_principal.runner]
  principal  = var.runner_application_id
  privileges = ["READ_VOLUME"]
}
resource "databricks_grant" "receipts_runner" {
  count      = var.runner_application_id == null ? 0 : 1
  provider   = databricks.workspace
  volume     = databricks_volume.receipts.id
  depends_on = [data.databricks_service_principal.runner]
  principal  = var.runner_application_id
  privileges = ["READ_VOLUME"]
}
# The authorized operator OWNS the input volumes/catalog; the Job receives no
# WRITE_VOLUME, CREATE_VOLUME, credential grants, external-location grants, or Azure RBAC.
resource "databricks_cluster_policy" "silver" {
  count      = var.runner_application_id == null ? 0 : 1
  provider   = databricks.workspace
  depends_on = [data.databricks_service_principal.runner]
  name       = "${var.catalog_name}-silver-ephemeral"
  definition = jsonencode(merge({
    spark_version                                 = { type = "fixed", value = "15.4.x-scala2.12" }
    node_type_id                                  = { type = "fixed", value = var.node_type_id }
    num_workers                                   = { type = "fixed", value = 0 }
    data_security_mode                            = { type = "fixed", value = "SINGLE_USER" }
    single_user_name                              = { type = "fixed", value = var.runner_application_id }
    "spark_conf.spark.databricks.cluster.profile" = { type = "fixed", value = "singleNode" }
    "spark_conf.spark.master"                     = { type = "fixed", value = "local[*]" }
    "custom_tags.ResourceClass"                   = { type = "fixed", value = "SingleNode" }
    "azure_attributes.availability"               = { type = "fixed", value = "ON_DEMAND_AZURE" }
    "cluster_type"                                = { type = "fixed", value = "job" }
    }, var.node_type_id == "Standard_D4ds_v4" ? {
    driver_node_type_id                                    = { type = "fixed", value = var.node_type_id }
    "driver_node_type_flexibility.alternate_node_type_ids" = { type = "fixed", value = "" }
    "worker_node_type_flexibility.alternate_node_type_ids" = { type = "fixed", value = "" }
  } : {}))
}
resource "databricks_permissions" "policy_runner" {
  count             = var.runner_application_id == null ? 0 : 1
  provider          = databricks.workspace
  cluster_policy_id = databricks_cluster_policy.silver[0].id
  access_control {
    service_principal_name = var.runner_application_id
    permission_level       = "CAN_USE"
  }
  access_control {
    user_name        = var.operator_user_name
    permission_level = "CAN_USE"
  }
}
output "deployment" {
  value = {
    provisioning_mode     = "workspace"
    created_metastore     = false
    manages_assignment    = false
    catalog               = databricks_catalog.silver.name
    schema                = databricks_schema.silver.name
    bronze_volume         = "/Volumes/${databricks_catalog.silver.name}/inputs/bronze"
    receipt_volume        = "/Volumes/${databricks_catalog.silver.name}/inputs/receipts"
    runner_application_id = var.runner_application_id
    policy_id             = var.runner_application_id == null ? null : databricks_cluster_policy.silver[0].id
    node_type_id          = var.node_type_id
    operator              = var.operator_user_name
    metastore_id          = data.databricks_current_metastore.assigned.id
  }
}
