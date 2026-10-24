variable "workspace_host" {
  type        = string
  description = "Workspace URL from platform Terraform output."
}
variable "workspace_resource_id" {
  type        = string
  description = "Azure ARM resource ID from platform output."
}
variable "workspace_id" {
  type        = string
  description = "Databricks numeric workspace ID from platform output."
}
variable "access_connector_id" {
  type        = string
  description = "Existing newly approved Access Connector ARM ID."
}
variable "storage_account_name" {
  type        = string
  description = "Existing ADLS account; never create/recreate it."
}
variable "operator_user_name" {
  type        = string
  description = "Verified workspace operator UPN; trusted receipt publisher/owner."
}
variable "metastore_id" {
  type        = string
  description = "Required metastore already assigned to the workspace; never created or reassigned."
  validation {
    condition     = var.metastore_id != null && can(regex("^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$", var.metastore_id))
    error_message = "An explicit existing metastore UUID is required."
  }
}
variable "catalog_name" {
  type        = string
  description = "New dedicated catalog; do not take over an existing catalog."
  default     = "admm_dev"
}
variable "node_type_id" {
  type    = string
  default = "Standard_D4ds_v5"
  validation {
    condition     = contains(["Standard_D4ds_v5", "Standard_D4as_v5"], var.node_type_id)
    error_message = "Use an approved four-vCPU dev SKU; larger sizes require a reviewed policy change."
  }
}
variable "workspace_profile" {
  type        = string
  default     = "admm-workspace"
  description = "Existing Databricks CLI workspace OAuth profile. No PAT."
}
variable "runner_application_id" {
  type        = string
  default     = null
  description = "Existing workspace service principal application UUID; null provisions UC foundation only."
  validation {
    condition     = var.runner_application_id == null ? true : can(regex("^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$", var.runner_application_id))
    error_message = "Use an existing service principal UUID, never a personal user identity."
  }
}
