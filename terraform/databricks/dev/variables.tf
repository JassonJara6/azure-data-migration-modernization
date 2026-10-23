variable "account_id" {
  type        = string
  description = "Databricks ACCOUNT ID (not Azure subscription or tenant ID)."
}
variable "account_profile" {
  type        = string
  description = "Existing Databricks CLI account OAuth profile. No PAT."
  default     = "admm-account"
}
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
  description = "Authorized Databricks account user UPN; trusted receipt publisher/owner."
}
variable "metastore_id" {
  type        = string
  description = "Regional metastore discovered via account API; null only for explicit creation."
  default     = null
}
variable "catalog_name" {
  type        = string
  description = "New dedicated catalog; do not take over an existing catalog."
  default     = "admm_dev"
}
variable "create_metastore" {
  description = "Explicit account-admin approval, only if no East US metastore exists."
  type        = bool
  default     = false
  validation {
    condition     = var.create_metastore != (var.metastore_id != null)
    error_message = "Select an existing regional metastore OR explicitly create one."
  }
}
variable "manage_assignment" {
  description = "False when workspace is already assigned; never replace an assignment."
  type        = bool
  default     = true
}
variable "region" {
  type    = string
  default = "eastus"
  validation {
    condition     = var.region == "eastus"
    error_message = "This root supports East US only."
  }
}
variable "node_type_id" {
  type    = string
  default = "Standard_D4ds_v5"
  validation {
    condition     = contains(["Standard_D4ds_v5", "Standard_D4as_v5"], var.node_type_id)
    error_message = "Use an approved four-vCPU dev SKU; larger sizes require a reviewed policy change."
  }
}
