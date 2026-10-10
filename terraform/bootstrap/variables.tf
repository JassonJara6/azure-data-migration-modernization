variable "subscription_id" {
  description = "Azure subscription ID selected through Azure CLI. Supply it at runtime; do not commit it."
  type        = string
  sensitive   = true
  nullable    = false

  validation {
    condition     = can(regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", var.subscription_id))
    error_message = "subscription_id must be a valid Azure subscription GUID."
  }
}

variable "location" {
  description = "Azure region for the remote-state resources."
  type        = string
  default     = "eastus"
}

variable "project" {
  description = "Short project identifier used in resource names and tags."
  type        = string
  default     = "admm"
}

variable "environment" {
  description = "Deployment environment identifier. Only dev is deployed in this portfolio."
  type        = string
  default     = "dev"
}
