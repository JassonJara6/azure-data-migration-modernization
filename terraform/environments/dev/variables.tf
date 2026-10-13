variable "subscription_id" {
  description = "Azure subscription selected through Azure CLI. Supply it at runtime; do not commit it."
  type        = string
  sensitive   = true
  nullable    = false

  validation {
    condition     = can(regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", var.subscription_id))
    error_message = "subscription_id must be a valid Azure subscription GUID."
  }
}

variable "location" {
  description = "Azure region for the dev platform."
  type        = string
  default     = "eastus"
}

variable "name_prefix" {
  description = "Short prefix used in future platform resource names."
  type        = string
  default     = "admm"
}

variable "environment" {
  description = "Environment name. This root configuration represents dev only."
  type        = string
  default     = "dev"

  validation {
    condition     = var.environment == "dev"
    error_message = "terraform/environments/dev only supports the dev environment."
  }
}
