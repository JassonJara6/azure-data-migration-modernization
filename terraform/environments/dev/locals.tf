locals {
  common_tags = {
    environment = var.environment
    managed-by  = "terraform"
    project     = "adventureworks-azure-data-platform"
  }
}
