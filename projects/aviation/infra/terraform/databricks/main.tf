terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "databricks" {
  profile = "arthur-datalake"
}

data "databricks_catalog" "workspace" {
  name = "workspace"
}

resource "databricks_catalog" "aviation" {
  name    = "elysium_aviation"
  comment = "Elysium Aviation Intelligence Lakehouse"

  lifecycle {
    # O Default Storage do catalogo importado e administrado pelo Databricks.
    ignore_changes  = [storage_root]
    prevent_destroy = true
  }
}

output "workspace_catalog" {
  value = data.databricks_catalog.workspace.name
}
