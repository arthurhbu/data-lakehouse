resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.aviation.name
  name         = "bronze"
  comment      = "Schema for bronze layer tables"
}

resource "databricks_volume" "landing" {
  catalog_name = databricks_catalog.aviation.name
  schema_name  = databricks_schema.bronze.name
  name         = "landing"
  volume_type  = "MANAGED"
  comment      = "Arquivos de entrada de ingestão Aviation"
}