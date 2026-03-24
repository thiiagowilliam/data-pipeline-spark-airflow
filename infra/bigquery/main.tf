locals {
  contract_files = fileset("${path.root}/../contracts", "*.json")
  schemas = {
    for f in local.contract_files :
    replace(f, ".json", "") => jsondecode(file("${path.root}/../contracts/${f}"))
  }

  domains = toset(keys(local.schemas))
  layers  = ["bronze", "silver", "gold"]
  type_mapping = {
    "integer" = "INTEGER"
    "string"  = "STRING"
    "double"  = "FLOAT64"
    "date"    = "DATE"
  }

  bigquery_schemas = {
    for domain, cfg in local.schemas :
    domain => concat(
      [
        for field in lookup(cfg, "fields", []) : {
          name        = field.name
          type        = lookup(local.type_mapping, lower(field.type), "STRING")
          mode        = try(field.nullable ? "NULLABLE" : "REQUIRED", "NULLABLE")
          description = try(field.metadata.description, "")
        } if field.name != "dt_ingestao"
      ],
      [{ 
        name        = "dt_ingestao"
        type        = "DATE"
        mode        = "NULLABLE"
        description = "Data de ingestão do registro"
      },
      { 
        name        = "_bq_run_id"
        type        = "STRING"
        mode        = "NULLABLE"
        description = "ID de execução do job no Airflow/Spark"
      },
      { 
        name        = "_bq_loaded_at"
        type        = "TIMESTAMP"
        mode        = "NULLABLE"
        description = "Carimbo de tempo exato da inserção no BigQuery"
      }]
    )
  }

  layer_domain_tables = flatten([
    for layer in local.layers : [
      for domain in local.domains : {
        key        = "${layer}_${domain}"
        dataset_id = layer
        table_id   = domain
        schema     = local.bigquery_schemas[domain]
      }
    ]
  ])
  tables_map = {
    for t in local.layer_domain_tables : t.key => t
  }
}

resource "google_bigquery_dataset" "layer_datasets" {
  for_each      = toset(local.layers)
  project       = var.project_id
  dataset_id    = each.value
  friendly_name = "Camada ${title(each.value)}"
  description   = "Dataset central para a camada ${each.value} do Data Lakehouse"
  location      = var.location
  labels = {
    env     = var.env
    managed = "terraform"
    layer   = each.value
  }
}

resource "google_bigquery_table" "domain_tables" {
  for_each   = local.tables_map
  project    = var.project_id
  dataset_id = google_bigquery_dataset.layer_datasets[each.value.dataset_id].dataset_id
  table_id   = each.value.table_id
  description = "Tabela de ${each.value.table_id} na camada ${each.value.dataset_id}"
  schema      = jsonencode(each.value.schema)
  deletion_protection = var.env == "prod"

  time_partitioning {
    type  = "DAY"
    field = "dt_ingestao"
  }
  lifecycle {
    ignore_changes = [encryption_configuration]
  }
  labels = {
    env    = var.env
    layer  = each.value.dataset_id
    domain = each.value.table_id
  }
}