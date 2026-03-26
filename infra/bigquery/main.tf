locals {
  contract_files = fileset("${path.root}/../contracts", "*.json")
  schemas = {
    for f in local.contract_files :
    replace(f, ".json", "") => jsondecode(file("${path.root}/../contracts/${f}"))
  }

  domains = toset(keys(local.schemas))
  layers_datasets = ["bronze", "silver", "gold"]
  layers_contract_tables = ["silver", "gold"]
  
  type_mapping = {
    "integer"       = "INTEGER"
    "integertype"   = "INTEGER"
    "long"          = "INT64"
    "longtype"      = "INT64"
    "string"        = "STRING"
    "stringtype"    = "STRING"
    "double"        = "FLOAT64"
    "doubletype"    = "FLOAT64"
    "boolean"       = "BOOLEAN"
    "booleantype"   = "BOOLEAN"
    "date"          = "DATE"
    "datetype"      = "DATE"
    "timestamp"     = "TIMESTAMP"
    "timestamptype" = "TIMESTAMP"
  }

  bigquery_schemas = {
    for domain, cfg in local.schemas :
    domain => concat(
      [
        for field in lookup(cfg, "fields", []) : {
          name        = field.name
          type        = lookup(local.type_mapping, lower(field.type), "STRING")
          mode        = try(field.not_null ? "REQUIRED" : "NULLABLE", "NULLABLE")
          description = try(field.description, "")
        } if field.name != "dt_ingest" # <-- Ajustado para dt_ingest
      ],
      [
        {
          name        = "dt_ingest" # <-- Ajustado para alinhar com o PySpark
          type        = "DATE"
          mode        = "REQUIRED"
          description = "Data lógica de ingestão do registro (Chave de Partição)"
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
          description = "Carimbo de tempo exato da inserção física no BigQuery"
        }
      ]
    )
  }

  layer_domain_tables = flatten([
    for layer in local.layers_contract_tables : [
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
  for_each      = toset(local.layers_datasets)
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
  for_each            = local.tables_map
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.layer_datasets[each.value.dataset_id].dataset_id
  table_id            = each.value.table_id
  description         = "Tabela de ${each.value.table_id} na camada ${each.value.dataset_id} (schema do contrato)"
  schema              = jsonencode(each.value.schema)
  deletion_protection = var.env == "prod"

  time_partitioning {
    type  = "DAY"
    field = "dt_ingest"
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