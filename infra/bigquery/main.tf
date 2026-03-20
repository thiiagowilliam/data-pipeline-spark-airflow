locals {
  contract_files = fileset("${path.root}/../contracts", "*.json")

  schemas = {
    for f in local.contract_files :
    replace(f, ".json", "") => jsondecode(file("${path.root}/../contracts/${f}"))
  }

  datasets = {
    bronze = "bronze"
    silver = "silver"
    gold   = "gold"
  }

  bigquery_schemas = {
    for name, cfg in local.schemas :
    name => [
      for field in cfg.fields : {
        name        = field.name
        type        = upper(field.type)
        mode        = field.nullable ? "NULLABLE" : "REQUIRED"
        description = try(field.metadata.description, null)
      }
    ]
  }
}

resource "google_bigquery_dataset" "this" {
  for_each      = local.datasets
  project       = var.project_id
  dataset_id    = each.value
  friendly_name = "${each.value}_layer"
  description   = "Camada ${each.value} gerenciada via Terraform"
  location      = var.location

  labels = {
    env     = var.env
    managed = "terraform"
    layer   = each.value
  }

  default_encryption_configuration {
    kms_key_name = var.kms_key
  }
}

resource "google_bigquery_table" "this" {
  for_each   = local.schemas
  project    = var.project_id
  dataset_id = google_bigquery_dataset.this["bronze"].dataset_id
  table_id   = each.key

  description = "Tabela ${each.key} gerenciada via Data Contract"
  schema      = jsonencode(local.bigquery_schemas[each.key])

  deletion_protection = var.env == "prod"

  lifecycle {
    ignore_changes = [encryption_configuration]
  }

  labels = {
    env   = var.env
    layer = "bronze"
  }
}