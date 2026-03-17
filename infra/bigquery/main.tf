locals {
  schemas = jsondecode(file("${path.root}/../contracts/schemas.json"))
  unique_datasets = distinct([for k, v in local.schemas : v.dataset])
}

resource "google_bigquery_dataset" "this" {
  for_each   = toset(local.unique_datasets) 
  project    = var.project_id
  dataset_id = each.key

  friendly_name = "${each.key}_layer"
  description   = "Camada ${each.key} gerenciada via Data Contract"
  location      = var.location
  labels = {
    env      = var.env
    managed  = "terraform"
    layer    = each.key
  }
  default_encryption_configuration {
    kms_key_name = var.kms_key
  }
}

resource "google_bigquery_table" "this" {
  project    = var.project_id
  for_each   = local.schemas
  
  dataset_id = google_bigquery_dataset.this[each.value.dataset].dataset_id
  table_id   = try(each.value.table_name, each.key)
  description = try(each.value.description, "Tabela da camada ${each.value.dataset}")

  schema     = jsonencode(each.value.columns)
  clustering = try(each.value.cluster_fields, [])

  deletion_protection = var.env == "prod" ? true : false

  dynamic "time_partitioning" {
    for_each = try(each.value.partition_field, null) != null ? [1] : []
    content {
      type  = "DAY"
      field = each.value.partition_field
    }
  }
  labels = {
    env   = var.env
    layer = each.value.dataset
  }

  lifecycle {
    ignore_changes = [encryption_configuration]
  }
}