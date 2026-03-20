# infra/bigquery/policy_tags.tf

resource "google_data_catalog_taxonomy" "lgpd_taxonomy" {
  provider     = google-beta
  project      = var.project_id
  region       = var.location
  display_name = "Governança de Dados (LGPD)"
  description  = "Taxonomia para classificação de dados sensíveis sob a LGPD."
}

resource "google_data_catalog_policy_tag" "pii_tag" {
  provider     = google-beta
  taxonomy     = google_data_catalog_taxonomy.lgpd_taxonomy.id
  display_name = "PII - Informação Pessoalmente Identificável"
  description  = "Marca colunas que contém PII, mesmo que pseudonimizadas (hash, etc)."
}
