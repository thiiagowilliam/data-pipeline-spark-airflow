module "bigquery" {
    source = "./bigquery"
    project_id = var.project_id
    expiration_ms = ""
    location = "US"
    kms_key = ""
    dataset_id = ""
    friendly = ""
    kms_key_ring = ""
    env = ""
}