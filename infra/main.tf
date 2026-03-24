module "bigquery" {
    source = "./bigquery"
    project_id = var.project_id
    location = "US"
    env = "dev"
}