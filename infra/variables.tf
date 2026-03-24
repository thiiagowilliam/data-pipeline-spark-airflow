variable "project_id" {
  default = "resolve-ai-407701"
}


variable "region" {
  description = "GCP Region (e.g. us-central1)"
  type        = string
  default     = "us-central1"   
}

variable "location" {
  description = "BigQuery multi-region or regional location (e.g. US, EU, us-central1)"
  type        = string
  default     = "US" 
}

variable "env" { 
  type = string
  default = "dev"
}
variable "kms_key" { 
  type = string
  default = null 
 }