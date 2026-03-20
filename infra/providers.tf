terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.50"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.50"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.location
}

provider "google-beta" {
  project = var.project_id
  region  = var.location
}