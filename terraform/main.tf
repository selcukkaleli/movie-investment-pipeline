terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = var.region
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = var.region
}

resource "google_bigquery_dataset" "analytics_staging" {
  dataset_id = "analytics_staging"
  location   = var.region
}

resource "google_bigquery_dataset" "analytics_intermediate" {
  dataset_id = "analytics_intermediate"
  location   = var.region
}

resource "google_bigquery_dataset" "analytics_marts" {
  dataset_id = "analytics_marts"
  location   = var.region
}