provider "google" {
  project = var.project_id
}

resource "google_bigquery_dataset" "new_dataset" {
  project    = var.project_id
  dataset_id = "dataset_from_terraform"
}