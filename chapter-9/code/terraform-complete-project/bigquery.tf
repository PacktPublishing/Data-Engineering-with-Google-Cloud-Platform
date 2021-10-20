# Enable BigQuery service
resource "google_project_service" "bigquery_service" {
  project = google_project.packt-data-eng-on-gcp-dev.project_id
  service = "bigquery.googleapis.com"
}

# Create the BigQuery datasets
resource "google_bigquery_dataset" "stg_dataset" {
  depends_on = [
    google_project.packt-data-eng-on-gcp-dev,
    google_project_service.bigquery_service
  ]

  project    = google_project.packt-data-eng-on-gcp-dev.project_id
  dataset_id = "stg_dataset"
  default_table_expiration_ms = 3600000
  default_partition_expiration_ms = 3600000
}

resource "google_bigquery_dataset" "dwh_dataset" {
  depends_on = [
    google_project.packt-data-eng-on-gcp-dev,
    google_project_service.bigquery_service
  ]

  project    = google_project.packt-data-eng-on-gcp-dev.project_id
  dataset_id = "dwh_dataset"
  default_table_expiration_ms = 3600000
  default_partition_expiration_ms = 3600000
}

resource "google_bigquery_dataset" "datamart_dataset" {
  depends_on = [
    google_project.packt-data-eng-on-gcp-dev,
    google_project_service.bigquery_service
  ]

  project    = google_project.packt-data-eng-on-gcp-dev.project_id
  dataset_id = "datamart_dataset"
  default_table_expiration_ms = 3600000
  default_partition_expiration_ms = 3600000
}
