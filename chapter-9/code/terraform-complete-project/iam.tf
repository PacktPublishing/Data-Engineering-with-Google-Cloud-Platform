resource "google_service_account" "bq-service_account" {
  depends_on = [
    google_project.packt-data-eng-on-gcp-dev
  ]

  account_id   = "sa-bigquery"
  display_name = "Service Account for accessing BigQuery"
  project      = google_project.packt-data-eng-on-gcp-dev.project_id
}


resource "google_project_iam_member" "bq-sa-role" {
  depends_on = [
    google_service_account.bq-service_account
  ]

  role         = "roles/bigquery.dataOwner"
  member       = "serviceAccount:${google_service_account.bq-service_account.email}"
  project      = google_project.packt-data-eng-on-gcp-dev.project_id
}