# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
