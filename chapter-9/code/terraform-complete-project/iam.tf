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