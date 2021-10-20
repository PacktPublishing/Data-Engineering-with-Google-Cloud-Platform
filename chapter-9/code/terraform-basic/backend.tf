terraform {
  backend "gcs" {
    bucket = "packt-data-eng-on-gcp-data-bucket"  # without gs://
    prefix = "terraform-backend-basic"
  }
}
