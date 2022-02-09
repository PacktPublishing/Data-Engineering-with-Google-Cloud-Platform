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

from kfp.dsl import pipeline
from kfp.v2 import compiler
from kfp.v2.dsl import component
from kfp.v2.google.client import AIPlatformClient

# TODO: Change with your project id and gcs bucket name
project_id = "packt-data-eng-on-gcp"
gcs_bucket = "packt-data-eng-on-gcp-vertex-ai-pipeline"
region = "us-central1"
pipeline_name = "ai-pipeline-credit-default-predict"
pipeline_root_path = f"gs://{gcs_bucket}/{pipeline_name}"

bigquery_table_id = f"{project_id}.ml_dataset.credit_card_default"
target_column = "default_payment_next_month"
model_name = "cc_default_rf_model.joblib"

@component(packages_to_install=["google-cloud-bigquery","google-cloud-storage","pandas","pyarrow"])
def load_data_from_bigquery(bigquery_table_id: str, output_gcs_bucket: str) -> str:
    from google.cloud import bigquery
    from google.cloud import storage

    project_id = "packt-data-eng-on-gcp"
    output_file = "ai-pipeline-credit-default-predict/artefacts/predict.csv"

    bq_client = bigquery.Client(project=project_id)
    sql = f"""SELECT limit_balance, education_level, age FROM `{bigquery_table_id}` LIMIT 10;"""
    dataframe = (bq_client.query(sql).result().to_dataframe())

    gcs_client = storage.Client(project=project_id)
    bucket = gcs_client.get_bucket(output_gcs_bucket)
    bucket.blob(output_file).upload_from_string(dataframe.to_csv(index=False), 'text/csv')

    return output_file

@component(packages_to_install=["google-cloud-storage","pandas","scikit-learn==0.21.3","fsspec","gcsfs"])
def predict_batch(gcs_bucket: str, predict_file_path: str, model_path: str, output_path: str):
    from sklearn.externals import joblib
    from google.cloud import storage
    import pandas as pd

    project_id = "packt-data-eng-on-gcp"
    model_local_uri = "cc_default_rf_model.joblib"
    gcs_client = storage.Client(project=project_id)
    bucket = gcs_client.get_bucket(gcs_bucket)

    # Load predict data from GCS to pandas
    dataframe = pd.read_csv(f'gs://{gcs_bucket}/{predict_file_path}')

    # Load ML model from GCS
    model_file = bucket.blob(model_path)
    model_file.download_to_filename(model_local_uri)
    loaded_model = joblib.load(model_local_uri)

    # Predict
    prediction = loaded_model.predict(dataframe)
    prediction = pd.DataFrame(prediction)

    # Store prediction to GCS
    bucket.blob(output_path).upload_from_string(prediction.to_csv(index=False), 'text/csv')

    print(f"Prediction file path: {output_path}")

@pipeline(
    name="ai-pipeline-credit-default-predict",
    description="An ML pipeline to predict credit card default",
    pipeline_root=pipeline_root_path,
)
def pipeline():
    load_data_from_bigquery_task = load_data_from_bigquery(bigquery_table_id, gcs_bucket)
    predict_batch(gcs_bucket,
    load_data_from_bigquery_task.output,
    "ai-pipeline-credit-default-train/artefacts/cc_default_rf_model.joblib",
    "ai-pipeline-credit-default-predict/artefacts/prediction.csv" )

compiler.Compiler().compile(
    pipeline_func=pipeline, package_path="{pipeline_name}.json"
)

api_client = AIPlatformClient(project_id=project_id, region=region)

response = api_client.create_run_from_job_spec(
    job_spec_path="{pipeline_name}.json",
    pipeline_root=pipeline_root_path
)
