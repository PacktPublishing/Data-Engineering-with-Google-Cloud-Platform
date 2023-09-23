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
pipeline_name = "ai-pipeline-credit-default-train"
pipeline_root_path = f"gs://{gcs_bucket}/{pipeline_name}"

bigquery_table_id = f"{project_id}.ml_dataset.credit_card_default"
target_column = "default_payment_next_month"
model_name = "cc_default_rf_model.joblib"

@component(packages_to_install=["google-cloud-bigquery","google-cloud-storage","pandas","pyarrow"])
def load_data_from_bigquery(bigquery_table_id: str, output_gcs_bucket: str) -> str:
    from google.cloud import bigquery
    from google.cloud import storage

    project_id = "packt-data-eng-on-gcp"
    output_file = "ai-pipeline-credit-default-train/artefacts/train.csv"

    bq_client = bigquery.Client(project=project_id)
    sql = f"""SELECT limit_balance, education_level, age, default_payment_next_month FROM `{bigquery_table_id}`;"""
    dataframe = (bq_client.query(sql).result().to_dataframe())

    gcs_client = storage.Client(project=project_id)
    bucket = gcs_client.get_bucket(output_gcs_bucket)
    bucket.blob(output_file).upload_from_string(dataframe.to_csv(index=False), 'text/csv')

    return output_file

@component(packages_to_install=["google-cloud-storage","pandas","scikit-learn==0.21.3","fsspec","gcsfs"])
def train_model(gcs_bucket: str, train_file_path: str, target_column: str, n_estimators: int, model_name: str):
    from google.cloud import storage
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn import metrics
    from sklearn.externals import joblib
    import pandas as pd

    dataframe = pd.read_csv(f'gs://{gcs_bucket}/{train_file_path}')
    labels = dataframe[target_column]
    features = dataframe.drop(target_column, axis = 1)
    output_file = f"ai-pipeline-credit-default-train/artefacts/{model_name}"

    print("Features :")
    print(features.head(5))

    print("Labels :")
    print(labels.head(5))

    x_train, x_test, y_train, y_test = train_test_split(features, labels, test_size=0.3)

    random_forest_classifier = RandomForestClassifier(n_estimators=n_estimators)
    random_forest_classifier.fit(x_train,y_train)

    y_pred=random_forest_classifier.predict(x_test)
    print("Simulate Prediction :")
    print(y_pred[:3])

    print("Accuracy:",metrics.accuracy_score(y_test, y_pred))

    joblib.dump(random_forest_classifier, model_name)

    bucket = storage.Client().bucket(gcs_bucket)
    blob = bucket.blob(output_file)
    blob.upload_from_filename(model_name)

    print(f"Model saved in : {output_file}")

@pipeline(
    name=pipeline_name,
    description="An ML pipeline to train credit card default",
    pipeline_root=pipeline_root_path,
)
def pipeline():
    load_data_from_bigquery_task = load_data_from_bigquery(bigquery_table_id, gcs_bucket)
    train_model(gcs_bucket, load_data_from_bigquery_task.output, target_column, 100, model_name)

compiler.Compiler().compile(
    pipeline_func=pipeline, package_path="{pipeline_name}.json"
)

api_client = AIPlatformClient(project_id=project_id, region=region)

response = api_client.create_run_from_job_spec(
    job_spec_path="{pipeline_name}.json",
    pipeline_root=pipeline_root_path
)
