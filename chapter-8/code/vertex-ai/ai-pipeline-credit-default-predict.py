from typing import NamedTuple

from kfp import dsl
from kfp.v2 import compiler
from kfp.v2.dsl import component
from kfp.v2.google.client import AIPlatformClient

project_id = "aw-general-dev"
region = "us-central1"
gcs_bucket = "aw-general-dev"
pipeline_root_path = f"gs://{gcs_bucket}/aipipeline-root/credit-card-default"

# TODO : Change to your project id
# project_id = "packt-data-eng-on-gcp"
public_table_id = "bigquery-public-data.ml_datasets.credit_card_default"
target_column = "default_payment_next_month"
model_name = "cc_default_rf_model.joblib"


@component(packages_to_install=["google-cloud-bigquery","google-cloud-storage","pandas","pyarrow"])
def load_data_from_bigquery(public_table_id: str, output_gcs_bucket: str) -> str:
    from google.cloud import bigquery
    from google.cloud import storage

    project_id = "aw-general-dev"
    output_file = "aipipeline-root/credit-card-default/artefacts/predict.csv"

    bq_client = bigquery.Client(project=project_id)
    sql = f"""SELECT limit_balance, education_level, age FROM `{public_table_id}` LIMIT 10;"""
    dataframe = (bq_client.query(sql).result().to_dataframe())
    
    gcs_client = storage.Client(project=project_id)
    bucket = gcs_client.get_bucket('aw-general-dev')
    bucket.blob(output_file).upload_from_string(dataframe.to_csv(index=False), 'text/csv')

    return output_file

@component(packages_to_install=["google-cloud-storage","pandas","scikit-learn==0.21.3","fsspec","gcsfs"])
def predict_batch(gcs_bucket: str, predict_file_path: str, model_path: str, output_path: str):
    from sklearn.externals import joblib
    from google.cloud import storage
    import pandas as pd

    project_id = "aw-general-dev"
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
    


@dsl.pipeline(
    name="credit-default-ml-pipeline-predict",
    description="An ML pipeline to predict credit card default",
    pipeline_root=pipeline_root_path,
)
def pipeline():
    load_data_from_bigquery_task = load_data_from_bigquery(public_table_id, gcs_bucket)
    predict_batch(gcs_bucket, 
    load_data_from_bigquery_task.output, 
    "aipipeline-root/credit-card-default/artefacts/cc_default_rf_model.joblib",
    "aipipeline-root/credit-card-default/artefacts/prediction.csv" )

compiler.Compiler().compile(
    pipeline_func=pipeline, package_path="credit_ml_pipeline_predict.json"
)

api_client = AIPlatformClient(project_id=project_id, region=region)

response = api_client.create_run_from_job_spec(
    job_spec_path="credit_ml_pipeline_predict.json", 
    pipeline_root=pipeline_root_path,
    service_account="service-general@aw-general-dev.iam.gserviceaccount.com"
)

