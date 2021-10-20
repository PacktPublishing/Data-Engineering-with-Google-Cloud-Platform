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
    output_file = "aipipeline-root/credit-card-default/artefacts/train.csv"

    bq_client = bigquery.Client(project=project_id)
    sql = f"""SELECT limit_balance, education_level, age, default_payment_next_month FROM `{public_table_id}` LIMIT 1000;"""
    dataframe = (bq_client.query(sql).result().to_dataframe())
    
    gcs_client = storage.Client(project=project_id)
    bucket = gcs_client.get_bucket('aw-general-dev')
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

    project_id = "aw-general-dev"

    dataframe = pd.read_csv(f'gs://{gcs_bucket}/{train_file_path}')
    labels = dataframe[target_column]
    features = dataframe.drop(target_column, axis = 1)
    output_file = f"aipipeline-root/credit-card-default/artefacts/{model_name}"

    print("Features :")
    print(features.head(5))

    print("Labels :")
    print(labels.head(5))

    X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.3) 
    
    random_forest_classifier = RandomForestClassifier(n_estimators=n_estimators)
    random_forest_classifier.fit(X_train,y_train)
    
    y_pred=random_forest_classifier.predict(X_test)
    print("Simulate Prediction :")
    print(y_pred[:3])

    print("Accuracy:",metrics.accuracy_score(y_test, y_pred))

    joblib.dump(random_forest_classifier, model_name)

    bucket = storage.Client().bucket(gcs_bucket)
    blob = bucket.blob(output_file)
    blob.upload_from_filename(model_name)
    
    print(f"Model saved in : {output_file}")


@dsl.pipeline(
    name="credit-default-ml-pipeline-train",
    description="An ML pipeline to train credit card default",
    pipeline_root=pipeline_root_path,
)
def pipeline():
    load_data_from_bigquery_task = load_data_from_bigquery(public_table_id, gcs_bucket)
    train_model(gcs_bucket, load_data_from_bigquery_task.output, target_column, 100, model_name)

compiler.Compiler().compile(
    pipeline_func=pipeline, package_path="credit_ml_pipeline.json"
)

api_client = AIPlatformClient(project_id=project_id, region=region)

response = api_client.create_run_from_job_spec(
    job_spec_path="credit_ml_pipeline.json", 
    pipeline_root=pipeline_root_path,
    service_account="service-general@aw-general-dev.iam.gserviceaccount.com",
    enable_caching=False
)
