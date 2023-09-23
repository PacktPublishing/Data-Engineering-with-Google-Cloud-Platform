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

from google.cloud import bigquery
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn import metrics
import joblib
import json

import pandas as pd

# TODO : Change to your project id
project_id = "packt-data-eng-on-gcp"
dataset_table_id = "ml_dataset.credit_card_default"
target_column = "default_payment_next_month"
model_name = "cc_default_rf_model.sav"

def load_data_from_bigquery(dataset_table_id):
    client = bigquery.Client()
    sql = """SELECT 
            limit_balance, 
            education_level, 
            age, 
            default_payment_next_month 
            FROM `{dataset_table_id}`""".format(dataset_table_id=dataset_table_id)

    dataframe = (client.query(sql).result().to_dataframe())

    print("This is our training table from BigQuery")
    print(dataframe.head(5))
    return dataframe

def train_model(dataframe):
    labels = dataframe[target_column]
    features = dataframe.drop(target_column, axis = 1)

    print("Features : {}").format(features.head(5))
    print("Labels : {}").format(labels.head(5))

    x_train, x_test, y_train, y_test = train_test_split(features, labels, test_size=0.3)

    random_forest_classifier = RandomForestClassifier(n_estimators=100)
    random_forest_classifier.fit(x_train,y_train)

    y_pred=random_forest_classifier.predict(x_test)
    print("Simulate Prediction : {}").format(y_pred[:3])

    print("Accuracy:",metrics.accuracy_score(y_test, y_pred))

    joblib.dump(random_forest_classifier, model_name)

    return random_forest_classifier

def predict_batch(dataset_table_id):
    loaded_model = joblib.load(model_name)

    client = bigquery.Client()
    sql = """SELECT 
                limit_balance, 
                education_level, 
                age 
                FROM `{dataset_table_id}` 
                LIMIT 10;""".format(dataset_table_id=dataset_table_id)

    dataframe = (client.query(sql).result().to_dataframe())

    prediction=loaded_model.predict(dataframe)
    print("Batch Prediction : {}").format(prediction)

def predict_online(feature_json):
    loaded_model = joblib.load(model_name)

    feature_list = json.loads(feature_json)
    feature_df = pd.DataFrame(feature_list)

    prediction=loaded_model.predict(feature_df)
    print("Online Prediction :")
    print(prediction)


# Load data from BigQuery public dataset to Pandas
data_in_pandas = load_data_from_bigquery(dataset_table_id)

# Train ML model using RandomForest
random_forest_classifier = train_model(data_in_pandas)

# Predict Batch
predict_batch(dataset_table_id)

# Predict Online
limit_balance = 1000
education_level = 1
age = 25
feature_json = json.dumps([[limit_balance, education_level, age]])
predict_online(feature_json)
