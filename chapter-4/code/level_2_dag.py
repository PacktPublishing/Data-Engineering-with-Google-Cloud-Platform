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

from airflow import DAG
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'packt-developer',
}

GCP_PROJECT_ID = 'packt-data-eng-on-gcp'
INSTANCE_NAME = 'mysql-instance'
EXPORT_URI = 'gs://packt-data-eng-on-gcp-data-bucket/mysql_export/from_composer/stations/stations.csv'
SQL_QUERY = "SELECT * FROM apps_db.stations"

export_body = {
    "exportContext": {
        "fileType": "csv",
        "uri": EXPORT_URI,
        "csvExportOptions":{
            "selectQuery": SQL_QUERY
        }
    }
}

with DAG(
    dag_id='level_2_dag_load_bigquery',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
) as dag:

    sql_export_task = CloudSqlInstanceExportOperator(
        project_id=GCP_PROJECT_ID, 
        body=export_body, 
        instance=INSTANCE_NAME, 
        task_id='sql_export_task'
    )

    gcs_to_bq_example = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "gcs_to_bq_example",
    bucket                              = 'packt-data-eng-on-gcp-data-bucket',
    source_objects                      = ['mysql_export/from_composer/stations/stations.csv'],
    destination_project_dataset_table   ='raw_bikesharing.stations',
    schema_fields=[
        {'name': 'station_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'region_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'capacity', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE'
    )

    bq_to_bq  = BigQueryOperator(
        task_id                     = "bq_to_bq",
        sql                         = "SELECT count(*) as count FROM `raw_bikesharing.stations`",
        destination_dataset_table   = 'dwh_bikesharing.temporary_stations_count',
        write_disposition           = 'WRITE_TRUNCATE',
        create_disposition          = 'CREATE_IF_NEEDED',
        use_legacy_sql              = False,
        priority                    = 'BATCH'
    )

    sql_export_task >> gcs_to_bq_example >> bq_to_bq

if __name__ == "__main__":
    dag.cli()
