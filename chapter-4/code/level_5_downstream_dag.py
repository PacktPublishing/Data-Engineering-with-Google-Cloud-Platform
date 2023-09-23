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

import os

from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from datetime import datetime

args = {
    'owner': 'packt-developer',
}

# Environment Variables
gcp_project_id = os.environ.get('GCP_PROJECT')

# Airflow Variables
settings = Variable.get("level_3_dag_settings", deserialize_json=True)
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_dwh_dataset         = settings['bq_dwh_dataset']

# DAG Variables
bq_datamart_dataset    = 'dm_bikesharing'
parent_dag = 'level_5_dag_sensor'
bq_fact_trips_daily_table_id = f'{gcp_project_id}.{bq_datamart_dataset}.facts_trips_daily'
sum_total_trips_table_id = f'{gcp_project_id}.{bq_datamart_dataset}.sum_total_trips_daily'

# Macros
execution_date_nodash = '{{ ds_nodash }}'
execution_date = '{{ ds }}'


with DAG(
    dag_id='level_5_downstream_dag',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=datetime(2018, 1, 1),
    end_date=datetime(2018, 1, 5)
) as dag:

    input_sensor = GoogleCloudStorageObjectSensor(
        task_id='sensor_task',
        bucket=gcs_source_data_bucket,
        object=f'chapter-4/data/signal/{parent_dag}/{execution_date_nodash}/_SUCCESS',
        mode='poke',
        poke_interval=60,
        timeout=60 * 60 * 24 * 7
    )

    data_mart_sum_total_trips  = BigQueryOperator(
        task_id                     = "data_mart_sum_total_trips",
        sql                         = f"""SELECT DATE(start_date) as trip_date,
                                      SUM(total_trips) sum_total_trips
                                      FROM `{bq_fact_trips_daily_table_id}`
                                      WHERE trip_date = DATE('{execution_date}')""",
        destination_dataset_table   = sum_total_trips_table_id,
        write_disposition           = 'WRITE_TRUNCATE',
        time_partitioning           = {'time_partitioning_type':'DAY','field': 'trip_date'},
        create_disposition          = 'CREATE_IF_NEEDED',
        use_legacy_sql              = False,
        priority                    = 'BATCH'
    )

    send_dag_success_signal = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id='send_dag_success_signal',
        source_bucket=gcs_source_data_bucket,
        source_object=f'chapter-4/data/signal/_SUCCESS',
        destination_bucket=gcs_source_data_bucket,
        destination_object='data/signal/staging/{{ dag }}/{{ ds }}/_SUCCESS'
    )

    input_sensor >> data_mart_sum_total_trips >> send_dag_success_signal

if __name__ == "__main__":
    dag.cli()
