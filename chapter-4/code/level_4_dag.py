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

import json
import os

from airflow import DAG
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from datetime import datetime

args = {
    'owner': 'packt-developer',
}

def read_json_schema(gcs_file_path):
    with open(gcs_file_path, "r") as file:
        schema_json = json.load(file)

    return schema_json

# Environment Variables
gcp_project_id = os.environ.get('GCP_PROJECT')
instance_name  = os.environ.get('MYSQL_INSTANCE_NAME')

# Airflow Variables
settings = Variable.get("level_3_dag_settings", deserialize_json=True)

# DAG Variables
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_raw_dataset         = settings['bq_raw_dataset']
bq_dwh_dataset         = settings['bq_dwh_dataset']

# Macros
extracted_date         = '{{ ds }}'
extracted_date_nodash  = '{{ ds_nodash }}'

# Stations
station_source_object  = f"chapter-4/stations/stations.csv"
sql_query              = "SELECT * FROM apps_db.stations"


export_body = {
    "exportContext": {
        "fileType": "csv",
        "uri": f"""gs://{gcs_source_data_bucket}/{station_source_object}""",
        "csvExportOptions":{
            "selectQuery": sql_query
        }
    }
}

bq_stations_table_name = "stations"
bq_stations_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_stations_table_name}"
bq_stations_table_schema = read_json_schema("/home/airflow/gcs/data/schema/stations_schema.json")

# Regions
gcs_regions_source_object = "from-git/chapter-3/dataset/regions/regions.csv"
gcs_regions_target_object = f"chapter-4/regions/{extracted_date_nodash}/regions.csv"
bq_regions_table_name = "regions"
bq_regions_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_regions_table_name}"
bq_regions_table_schema = read_json_schema("/home/airflow/gcs/data/schema/regions_schema.json")

# Trips
bq_temporary_extract_dataset_name = "temporary_staging"
bq_temporary_extract_table_name = "trips"
bq_temporary_table_id = f"{gcp_project_id}.{bq_temporary_extract_dataset_name}.{bq_temporary_extract_table_name}_{extracted_date_nodash}"

gcs_trips_source_object = f"chapter-4/trips/{extracted_date_nodash}/*.csv"
gcs_trips_source_uri = f"gs://{gcs_source_data_bucket}/{gcs_trips_source_object}"

bq_trips_table_name = "trips"
bq_trips_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_trips_table_name}"
bq_trips_table_schema = read_json_schema("/home/airflow/gcs/data/schema/trips_schema.json")

# DWH
bq_fact_trips_daily_table_name = "facts_trips_daily"
bq_fact_trips_daily_table_id = f"{gcp_project_id}.{bq_dwh_dataset}.{bq_fact_trips_daily_table_name}${extracted_date_nodash}"

bq_dim_stations_table_name = "dim_stations"
bq_dim_stations_table_id = f"{gcp_project_id}.{bq_dwh_dataset}.{bq_dim_stations_table_name}"

with DAG(
    dag_id='level_4_dag_task_idempotency',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=datetime(2018, 1, 1),
    end_date=datetime(2018, 1, 5)
) as dag:

    ### Load Station Table ###
    export_mysql_station = CloudSqlInstanceExportOperator(
        task_id='export_mysql_station',
        project_id=gcp_project_id,
        body=export_body,
        instance=instance_name
    )

    gcs_to_bq_station = GoogleCloudStorageToBigQueryOperator(
        task_id                             = "gcs_to_bq_station",
        bucket                              = gcs_source_data_bucket,
        source_objects                      = [station_source_object],
        destination_project_dataset_table   = bq_stations_table_id,
        schema_fields                       = bq_stations_table_schema,
        write_disposition                   = 'WRITE_TRUNCATE'
    )

    ### Load Region Table ###
    gcs_to_gcs_region = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id             = 'gcs_to_gcs_region',
        source_bucket       = gcs_source_data_bucket,
        source_object       = gcs_regions_source_object,
        destination_bucket  = gcs_source_data_bucket,
        destination_object  = gcs_regions_target_object
    )

    gcs_to_bq_region = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "gcs_to_bq_region",
    bucket                              = gcs_source_data_bucket,
    source_objects                      = [gcs_regions_target_object],
    destination_project_dataset_table   = bq_regions_table_id,
    schema_fields                       = bq_regions_table_schema,
    write_disposition                   ='WRITE_TRUNCATE'
    )

    ### Load Trips Table ###
    bq_to_bq_temporary_trips = BigQueryOperator(
    task_id='bq_to_bq_temporary_trips',
    sql=f"""
        SELECT * FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
        WHERE DATE(start_date) = DATE('{extracted_date}')
        """,
    use_legacy_sql=False,
    destination_dataset_table=bq_temporary_table_id,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED')

    bq_to_gcs_extract_trips = BigQueryToCloudStorageOperator(
    task_id='bq_to_gcs_extract_trips',
    source_project_dataset_table=bq_temporary_table_id,
    destination_cloud_storage_uris=[gcs_trips_source_uri],
    print_header=False,
    export_format='CSV')

    gcs_to_bq_trips = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "gcs_to_bq_trips",
    bucket                              = gcs_source_data_bucket,
    source_objects                      = [gcs_trips_source_object],
    destination_project_dataset_table   = bq_trips_table_id + f"${extracted_date_nodash}",
    schema_fields                       = bq_trips_table_schema,
    time_partitioning                   = {'time_partitioning_type':'DAY','field': 'start_date'},
    write_disposition                   ='WRITE_TRUNCATE'
    )

    ### Load DWH Tables ###
    dwh_fact_trips_daily  = BigQueryOperator(
        task_id                     = "dwh_fact_trips_daily",
        sql                         = f"""SELECT DATE(start_date) as trip_date,
                                      start_station_id,
                                      COUNT(trip_id) as total_trips,
                                      SUM(duration_sec) as sum_duration_sec,
                                      AVG(duration_sec) as avg_duration_sec
                                      FROM `{bq_trips_table_id}`
                                      WHERE DATE(start_date) = DATE('{extracted_date}')
                                      GROUP BY trip_date, start_station_id""",
        destination_dataset_table   = bq_fact_trips_daily_table_id,
        write_disposition           = 'WRITE_TRUNCATE',
        time_partitioning           = {'time_partitioning_type':'DAY','field': 'trip_date'},
        create_disposition          = 'CREATE_IF_NEEDED',
        use_legacy_sql              = False,
        priority                    = 'BATCH'
    )

    dwh_dim_stations  = BigQueryOperator(
        task_id                     = "dwh_dim_stations",
        sql                         = f"""SELECT station_id,
                                      stations.name as station_name,
                                      regions.name as region_name,
                                      capacity
                                      FROM `{bq_stations_table_id}` stations
                                      JOIN `{bq_regions_table_id}` regions
                                      ON stations.region_id = CAST(regions.region_id AS STRING)
                                      ;""",
        destination_dataset_table   = bq_dim_stations_table_id,
        write_disposition           = 'WRITE_TRUNCATE',
        create_disposition          = 'CREATE_IF_NEEDED',
        use_legacy_sql              = False,
        priority                    = 'BATCH'
    )

    ### BQ Row Count Checker ###
    bq_row_count_check_dwh_fact_trips_daily = BigQueryCheckOperator(
    task_id='bq_row_count_check_dwh_fact_trips_daily',
    sql=f"""
    select count(*) from `{gcp_project_id}.{bq_dwh_dataset}.{bq_fact_trips_daily_table_name}`
    WHERE trip_date = DATE('{extracted_date}')
    """,
    use_legacy_sql=False)

    bq_row_count_check_dwh_dim_stations = BigQueryCheckOperator(
    task_id='bq_row_count_check_dwh_dim_stations',
    sql=f"""
    select count(*) from `{bq_dim_stations_table_id}`
    """,
    use_legacy_sql=False)

    ### Load Data Mart ###
    export_mysql_station >> gcs_to_bq_station
    gcs_to_gcs_region >> gcs_to_bq_region
    bq_to_bq_temporary_trips >> bq_to_gcs_extract_trips >> gcs_to_bq_trips

    [gcs_to_bq_station,gcs_to_bq_region,gcs_to_bq_trips] >> dwh_fact_trips_daily >> bq_row_count_check_dwh_fact_trips_daily
    [gcs_to_bq_station,gcs_to_bq_region,gcs_to_bq_trips] >> dwh_dim_stations >> bq_row_count_check_dwh_dim_stations

if __name__ == "__main__":
    dag.cli()
