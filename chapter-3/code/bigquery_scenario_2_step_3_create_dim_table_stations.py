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

# TODO : Change to your project id
PROJECT_ID = "packt-data-eng-on-gcp"
TARGET_TABLE_ID = "{}.dwh_bikesharing.dim_stations".format(PROJECT_ID)

def create_dim_table(PROJECT_ID, TARGET_TABLE_ID):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination=TARGET_TABLE_ID,
        write_disposition='WRITE_TRUNCATE')

    sql = """SELECT station_id,
          stations.name as station_name,
          regions.name as region_name,
          capacity
          FROM `{PROJECT_ID}.raw_bikesharing.stations` stations
          JOIN `{PROJECT_ID}.raw_bikesharing.regions` regions
          ON stations.region_id = CAST(regions.region_id AS STRING)
          ;""".format(PROJECT_ID=PROJECT_ID)

    query_job = client.query(sql, job_config=job_config)

    try:
        query_job.result()
        print("Query success")
    except Exception as exception:
            print(exception)

if __name__ == '__main__':
    create_dim_table(PROJECT_ID, TARGET_TABLE_ID)
