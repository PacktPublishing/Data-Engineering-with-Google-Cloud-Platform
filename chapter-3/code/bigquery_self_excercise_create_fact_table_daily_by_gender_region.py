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

import sys
from google.cloud import bigquery

# TODO : Change to your project id
PROJECT_ID = "packt-data-eng-on-gcp"
target_table_id = "{PROJECT_ID}.dwh_bikesharing.fact_region_gender_daily".format(PROJECT_ID)


def create_fact_table(PROJECT_ID, target_table_id):
    load_date = sys.argv[1] # date format : yyyy-mm-dd
    print("\nLoad date:", load_date)

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
    destination=target_table_id,
    write_disposition='WRITE_APPEND')

    sql = """SELECT DATE(start_date) as trip_date,
                region_id,
                member_gender,
                COUNT(trip_id) as total_trips
                FROM `{PROJECT_ID}.raw_bikesharing.trips` trips
                JOIN `{PROJECT_ID}.raw_bikesharing.stations` stations
                ON trips.start_station_id = stations.station_id
                WHERE DATE(start_date) = DATE('{load_date}') AND member_gender IS NOT NULL
                GROUP BY trip_date, region_id, member_gender
                ;""".format(PROJECT_ID=PROJECT_ID, load_date=load_date)

    query_job = client.query(sql, job_config=job_config)

    try:
        query_job.result()
        print("Query success")
    except Exception as exception:
        print(exception)

if __name__ == '__main__':
    create_fact_table(PROJECT_ID, target_table_id)

