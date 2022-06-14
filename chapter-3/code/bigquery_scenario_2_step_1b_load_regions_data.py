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
PUBLIC_TABLE_ID = "bigquery-public-data.san_francisco_bikeshare.bikeshare_regions"
TARGET_TABLE_ID = "{}.raw_bikesharing.regions".format(PROJECT_ID)

def load_data_from_bigquery_public(PUBLIC_TABLE_ID, TARGET_TABLE_ID):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination = TARGET_TABLE_ID,
        write_disposition ='WRITE_TRUNCATE')

    sql = "SELECT * FROM `{}`;".format(PUBLIC_TABLE_ID)
    query_job = client.query(sql, job_config=job_config)

    try:
        query_job.result()
        print("Query success")
    except Exception as exception:
        print(exception)

if __name__ == '__main__':
    load_data_from_bigquery_public(PUBLIC_TABLE_ID, TARGET_TABLE_ID)
