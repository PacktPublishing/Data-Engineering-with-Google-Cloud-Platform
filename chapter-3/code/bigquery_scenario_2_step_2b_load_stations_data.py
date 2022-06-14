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
TABLE_ID = "{}.raw_bikesharing.stations".format(PROJECT_ID)
GCS_URI = "gs://{}-data-bucket/mysql_export/stations/20180102/stations.csv".format(project_id)

def load_gcs_to_bigquery_snapshot_data(GCS_URI, TABLE_ID, table_schema):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema = table_schema,
        source_format = bigquery.SourceFormat.CSV,
        write_disposition = 'WRITE_TRUNCATE'
        )

    load_job = client.load_table_from_uri(
        GCS_URI, TABLE_ID, job_config=job_config
    )
    load_job.result()
    table = client.get_table(TABLE_ID)

    print("Loaded {} rows to table {}".format(table.num_rows, TABLE_ID))

bigquery_table_schema = [
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("region_id", "STRING"),
    bigquery.SchemaField("capacity", "INTEGER")
]

if __name__ == '__main__':
    load_gcs_to_bigquery_snapshot_data(GCS_URI, TABLE_ID, bigquery_table_schema)
