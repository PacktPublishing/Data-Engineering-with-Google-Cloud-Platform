from google.cloud import bigquery

client = bigquery.Client()

# TODO : Change to your project id
project_id = "packt-data-eng-on-gcp"
gcs_uri = "gs://{}-data-bucket/example-data/trips/20180101/*.json".format(project_id)

# This uri for load data from 2018-01-02
#gcs_uri = "gs://{}/example-data/trips/20180102/*.json".format(project_id)

table_id = "{}.raw_bikesharing.trips".format(project_id)

def load_gcs_to_bigquery_event_data(gcs_uri, table_id, table_schema):
    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition = 'WRITE_APPEND'
        )

    load_job = client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )

    load_job.result()
    table = client.get_table(table_id)

    print("Loaded {} rows to table {}".format(table.num_rows, table_id))

bigquery_table_schema = [
    bigquery.SchemaField("trip_id", "STRING"),
    bigquery.SchemaField("duration_sec", "INTEGER"),
    bigquery.SchemaField("start_date", "STRING"),
    bigquery.SchemaField("start_station_name", "STRING"),
    bigquery.SchemaField("start_station_id", "STRING"),
    bigquery.SchemaField("end_date", "STRING"),
    bigquery.SchemaField("end_station_name", "STRING"),
    bigquery.SchemaField("end_station_id", "STRING"),
    bigquery.SchemaField("member_gender", "STRING")
]

load_gcs_to_bigquery_event_data(gcs_uri, table_id, bigquery_table_schema)
