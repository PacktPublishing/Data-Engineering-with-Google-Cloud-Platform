from google.cloud import bigquery

# TODO : Change to your project id
project_id = "packt-data-eng-on-gcp"
table_id = "{}.raw_bikesharing.stations".format(project_id)
gcs_uri = "gs://{}-data-bucket/mysql_export/stations/20180102/stations.csv".format(project_id)

def load_gcs_to_bigquery_snapshot_data(gcs_uri, table_id, table_schema):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema = table_schema,
        source_format = bigquery.SourceFormat.CSV,
        write_disposition = 'WRITE_TRUNCATE'
        )

    load_job = client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    load_job.result()
    table = client.get_table(table_id)

    print("Loaded {} rows to table {}".format(table.num_rows, table_id))

bigquery_table_schema = [
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("region_id", "STRING"),
    bigquery.SchemaField("capacity", "INTEGER")
]

load_gcs_to_bigquery_snapshot_data(gcs_uri, table_id, bigquery_table_schema)
