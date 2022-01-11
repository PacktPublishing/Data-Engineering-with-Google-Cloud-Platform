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
