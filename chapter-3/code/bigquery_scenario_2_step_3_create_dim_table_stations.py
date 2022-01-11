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
