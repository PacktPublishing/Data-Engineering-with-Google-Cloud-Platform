from google.cloud import bigquery

# TODO : Change to your project id
project_id = "packt-data-eng-on-gcp"
target_table_id = "{}.dwh_bikesharing.dim_stations".format(project_id)

def create_dim_table(project_id, target_table_id):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
    destination=target_table_id,
    write_disposition='WRITE_TRUNCATE')

    sql = """SELECT station_id,
          stations.name as station_name,
          regions.name as region_name,
          capacity
          FROM `{}.raw_bikesharing.stations` stations
          JOIN `{}.raw_bikesharing.regions` regions
          ON stations.region_id = CAST(regions.region_id AS STRING)
          ;""".format(project_id, project_id)

    query_job = client.query(sql, job_config=job_config)

    try:
        query_job.result()
        print("Query success")
    except Exception as e:
            print(e)

create_dim_table(project_id, target_table_id)
