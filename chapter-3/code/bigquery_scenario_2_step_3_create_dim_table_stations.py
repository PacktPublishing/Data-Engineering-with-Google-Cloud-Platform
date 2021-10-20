from google.cloud import bigquery

# TODO : Change to your project id
project_id = "packt-data-eng-on-gcp"
target_table_id = "{}.dwh_bikesharing.dim_stations".format(project_id)

def create_dim_table(project_id, target_table_id):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
    destination=target_table_id,
    write_disposition='WRITE_TRUNCATE')

    table = bigquery.Table(target_table_id)
    sql = f"""SELECT station_id,
          stations.name as station_name,
          regions.name as region_name,
          capacity
          FROM `{project_id}.raw_bikesharing.stations` stations
          JOIN `{project_id}.raw_bikesharing.regions` regions
          ON stations.region_id = CAST(regions.region_id AS STRING)
          ;"""

    query_job = client.query(sql, job_config=job_config)

    try:
        query_job.result()
        print("Query success")
    except Exception as e:
            print(e)

create_dim_table(project_id, target_table_id)
