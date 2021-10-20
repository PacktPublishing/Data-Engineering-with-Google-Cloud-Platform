from google.cloud import bigquery

# TODO : Change to your project id
project_id = "packt-data-eng-on-gcp"
public_table_id = "bigquery-public-data.san_francisco_bikeshare.bikeshare_regions"
target_table_id = "{}.raw_bikesharing.regions".format(project_id)

def load_data_from_bigquery_public(public_table_id, target_table_id):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
    destination = target_table_id,
    write_disposition ='WRITE_TRUNCATE')

    sql = "SELECT * FROM `{}`;".format(public_table_id)

    query_job = client.query(sql, job_config=job_config)
    
    try:
        query_job.result()
        print("Query success")
    except Exception as e:
            print(e)

load_data_from_bigquery_public(public_table_id, target_table_id)
