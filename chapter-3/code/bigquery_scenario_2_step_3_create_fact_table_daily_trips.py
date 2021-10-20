import sys
from google.cloud import bigquery

# TODO : Change to your project id
project_id = "packt-data-eng-on-gcp"
target_table_id = "{}.dwh_bikesharing.fact_trips_daily".format(project_id)


def create_fact_table(project_id, target_table_id):
    load_date = sys.argv[1] # date format : yyyy-mm-dd
    print("\nLoad date:", load_date)

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
    destination=target_table_id,
    write_disposition='WRITE_APPEND')

    sql = """SELECT DATE(start_date) as trip_date,
          start_station_id,
          COUNT(trip_id) as total_trips,
          SUM(duration_sec) as sum_duration_sec,
          AVG(duration_sec) as avg_duration_sec
          FROM `{}.raw_bikesharing.trips` trips
          JOIN `{}.raw_bikesharing.stations` stations
          ON trips.start_station_id = stations.station_id
          WhERE DATE(start_date) = DATE('{}')
          GROUP BY trip_date, start_station_id
          ;""".format(project_id, project_id, load_date)

    query_job = client.query(sql, job_config=job_config)

    try:
        query_job.result()
        print("Query success")
    except Exception as e:
            print(e)

create_fact_table(project_id, target_table_id)
