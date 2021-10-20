import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import json
import logging

input_subscription= 'projects/packt-data-eng-on-gcp/subscriptions/bike-sharing-trips-subs-1'
output_table = 'packt-data-eng-on-gcp:raw_bikesharing.bike_trips_streaming'

def run():
    pipeline_options = PipelineOptions(streaming=True)    

    with beam.Pipeline(options=pipeline_options) as p:(
        p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | "Parse JSON" >> beam.Map(json.loads)
        | 'Write to Table' >> beam.io.WriteToBigQuery(output_table,
                        schema='trip_id:STRING,start_date:TIMESTAMP,start_station_id:STRING,bike_number:STRING,duration_sec:INTEGER',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()