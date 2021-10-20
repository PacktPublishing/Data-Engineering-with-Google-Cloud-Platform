import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from datetime import datetime, time

import json
import logging

input_topic= 'projects/packt-data-eng-on-gcp/topics/bike-trips'
output_table = 'packt-data-eng-on-gcp:raw_bikesharing.bike_trips_streaming_sum_aggr'

class BuildRecordFn(beam.DoFn):
    def process(self, element,  window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().isoformat()
        return [element + (window_start,)]

def run(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(streaming=True)    

    with beam.Pipeline(options=pipeline_options) as p:(
        p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=None)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | "Parse JSON" >> beam.Map(json.loads)
        | "UseFixedWindow" >> beam.WindowInto(beam.window.FixedWindows(60))
        | 'Group By User ID' >> beam.Map(lambda elem: (elem['start_station_id'], elem['duration_sec']))
        | 'Sum' >> beam.CombinePerKey(sum)
        | 'AddWindowEndTimestamp' >> (beam.ParDo(BuildRecordFn()))
        #| 'Print' >> beam.Map(print)
        | 'Parse to JSON' >> beam.Map(lambda x : {'start_station_id': x[0],'sum_duration_sec':x[1],'window_timestamp':x[2]})
        | 'Write to Table' >> beam.io.WriteToBigQuery(output_table,
                        schema='start_station_id:STRING,sum_duration_sec:INTEGER,window_timestamp:TIMESTAMP',
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()