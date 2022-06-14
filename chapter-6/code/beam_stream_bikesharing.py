# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam
import argparse
import json
import logging

from apache_beam.options.pipeline_options import PipelineOptions

INPUT_SUBSCRIPTION= 'projects/packt-data-eng-on-gcp/subscriptions/bike-sharing-trips-subs-1'
OUTPUT_TABLE = 'packt-data-eng-on-gcp:raw_bikesharing.bike_trips_streaming'

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
beam_options = PipelineOptions(beam_args, streaming=True)

def run():
    with beam.Pipeline(options=beam_options) as p:(
        p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | "Parse JSON" >> beam.Map(json.loads)
        | 'Write to Table' >> beam.io.WriteToBigQuery(OUTPUT_TABLE,
                        schema='trip_id:STRING,start_date:TIMESTAMP,start_station_id:STRING,bike_number:STRING,duration_sec:INTEGER',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()