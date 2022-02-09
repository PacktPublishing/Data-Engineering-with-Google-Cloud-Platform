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
import logging

from apache_beam.transforms.combiners import Sample
from apache_beam.options.pipeline_options import PipelineOptions


INPUT_FILE = 'gs://packt-data-eng-on-gcp-data-bucket/from-git/chapter-6/dataset/logs_example.txt'
OUTPUT_PATH = 'gs://packt-data-eng-on-gcp-data-bucket/chapter-6/dataflow/output/output_file_'

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
beam_options = PipelineOptions(beam_args)

class Split(beam.DoFn):
    def process(self, element):
        rows = element.split(" ")
        return [{
            'ip': str(rows[0]),
            'date': str(rows[3]),
            'method': str(rows[5]),
            'url': str(rows[6]),
        }]

def split_map(records):
    rows = records.split(" ")
    return {
                'ip': str(rows[0]),
                'date': str(rows[3]),
                'method': str(rows[5]),
                'url': str(rows[6]),
            }

def run():
    with beam.Pipeline(options=beam_options) as p:(
        p
        | 'Read' >> beam.io.textio.ReadFromText(INPUT_FILE)
        #| 'Split' >> beam.Map(split_map)
        | 'Split' >> beam.ParDo(Split())
        | 'Get URL' >> beam.Map(lambda s: (s['url'], 1))
        | 'Count per Key' >> beam.combiners.Count.PerKey()
        | 'Sample' >> Sample.FixedSizeGlobally(10)
        #| 'Print' >> beam.Map(print)
        | 'Write' >> beam.io.textio.WriteToText(OUTPUT_PATH)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
