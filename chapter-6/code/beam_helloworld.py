import argparse

import apache_beam as beam
from apache_beam.transforms.combiners import Sample
from apache_beam.options.pipeline_options import PipelineOptions
import logging

input_file = 'gs://packt-data-eng-on-gcp-data-bucket/from-git/chapter-5/dataset/logs_example.txt'
output_path = 'gs://packt-data-eng-on-gcp-data-bucket/chapter-6/dataflow/output/output_file_'

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
        | 'Read' >> beam.io.textio.ReadFromText(input_file)
        #| 'Split' >> beam.Map(split_map)
        | 'Split' >> beam.ParDo(Split()) 
        | 'Get URL' >> beam.Map(lambda s: (s['url'], 1))
        | 'Count per Key' >> beam.combiners.Count.PerKey()
        | 'Sample' >> Sample.FixedSizeGlobally(10)
        #| 'Print' >> beam.Map(print)
        | 'Write' >> beam.io.textio.WriteToText(output_path)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
