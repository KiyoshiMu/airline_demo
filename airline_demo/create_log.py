"""A stript for creating real logs from downloaded data"""

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

RAW_DATA = 'data/2018-09.csv'
LOGS = 'data/logs'

class CleanLine(beam.DoFn):

    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def process(self, line):
        yield [word.replace('"', '') for word in line.split(self.delimiter)[:-1]]

class CreatEvents(beam.DoFn):

    def process(self, fields):
        dep_time = fields[14]
        if dep_time:
            event = list(fields)
            event.extend(['departed', dep_time])
            # 17:WHEELS_OFF; 18:WHEELS_ON; 19:TAXI_IN; 20:CRS_ARR_TIME; 21:ARR_TIME
            # 22:ARR_DELAY; 23:CANCELLED 24:CANCELLATION_CODE 25:DIVERTED
            for f in (17, 18, 19, 20, 21, 22, 25):
                event[f] = ''
            yield event

        arr_time = fields[21]
        if arr_time:
            event = list(fields)
            event.extend(['arrived', arr_time])
            yield event

# class CreatLogOptions(PipelineOptions):
#     @classmethod
#     def _add_argparse_args(cls, parser):
#         parser.add_value_provider_argument('--input', required=False,
#         help='Input file to be read. This can be a local file or '
#              'a file in Google Strorage Bucket',
#         default='gs://linelineline/tmp/csvs/2018*.csv')
#         parser.add_value_provider_argument('--output', required=False, 
#         help='Write result log to',
#         default='gs://linelineline/tmp/logs/log')

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=False,
        help='Input file to be read. This can be a local file or '
             'a file in Google Strorage Bucket',
        default='gs://linelineline/flights/csvs/2018*.csv')
    parser.add_argument('--output', required=False, help='Write result log to',
        default='gs://linelineline/flights/logs/log')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # known_args = pipeline_options.view_as(CreatLogOptions)
        _ = (pipeline
            | 'Read' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | 'Create_Fields' >> beam.ParDo(CleanLine())
            | 'Create_Events' >> beam.ParDo(CreatEvents())
            | 'Create_Log' >> beam.Map(lambda fields: ','.join(fields))
            | 'Write' >> beam.io.WriteToText(known_args.output)
        )

if __name__ == "__main__":
    argv = [
        '--project={}'.format('airlinegcp'),
        '--job_name=creat-log',
        # 'flexrs_goal=COST_OPTIMIZED',
        '--staging_location=gs://{}/tmp/staging1/'.format('linelineline'),
        '--temp_location=gs://{}/tmp/temp1/'.format('linelineline'),
        # '--setup_file=./setup.py',
        '--max_num_workers=10',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--runner=DataflowRunner'
        '--region=us-central1'
        ]
    run(argv)