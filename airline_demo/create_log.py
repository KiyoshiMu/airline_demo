"""A stript for creating real logs from downloaded data"""

import apache_beam as beam

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

with beam.Pipeline() as pipeline:
    _ = (pipeline
        | 'Read' >> beam.io.ReadFromText(RAW_DATA, skip_header_lines=1)
        | 'Create_Fields' >> beam.ParDo(CleanLine())
        | 'Create_Events' >> beam.ParDo(CreatEvents())
        | 'Create_Log' >> beam.Map(lambda fields: ','.join(fields))
        | 'Write' >> beam.io.WriteToText(LOGS)
    )