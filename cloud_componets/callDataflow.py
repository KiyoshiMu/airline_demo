"""A script for merging real-time depart and arrive data to a simgle sample 
used to build the deep-learning model"""

import time
import tempfile
import os
import argparse

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def makesetup():
    tmp = tempfile.mkdtemp()
    setup_p = os.path.join(tmp, 'setup.py')
    with open(setup_p, 'w') as s_:
        s_.write("""\
import setuptools

REQUIRED_PACKAGES = [
    'timezonefinder==3.0.0',
    'pytz'
    ]
setuptools.setup(
    name='merge_event',
    version='0.0.1',
    description='Data Science on GCP flights analysis pipelines',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    )
"""
    )
    return setup_p

class CleanLine(beam.DoFn):

    def __init__(self, delimiter=',', cutend=True):
        self.delimiter = delimiter
        self.cutend = cutend

    def process(self, line):
        if self.cutend:
            yield [word.replace('"', '')
                    for word in line.split(self.delimiter)[:-1]]
        else:
            yield [word.replace('"', '')
                    for word in line.split(self.delimiter)]

class AddTz(beam.DoFn):

    def process(self, fields):
        try:
            yield (fields[0], (self._add_timezone(fields[1], fields[2])))
        except:
            logging.exception('AddTz has errors when working on %s', ','.join(fields))

    def _add_timezone(self, lat_str, lng_str):
        lng = float(lng_str)
        lat = float(lat_str)
        import timezonefinder
        tfinder = timezonefinder.TimezoneFinder()
        tzone = tfinder.timezone_at(lng=lng, lat=lat)
        if tzone is None:
            tzone = 'UTC'
        return (lat_str, lng_str, tzone)

class TzCorrect(beam.DoFn):

    def process(self, fields, airport_tzs):
        len_ = len(fields)
        if len_ == 27:
            dep_airport_id = fields[7]
            arr_airport_id = fields[10]
            try:
                dep_tz_info = airport_tzs[dep_airport_id]
                arr_tz_info = airport_tzs[arr_airport_id]
            except KeyError:
                logging.exception('AirportId miss tz: %s; %s', dep_airport_id, arr_airport_id)
            else:
                dep_lat, dep_lng, dep_tz = dep_tz_info
                arr_lat, arr_lng, arr_tz = arr_tz_info
                dep_tz = self._get_tz(dep_tz)
                arr_tz = self._get_tz(arr_tz)
                for dep_rel in (13, 14, 17): #CRS_DEP_TIME, DEP_TIME, WHEELS_OFF
                    fields[dep_rel], dep_offset = self._as_utc(fields[0],
                                                            fields[dep_rel], dep_tz)
                for arr_rel in (18, 20, 21): #WHEELS_ON, CRS_ARR_TIME, ARR_TIME
                    fields[arr_rel], arr_offset = self._as_utc(fields[0],
                                                            fields[arr_rel], arr_tz)
                for arr_rel in (18, 20, 21):
                    fields[arr_rel] = self._add_24h_if_before(fields[arr_rel], fields[14])
                fields.extend([dep_lat, dep_lng, str(dep_offset)])
                fields.extend([arr_lat, arr_lng, str(arr_offset)])
                yield fields           
        else:
            logging.error('Fields of %s is %d, which should be 27', ','.join(fields), len_)
    
    def _as_utc(self, date, hhmm, tzone):
        if len(hhmm) == 4 and tzone is not None:
            import datetime, pytz
            loc_dt = tzone.localize(datetime.datetime.strptime(date, '%Y-%m-%d'),
                                     is_dst=False)
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[-2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.isoformat(), loc_dt.utcoffset().total_seconds()
        else:
            return '', 0

    def _get_tz(self, tz_info):
        if tz_info is None:
            return tz_info
        import pytz
        return pytz.timezone(tz_info)

    def _add_24h_if_before(self, arr_time, dep_time):
        import datetime
        if arr_time and dep_time and arr_time < dep_time:
            real_arr_time = datetime.datetime.fromisoformat(arr_time)
            real_arr_time += datetime.timedelta(hours=24)
            return real_arr_time.isoformat()
        else:
            return arr_time

def run(argv=None):
    HEADER = ['FL_DATE', 'MKT_UNIQUE_CARRIER', 'MKT_CARRIER_AIRLINE_ID', 
'MKT_CARRIER', 'MKT_CARRIER_FL_NUM', 'OP_CARRIER_FL_NUM', 'ORIGIN_AIRPORT_ID',
 'ORIGIN_AIRPORT_SEQ_ID', 'ORIGIN_CITY_MARKET_ID', 'DEST_AIRPORT_ID', 
 'DEST_AIRPORT_SEQ_ID', 'DEST_CITY_MARKET_ID', 'DEST', 'CRS_DEP_TIME', 
 'DEP_TIME', 'DEP_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON', 'TAXI_IN', 
 'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY', 'CANCELLED', 'CANCELLATION_CODE', 
 'DIVERTED', 'DISTANCE', 'DEP_AIRPORT_LAT', 'DEP_AIRPORT_LON', 
 'DEP_AIRPORT_TZOFFSET', 'ARR_AIRPORT_LAT', 'ARR_AIRPORT_LON', 
 'ARR_AIRPORT_TZOFFSET']
    SCHEMA = 'FL_DATE:date,MKT_UNIQUE_CARRIER:string,MKT_CARRIER_AIRLINE_ID:string,MKT_CARRIER:string,MKT_CARRIER_FL_NUM:string,OP_CARRIER_FL_NUM:string,ORIGIN_AIRPORT_ID:string,ORIGIN_AIRPORT_SEQ_ID:string,ORIGIN_CITY_MARKET_ID:string,DEST_AIRPORT_ID:string,DEST_AIRPORT_SEQ_ID:string,DEST_CITY_MARKET_ID:string,DEST:string,CRS_DEP_TIME:timestamp,DEP_TIME:timestamp,DEP_DELAY:float,TAXI_OUT:float,WHEELS_OFF:timestamp,WHEELS_ON:timestamp,TAXI_IN:float,CRS_ARR_TIME:timestamp,ARR_TIME:timestamp,ARR_DELAY:float,CANCELLED:float,CANCELLATION_CODE:string,DIVERTED:string,DISTANCE:float,DEP_AIRPORT_LAT:float,DEP_AIRPORT_LON:float,DEP_AIRPORT_TZOFFSET:float,ARR_AIRPORT_LAT:float,ARR_AIRPORT_LON:float,ARR_AIRPORT_TZOFFSET:float'
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=False,
        help='Input file to be read. This can be a local file or '
             'a file in Google Strorage Bucket',
        default='gs://linelineline/tmp/csvs/2019-01.csv')
    parser.add_argument('--output', required=False, help='Write result Merge to',
        default='airlinegcp:flights.lake')
    parser.add_argument('--airport', required=False, help='Write result Merge to',
        default='gs://linelineline/airports/air')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        if known_args.airport.endswith('.csv'):
            airports = (pipeline
                    | 'airports:ReadData' >> beam.io.ReadFromText(known_args.airport,
                                                                skip_header_lines=1)
                    | 'airports:ToField' >> beam.ParDo(CleanLine())
                    | 'airports:AddTz' >> beam.ParDo(AddTz()))
            _ = (airports 
                | beam.Map(lambda fields : ','.join([str(fields[0]), str(fields[1][0]),
                                str(fields[1][1]), str(fields[1][2])]))
                | beam.io.WriteToText('air') # move place later
            )
        else:
        # make airport TZ like a cache, once in local 
            airports = (pipeline
                        | 'airports:ReadCache' >> beam.io.ReadFromText(known_args.airport)
                        | beam.ParDo(CleanLine(cutend=False))
                        | 'airports:getField' >> beam.Map(lambda fields: (fields[0], (fields[1], fields[2], fields[3])))
                        )
        _ = (pipeline
                | 'flights:Read' >> beam.io.ReadFromText(known_args.input)
                | 'flights:GetField' >> beam.ParDo(CleanLine())
                # you will not need the prediction for a canceled flight, which never arrives
                | 'flights:GetUseful' >> beam.Filter(lambda fields: fields[21] != '') # ARR_TIME
                | 'flights:TzCorrect' >> beam.ParDo(TzCorrect(), beam.pvalue.AsDict(airports))
                | 'flights:toRows' >> beam.Map(lambda fields:dict(zip(HEADER, fields)))
                | 'flights:toBq' >> beam.io.Write(beam.io.WriteToBigQuery(
                    known_args.output, 
                    schema=SCHEMA,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
        )

def main(event, context):
    s_p = makesetup()
    file_ = event
    argv = [
        '--project={}'.format('airlinegcp'),
        '--job_name=create-merge',
        '--staging_location=gs://{}/tmp/staging2/'.format('sssoooeee'),
        '--temp_location=gs://{}/tmp/temp2/'.format('sssoooeee'),
        '--setup_file={}'.format(s_p),
        '--max_num_workers=4',
        '--runner=DataflowRunner',
        '--region=us-central1',
        '--input={}'.format(file_['bucket']+file_['name'])
        ]
    run(argv)

