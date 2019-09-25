import logging
import apache_beam as beam

TEST = '201809.csv'
OUT = 'out'
AIRPORT = 'data\\airports.csv'

class CleanLine(beam.DoFn):

    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def process(self, line):
        yield [word.replace('"', '') for word in line.split(self.delimiter)[:-1]]

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

class GetEvents(beam.DoFn):
    def process(self, fields):
        dep_time = fields[14]
        if dep_time:
            event = list(fields)
            event.extend(['departed', dep_time])
            for f in (16, 17, 18, 19, 21, 22, 24):
                event[f] = ''
            yield event

        arr_time = fields[21]
        if arr_time:
            event = list(fields)
            event.extend(['arrived', arr_time])
            yield event

with beam.Pipeline() as pipeline:

    airports = (pipeline
              | 'airports:ReadData' >> beam.io.ReadFromText(AIRPORT, skip_header_lines=1)
              | 'airports:ToField' >> beam.ParDo(CleanLine())
              | 'airports:AddTz' >> beam.ParDo(AddTz()))

    flights = (pipeline
        | 'flights:ReadData' >> beam.io.ReadFromText(TEST, skip_header_lines=1)
        | 'flights:ToField' >> beam.ParDo(CleanLine())
        | 'flights:TzCorrect' >> beam.ParDo(TzCorrect(), beam.pvalue.AsDict(airports))
        | 'flights:GetEvents' >> beam.ParDo(GetEvents())
        | 'flights:WriteData' >> beam.io.WriteToText(OUT))
