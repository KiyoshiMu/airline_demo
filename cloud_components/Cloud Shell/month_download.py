"""A stript for creating a cloud VM with start-up stript"""

import os
import tempfile
import logging
import datetime
import calendar
from urllib.request import urlopen
import zipfile
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(module)s - %(levelname)s - %(message)s')

URL = 'https://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=237&Has_Group=3&Is_Zipped=0'
PARAMS = 'UserTableName=Marketing_Carrier_On_Time_Performance_Beginning_{month}_{year}&DBShortName=On_Time&RawDataTable=T_ONTIME_MARKETING&sqlstr=+SELECT+FL_DATE%2CMKT_UNIQUE_CARRIER%2CMKT_CARRIER_AIRLINE_ID%2CMKT_CARRIER%2CMKT_CARRIER_FL_NUM%2COP_CARRIER_FL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CDISTANCE+FROM++T_ONTIME_MARKETING+WHERE+Month+%3D{m_num}+AND+YEAR%3D{year}&varlist=FL_DATE%2CMKT_UNIQUE_CARRIER%2CMKT_CARRIER_AIRLINE_ID%2CMKT_CARRIER%2CMKT_CARRIER_FL_NUM%2COP_CARRIER_FL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CDISTANCE&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time={month}&timename=Month&GEOGRAPHY=All&XYEAR={year}&FREQUENCY=1&VarDesc=Year&VarType=Num&VarDesc=Quarter&VarType=Num&VarDesc=Month&VarType=Num&VarDesc=DayofMonth&VarType=Num&VarDesc=DayOfWeek&VarType=Num&VarName=FL_DATE&VarDesc=FlightDate&VarType=Char&VarName=MKT_UNIQUE_CARRIER&VarDesc=Marketing_Airline_Network&VarType=Char&VarDesc=Operated_or_Branded_Code_Share_Partners&VarType=Char&VarName=MKT_CARRIER_AIRLINE_ID&VarDesc=DOT_ID_Marketing_Airline&VarType=Num&VarName=MKT_CARRIER&VarDesc=IATA_Code_Marketing_Airline&VarType=Char&VarName=MKT_CARRIER_FL_NUM&VarDesc=Flight_Number_Marketing_Airline&VarType=Char&VarDesc=Originally_Scheduled_Code_Share_Airline&VarType=Char&VarDesc=DOT_ID_Originally_Scheduled_Code_Share_Airline&VarType=Num&VarDesc=IATA_Code_Originally_Scheduled_Code_Share_Airline&VarType=Char&VarDesc=Flight_Num_Originally_Scheduled_Code_Share_Airline&VarType=Char&VarDesc=Operating_Airline&VarType=Char&VarDesc=DOT_ID_Operating_Airline&VarType=Num&VarDesc=IATA_Code_Operating_Airline&VarType=Char&VarDesc=Tail_Number&VarType=Char&VarName=OP_CARRIER_FL_NUM&VarDesc=Flight_Number_Operating_Airline&VarType=Char&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType=Num&VarDesc=Origin&VarType=Char&VarDesc=OriginCityName&VarType=Char&VarDesc=OriginState&VarType=Char&VarDesc=OriginStateFips&VarType=Char&VarDesc=OriginStateName&VarType=Char&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarDesc=DestCityName&VarType=Char&VarDesc=DestState&VarType=Char&VarDesc=DestStateFips&VarType=Char&VarDesc=DestStateName&VarType=Char&VarDesc=DestWac&VarType=Num&VarName=CRS_DEP_TIME&VarDesc=CRSDepTime&VarType=Char&VarName=DEP_TIME&VarDesc=DepTime&VarType=Char&VarName=DEP_DELAY&VarDesc=DepDelay&VarType=Num&VarDesc=DepDelayMinutes&VarType=Num&VarDesc=DepDel15&VarType=Num&VarDesc=DepartureDelayGroups&VarType=Num&VarDesc=DepTimeBlk&VarType=Char&VarName=TAXI_OUT&VarDesc=TaxiOut&VarType=Num&VarName=WHEELS_OFF&VarDesc=WheelsOff&VarType=Char&VarName=WHEELS_ON&VarDesc=WheelsOn&VarType=Char&VarName=TAXI_IN&VarDesc=TaxiIn&VarType=Num&VarName=CRS_ARR_TIME&VarDesc=CRSArrTime&VarType=Char&VarName=ARR_TIME&VarDesc=ArrTime&VarType=Char&VarName=ARR_DELAY&VarDesc=ArrDelay&VarType=Num&VarDesc=ArrDelayMinutes&VarType=Num&VarDesc=ArrDel15&VarType=Num&VarDesc=ArrivalDelayGroups&VarType=Num&VarDesc=ArrTimeBlk&VarType=Char&VarName=CANCELLED&VarDesc=Cancelled&VarType=Num&VarName=CANCELLATION_CODE&VarDesc=CancellationCode&VarType=Char&VarName=DIVERTED&VarDesc=Diverted&VarType=Num&VarName=DUP&VarDesc=Duplicate&VarType=Char&VarDesc=CRSElapsedTime&VarType=Num&VarDesc=ActualElapsedTime&VarType=Num&VarDesc=AirTime&VarType=Num&VarDesc=Flights&VarType=Num&VarDesc=Distance&VarType=Num&VarDesc=DistanceGroup&VarType=Num&VarDesc=CarrierDelay&VarType=Num&VarDesc=WeatherDelay&VarType=Num&VarDesc=NASDelay&VarType=Num&VarDesc=SecurityDelay&VarType=Num&VarDesc=LateAircraftDelay&VarType=Num&VarDesc=FirstDepTime&VarType=Char&VarDesc=TotalAddGTime&VarType=Num&VarDesc=LongestAddGTime&VarType=Num&VarDesc=DivAirportLandings&VarType=Num&VarDesc=DivReachedDest&VarType=Num&VarDesc=DivActualElapsedTime&VarType=Num&VarDesc=DivArrDelay&VarType=Num&VarDesc=DivDistance&VarType=Num&VarDesc=Div1Airport&VarType=Char&VarDesc=Div1AirportID&VarType=Num&VarDesc=Div1AirportSeqID&VarType=Num&VarDesc=Div1WheelsOn&VarType=Char&VarDesc=Div1TotalGTime&VarType=Num&VarDesc=Div1LongestGTime&VarType=Num&VarDesc=Div1WheelsOff&VarType=Char&VarDesc=Div1TailNum&VarType=Char&VarDesc=Div2Airport&VarType=Char&VarDesc=Div2AirportID&VarType=Num&VarDesc=Div2AirportSeqID&VarType=Num&VarDesc=Div2WheelsOn&VarType=Char&VarDesc=Div2TotalGTime&VarType=Num&VarDesc=Div2LongestGTime&VarType=Num&VarDesc=Div2WheelsOff&VarType=Char&VarDesc=Div2TailNum&VarType=Char&VarDesc=Div3Airport&VarType=Char&VarDesc=Div3AirportID&VarType=Num&VarDesc=Div3AirportSeqID&VarType=Num&VarDesc=Div3WheelsOn&VarType=Char&VarDesc=Div3TotalGTime&VarType=Num&VarDesc=Div3LongestGTime&VarType=Num&VarDesc=Div3WheelsOff&VarType=Char&VarDesc=Div3TailNum&VarType=Char&VarDesc=Div4Airport&VarType=Char&VarDesc=Div4AirportID&VarType=Num&VarDesc=Div4AirportSeqID&VarType=Num&VarDesc=Div4WheelsOn&VarType=Char&VarDesc=Div4TotalGTime&VarType=Num&VarDesc=Div4LongestGTime&VarType=Num&VarDesc=Div4WheelsOff&VarType=Char&VarDesc=Div4TailNum&VarType=Char&VarDesc=Div5Airport&VarType=Char&VarDesc=Div5AirportID&VarType=Num&VarDesc=Div5AirportSeqID&VarType=Num&VarDesc=Div5WheelsOn&VarType=Char&VarDesc=Div5TotalGTime&VarType=Num&VarDesc=Div5LongestGTime&VarType=Num&VarDesc=Div5WheelsOff&VarType=Char&VarDesc=Div5TailNum&VarType=Char'


class Uploader:
    """A object used to upload file to google cloud storage"""

    def __init__(self, bucket_id):
        client = storage.Client()
        self.bucket = client.get_bucket(bucket_id)

    def upload(self, f_p, blobname):
        self.bucket.blob(blobname).upload_from_filename(filename=f_p)
        logging.info('%s upload completed', f_p)

    def exist_check(self, date, blobpath):
        blobs = set(os.path.basename(file_.name)
                    for file_ in self.bucket.list_blobs(prefix=blobpath))
        return f'{date.year}-{date.month:02d}.csv' in blobs


def _get_download_data(date):
    target = {'year': date.year,
              'month': calendar.month_name[date.month], 'm_num': date.month}
    data = PARAMS.format(**target).encode('utf8')
    name = '{year}-{m_num:02d}.zip'.format(**target)
    return name, data


def _get_zip(f_p, data):
    with open(f_p, 'wb') as file_:
        response = urlopen(URL, data=data)
        file_.write(response.read())
    logging.info('%s download completed', f_p)


def _unzip(f_p, dst):
    with zipfile.ZipFile(f_p) as zipfile_:
        member = zipfile_.infolist()[0]
        csv_p = f_p.replace('.zip', '.csv', 1)
        zipfile_.extract(member, dst)
        os.rename(os.path.join(dst, member.filename), csv_p)
    logging.info('Unzip completed to %s', csv_p)
    return csv_p


def download(date, dst) -> str:
    name, data = _get_download_data(date)
    f_p = os.path.join(dst, name)
    _get_zip(f_p, data)
    return f_p


def _size_check(f_p):
    # the normal file size is at least large than 5 MB
    return os.stat(f_p).st_size > 5000000


def main(bucket, date=None):
    dst = tempfile.mkdtemp()
    if date is None:
        date = datetime.date.today()
        date = date.replace(year=2018)
    uploader = Uploader(bucket)
    blobpath = 'tmp/csvs/'
    if uploader.exist_check(date, blobpath):
        logging.info('Data of %s already exist', date)
        return

    f_p = download(date, dst)
    if _size_check(f_p):
        csv_p = _unzip(f_p, dst)
        os.remove(f_p)
        blobname = f'tmp/csvs/{os.path.basename(csv_p)}'
        uploader.upload(csv_p, blobname)
        # os.remove(csv_p)
    else:
        logging.warning('Data of %s may not publish yet', date)
