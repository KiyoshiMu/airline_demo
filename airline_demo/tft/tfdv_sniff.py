import tensorflow_data_validation as tfdv
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import time
import os

PROJECT_ID = 'airlinegcp'
JOB_NAME = 'tfdv-{}'.format(int(time.time()))
GCS_STAGING_LOCATION = 'gs://linelineline/tfdv/stage'
GCS_TMP_LOCATION = 'gs://linelineline/tfdv/tmp'
GCS_DATA_LOCATION = 'gs://linelineline/flights/merges/merge*'
# GCS_STATS_OUTPUT_PATH is the file path to which to output the data statistics
# result.
GCS_STATS_OUTPUT_PATH = 'gs://linelineline/tfdv/output'

PATH_TO_WHL_FILE = './tfdv.whl'

CLOUMNS = "FL_DATE, MKT_UNIQUE_CARRIER, ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID, DEP_TIME, DEP_DELAY, ARR_DELAY, DISTANCE, dep_lat, dep_lng, arr_lat, arr_lng".split(', ')
# Create and set your PipelineOptions.
options = PipelineOptions()

# For Cloud execution, set the Cloud Platform project, job_name,
# staging location, temp_location and specify DataflowRunner.
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = PROJECT_ID
google_cloud_options.job_name = JOB_NAME
google_cloud_options.staging_location = GCS_STAGING_LOCATION
google_cloud_options.temp_location = GCS_TMP_LOCATION
options.view_as(StandardOptions).runner = 'DataflowRunner'

setup_options = options.view_as(SetupOptions)
# PATH_TO_WHL_FILE should point to the downloaded tfdv wheel file.
# setup_options.extra_packages = [PATH_TO_WHL_FILE]
setup_options.setup_file = os.path.join(os.getcwd(),'tfdv_setup.py')
stat_options = tfdv.StatsOptions(sample_rate=0.01)

tfdv.generate_statistics_from_csv(GCS_DATA_LOCATION,
                                    column_names=CLOUMNS,
                                       output_path=GCS_STATS_OUTPUT_PATH,
                                       stats_options=stat_options,
                                       pipeline_options=options,)
