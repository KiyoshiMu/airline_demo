"""
Send json data to a deployed model for prediction.
Function dependencies:
google-api-python-client
tensorflow-transform==0.14.0
tensorflow==1.14
"""

from tensorflow_transform.coders import csv_coder
from tensorflow_transform import coders as tft_coders
import base64
import tensorflow as tf
from tensorflow_transform.tf_metadata import dataset_metadata, schema_utils
import googleapiclient.discovery
from flask import jsonify

CATEGORICAL_FEATURE_KEYS = ['MKT_UNIQUE_CARRIER']

NUMERIC_FEATURE_KEYS = ['arr_lat', 'dep_lat', 'dep_lng', 'arr_lng', 'DEP_DELAY',
                        'DISTANCE',
                        ]
NUMERIC_FEATURE_KEYS_INT = ['hour', 'month']

ORDERED_COLUMNS = ['MKT_UNIQUE_CARRIER',
                   'DEP_DELAY', 'DISTANCE',
                   'dep_lat', 'dep_lng', 'arr_lat', 'arr_lng', 'month', 'hour']


def _create_raw_metadata():
    """Create a DatasetMetadata for the raw data."""
    feature_spec = {
        key: tf.io.FixedLenFeature([], tf.string)
        for key in CATEGORICAL_FEATURE_KEYS
    }
    feature_spec.update({
        key: tf.io.FixedLenFeature([], tf.float32)
        for key in NUMERIC_FEATURE_KEYS
    })
    feature_spec.update({
        key: tf.io.FixedLenFeature([], tf.int64)
        for key in NUMERIC_FEATURE_KEYS_INT
    })

    raw_data_metadata = dataset_metadata.DatasetMetadata(
        schema_utils.schema_from_feature_spec(feature_spec))
    return raw_data_metadata


RAW_DATA_METADATA = _create_raw_metadata()

csv_coder_ = csv_coder.CsvCoder(ORDERED_COLUMNS, RAW_DATA_METADATA.schema)
proto_coder = tft_coders.ExampleProtoCoder(RAW_DATA_METADATA.schema)


def cus_input(one_line):
    one_example = csv_coder_.decode(one_line)
    serialized_example = proto_coder.encode(one_example)
    json_example = {"inputs": {
        "b64": base64.b64encode(serialized_example).decode()}}
    return json_example


def predict_json(request):
    """
    You need to headcode project, model and version
    """
    project = 'eeeooosss'
    model = 'flightFlow'
    version = 'v1'
    instances = request.get_json()
    for idx, line in enumerate(instances['instances']):
        instances['instances'][idx] = cus_input(line)

    service = googleapiclient.discovery.build('ml', 'v1')
    name = 'projects/{}/models/{}'.format(project, model)
    name += '/versions/{}'.format(version)
    response = service.projects().predict(
        name=name,
        body=instances
    ).execute()

    if 'error' in response:
        raise RuntimeError(response['error'])

    return jsonify(response)
