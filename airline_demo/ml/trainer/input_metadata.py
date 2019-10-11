import json
import tensorflow as tf
from tensorflow_transform.tf_metadata import dataset_metadata, schema_utils

HASH_STRING_FEATURE_KEYS = {'MKT_UNIQUE_CARRIER':12, 'ORIGIN_AIRPORT_ID':373, 'DEST_AIRPORT_ID':373}

CATEGORICAL_FEATURE_KEYS = list(HASH_STRING_FEATURE_KEYS.keys())

# CATEGORICAL_FEATURE_KEYS_TO_BE_REMOVED = []

NUMERIC_FEATURE_KEYS = ['arr_lat', 'dep_lat', 'dep_lng', 'arr_lng','DEP_DELAY', 
                        'DISTANCE', 
        ]
NUMERIC_FEATURE_KEYS_INT = ['hour', 'month']

# NUMERIC_FEATURE_KEYS_TO_BE_REMOVED = []

TO_BE_BUCKETIZED_FEATURE = {
     'arr_lat':6, 'dep_lat':6, 'dep_lng':6, 'arr_lng':6,
}

LABEL_KEY = 'cancel'

ORDERED_COLUMNS = ['MKT_UNIQUE_CARRIER', 'ORIGIN_AIRPORT_ID', 'DEST_AIRPORT_ID',
                    'DEP_DELAY', 'DISTANCE', 
                    'dep_lat', 'dep_lng', 'arr_lat', 'arr_lng', 'month', 'hour', 'cancel']

def _create_raw_metadata():
    """Create a DatasetMetadata for the raw data."""
    feature_spec = {
        key: tf.FixedLenFeature([], tf.string)
        for key in CATEGORICAL_FEATURE_KEYS
    }
    feature_spec.update({
        key: tf.FixedLenFeature([], tf.float32)
        for key in NUMERIC_FEATURE_KEYS
    })
    feature_spec.update({
        key: tf.FixedLenFeature([], tf.int64)
        for key in NUMERIC_FEATURE_KEYS_INT
    })
    feature_spec.update({
        LABEL_KEY: tf.FixedLenFeature([], tf.int64)})

    raw_data_metadata = dataset_metadata.DatasetMetadata(
                            schema_utils.schema_from_feature_spec(feature_spec))
    return raw_data_metadata

RAW_DATA_METADATA = _create_raw_metadata()