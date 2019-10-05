import json
import tensorflow as tf
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import dataset_schema

HASH_STRING_FEATURE_KEYS = {'MKT_UNIQUE_CARRIER':12, 'ORIGIN_AIRPORT_ID':373, 'DEST_AIRPORT_ID':373}

CATEGORICAL_FEATURE_KEYS = list(HASH_STRING_FEATURE_KEYS.keys())

# CATEGORICAL_FEATURE_KEYS_TO_BE_REMOVED = []

NUMERIC_FEATURE_KEYS = ['DEP_DELAY', 'ARR_DELAY', 'DISTANCE', 'dep_lat',
                                                 'dep_lng', 'arr_lat', 'arr_lng',
        ]
NUMERIC_FEATURE_KEYS_INT = ['dep_hour', 'month']

# NUMERIC_FEATURE_KEYS_TO_BE_REMOVED = []

TO_BE_BUCKETIZED_FEATURE = {
    'dep_hour':24, 'month':12
}

LABEL_KEY = 'ARR_DELAY'

ORDERED_COLUMNS = ['MKT_UNIQUE_CARRIER', 'ORIGIN_AIRPORT_ID', 'DEST_AIRPORT_ID',
                    'DEP_DELAY', 'ARR_DELAY', 'DISTANCE', 
                    'dep_lat', 'dep_lng', 'arr_lat', 'arr_lng', 'dep_hour', 'month']

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
        LABEL_KEY: tf.FixedLenFeature([], tf.float32)})

    raw_data_metadata = dataset_metadata.DatasetMetadata(dataset_schema.from_feature_spec(
            feature_spec))
    return raw_data_metadata

RAW_DATA_METADATA = _create_raw_metadata()