import argparse
import os
import logging

import tensorflow as tf
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
from tensorflow_transform.tf_metadata import schema_utils
import tensorflow_model_analysis as tfma

try:
    from airline_demo.trainer import input_metadata
except ImportError:
    import input_metadata

tf.config.optimizer.set_jit(True)

HASH_STRING_FEATURE_KEYS = input_metadata.HASH_STRING_FEATURE_KEYS
LABEL_KEY = input_metadata.LABEL_KEY
NUMERIC_FEATURE_KEYS = input_metadata.NUMERIC_FEATURE_KEYS
ORDERED_COLUMNS = input_metadata.ORDERED_COLUMNS
RAW_DATA_METADATA = input_metadata.RAW_DATA_METADATA
TO_BE_BUCKETIZED_FEATURE = input_metadata.TO_BE_BUCKETIZED_FEATURE

def get_raw_feature_spec():
    return schema_utils.schema_as_feature_spec(RAW_DATA_METADATA.schema).feature_spec

def build_estimator(config, hidden_units=None):

    real_valued_columns = [
        tf.feature_column.numeric_column(key, shape=())
        for key in NUMERIC_FEATURE_KEYS]

    categorical_columns = [
        tf.feature_column.indicator_column(
            tf.feature_column.categorical_column_with_identity(
            key, num_buckets=HASH_STRING_FEATURE_KEYS[key], default_value=0))
        for key in HASH_STRING_FEATURE_KEYS]
    
    categorical_columns.extend([
        tf.feature_column.indicator_column(
            tf.feature_column.categorical_column_with_identity(
            f'{key}_b', num_buckets=TO_BE_BUCKETIZED_FEATURE[key], default_value=0))
            for key in TO_BE_BUCKETIZED_FEATURE])

    categorical_columns.extend([
        tf.feature_column.indicator_column(
            tf.feature_column.crossed_column(['arr_lat_b', 'arr_lng_b'], 36)),
        tf.feature_column.indicator_column(
            tf.feature_column.crossed_column(['dep_lat_b', 'dep_lng_b'], 36))
    ])
    
    return tf.estimator.DNNRegressor(config=config,
                                    feature_columns=real_valued_columns+categorical_columns,
                                    hidden_units=hidden_units or [70, 50, 25])

def example_serving_receiver_fn(tf_transform_output):
    """Build the serving in inputs.

    Args:
      tf_transform_output: A TFTransformOutput.

    Returns:
      Tensorflow graph which parses examples, applying tf-transform to them.
    """
    raw_feature_spec = get_raw_feature_spec()
    raw_feature_spec.pop(LABEL_KEY)

    raw_input_fn = tf.estimator.export.build_parsing_serving_input_receiver_fn(
      raw_feature_spec, default_batch_size=None)
    serving_input_receiver = raw_input_fn()

    transformed_features = tf_transform_output.transform_raw_features(
      serving_input_receiver.features)

    return tf.estimator.export.ServingInputReceiver(
      transformed_features, serving_input_receiver.receiver_tensors)


def eval_input_receiver_fn(tf_transform_output):
    """Build everything needed for the tf-model-analysis to run the model.

    Args:
      tf_transform_output: A TFTransformOutput.

    Returns:
      EvalInputReceiver function, which contains:
        - Tensorflow graph which parses raw untranformed features, applies the
        tf-transform preprocessing operators.
        - Set of raw, untransformed features.
        - Label against which predictions will be compared.
  """
  # Notice that the inputs are raw features, not transformed features here.
    raw_feature_spec = get_raw_feature_spec()

    serialized_tf_example = tf.placeholder(
      dtype=tf.string, shape=[None], name='input_example_tensor')

    # Add a parse_example operator to the tensorflow graph, which will parse
    # raw, untransformed, tf examples.
    features = tf.parse_example(serialized_tf_example, raw_feature_spec)

    # Now that we have our raw examples, process them through the tf-transform
    # function computed during the preprocessing step.
    transformed_features = tf_transform_output.transform_raw_features(features)

    # The key name MUST be 'examples'.
    receiver_tensors = {'examples': serialized_tf_example}

    # NOTE: Model is driven by transformed features (since training works on the
    # materialized output of TFT, but slicing will happen on raw features.
    features.update(transformed_features)
    return tfma.export.EvalInputReceiver(
        features=features,
        receiver_tensors=receiver_tensors,
        labels=transformed_features[LABEL_KEY])

def input_fn(filenames, tf_transform_output, batch_size=200):
    """Generates features and labels for training or evaluation.

    Args:
        filenames: [str] list of CSV files to read data from.
        tf_transform_output: A TFTransformOutput.
        batch_size: int First dimension size of the Tensors returned by input_fn

    Returns:
        A (features, indices) tuple where features is a dictionary of
            Tensors, and indices is a single Tensor of label indices.
    """
    transformed_feature_spec = (
            tf_transform_output.transformed_feature_spec().copy())

    dataset = tf.data.experimental.make_batched_features_dataset(
        filenames, batch_size, transformed_feature_spec, reader=tf.data.TFRecordDataset)

    transformed_features = dataset.make_one_shot_iterator().get_next()
    # We pop the label because we do not want to use it as a feature while we're
    # training.
    label = transformed_features.pop(LABEL_KEY)
    return (transformed_features, label)