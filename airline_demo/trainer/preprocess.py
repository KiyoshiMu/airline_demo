import argparse
import os
import logging

import apache_beam as beam
import tensorflow as tf
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam

from airline_demo.trainer import input_metadata

HASH_STRING_FEATURE_KEYS = input_metadata.HASH_STRING_FEATURE_KEYS
LABEL_KEY = input_metadata.LABEL_KEY
NUMERIC_FEATURE_KEYS = input_metadata.NUMERIC_FEATURE_KEYS
ORDERED_COLUMNS = input_metadata.ORDERED_COLUMNS
RAW_DATA_METADATA = input_metadata.RAW_DATA_METADATA
TO_BE_BUCKETIZED_FEATURE = input_metadata.TO_BE_BUCKETIZED_FEATURE

class RemoveNull(beam.DoFn):
    def process(self, line):
        items = line.split(',')
        if all(item for item in items):
            yield line
        else:
            logging.error(f'data incomplete: {line}')

def preprocessing_fn(inputs):
    """Preprocess input columns into transformed columns."""
    outputs = {}

    # Scale numeric columns to have range [-1, 1].
    for key in NUMERIC_FEATURE_KEYS:
        outputs[key] = tft.scale_to_z_score(inputs[key])

    # bucketize numeric columns
    for key in TO_BE_BUCKETIZED_FEATURE:
        outputs[f'{key}_b'] = tft.bucketize(
            inputs[key],
            TO_BE_BUCKETIZED_FEATURE[key]
        )

    for key in HASH_STRING_FEATURE_KEYS:
        outputs[key] = tft.hash_strings(inputs[key], HASH_STRING_FEATURE_KEYS[key])

    # For the label column we transform it either 0 or 1 if there are row leads
    outputs[LABEL_KEY] = _convert_label(inputs[LABEL_KEY])
    return outputs

def _convert_label(label):

    return tf.dtypes.cast(label > 10, tf.int64)

# Functions for preprocessing
def transform_data(train_data_file,
                   test_data_file,
                   working_dir,
                   root_train_data_out,
                   root_test_data_out,
                   pipeline_options):
    """Transform the data and write out as a TFRecord of Example protos.
    Read in the data using the CSV reader, and transform it using a
    preprocessing pipeline that scales numeric data and converts categorical data
    from strings to int64 values indices, by creating a vocabulary for each
    category.
    Args:
        train_data_file: File containing training data
        test_data_file: File containing test data
        working_dir: Directory to write transformed data and metadata to
        root_train_data_out: Root of file containing transform training data
        root_test_data_out: Root of file containing transform test data
        pipeline_options: beam.pipeline.PipelineOptions defining DataFlow options
    """

    # The "with" block will create a pipeline, and run that pipeline at the exit
    # of the block.
    with beam.Pipeline(options=pipeline_options) as pipeline:
        tmp_dir = pipeline_options.get_all_options()['temp_location']
        with tft_beam.Context(tmp_dir):

            converter = tft.coders.csv_coder.CsvCoder(ORDERED_COLUMNS, 
                                RAW_DATA_METADATA.schema)
            raw_data = (
                    pipeline
                    | 'Train:ReadData' >> beam.io.ReadFromText(train_data_file, 
                                                               skip_header_lines=1)
                    | 'Train:RemoveNull' >> beam.ParDo(RemoveNull())
                    | 'Train:Decode' >> beam.Map(converter.decode))

            raw_dataset = (raw_data, RAW_DATA_METADATA)
            transformed_dataset, transform_fn = (
                    raw_dataset | tft_beam.AnalyzeAndTransformDataset(preprocessing_fn))
            transformed_data, transformed_metadata = transformed_dataset

            # the important part
            transformed_data_coder = tft.coders.ExampleProtoCoder(transformed_metadata.schema)

            _ = transformed_data | 'Train:WriteData' >> beam.io.WriteToTFRecord(
                    os.path.join(working_dir, root_train_data_out),
                    coder=transformed_data_coder)

            raw_test_data = (
                    pipeline
                    | 'Test:ReadData' >> beam.io.ReadFromText(test_data_file,
                                                              skip_header_lines=1)
                    | 'Test:RemoveNull' >> beam.ParDo(RemoveNull())
                    | 'Test:DecodeData' >> beam.Map(converter.decode))

            raw_test_dataset = (raw_test_data, RAW_DATA_METADATA)

            transformed_test_dataset = (
                    (raw_test_dataset, transform_fn) | tft_beam.TransformDataset())
            # Don't need transformed data schema, it's the same as before.
            transformed_test_data, _ = transformed_test_dataset

            _ = transformed_test_data | 'Test:WriteData' >> beam.io.WriteToTFRecord(
                    os.path.join(working_dir, root_test_data_out),
                    coder=transformed_data_coder)

            # Will write a SavedModel and metadata to two subdirectories of
            # working_dir, given by transform_fn_io.TRANSFORM_FN_DIR and
            # transform_fn_io.TRANSFORMED_METADATA_DIR respectively.
            _ = (
                transform_fn
                | 'WriteTransformFn' >>
                tft_beam.WriteTransformFn(working_dir))

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--train-data-file',
        help='Path to training data',
        required=True
    )
    parser.add_argument(
        '--test-data-file',
        help='Path to test data',
        required=True
    )
    parser.add_argument(
        '--root-train-data-out',
        help='Root for files with train data',
        required=True
    )
    parser.add_argument(
        '--root-test-data-out',
        help='Root for files with test data',
        required=True
    )
    parser.add_argument(
        '--working-dir',
        help='Path to the directory where transformed data are written',
        required=True
    )

    args, pipeline_args = parser.parse_known_args(argv)

    if '--temp_location' not in pipeline_args:
        pipeline_args = pipeline_args + ['--temp_location',
        os.path.join(args.working_dir, 'tmp')]

    if '--staging_location' not in pipeline_args:
        pipeline_args = pipeline_args + ['staging_location',
        os.path.join(args.working_dir, 'tmp', 'staging')]

    pipeline_options = beam.pipeline.PipelineOptions(pipeline_args)

    transform_data(train_data_file=args.train_data_file,
                                 test_data_file=args.test_data_file,
                                 working_dir=args.working_dir,
                                 root_train_data_out=args.root_train_data_out,
                                 root_test_data_out=args.root_test_data_out,
                                 pipeline_options=pipeline_options)

