from airline_demo.ml import preprocess
from airline_demo.ml.trainer import task

import tensorflow_transform as tft

if __name__ == "__main__":
    ARGV1 = [
        '--train-data-file=data/train.csv',
        '--test-data-file=data/eval.csv',
        '--root-train-data-out=train',
        '--root-test-data-out=eval',
        '--working-dir=work_dir'
    ]
    preprocess.main(ARGV1)

    ARGV2 = [
        '--train_files=work_dir/train*',
        '--tf_transform_dir=work_dir',
        '--output_dir=models',
        '--eval_files=work_dir/eval*',
        '--train_steps=10000',
        '--num_dnn_layers=3',
        '--first_dnn_layer_size=100',
        '--wide=False'
        # '--tag=flights'
        #  '--job_dir',
    ]
    # task.main(ARGV2)
