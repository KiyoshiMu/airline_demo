from airline_demo.ml import preprocess
from airline_demo.ml.trainer import task

import tensorflow_transform as tft

if __name__ == "__main__":
    ARGV1 = [
        '--train-data-file=data/train_local.csv',
        '--test-data-file=data/eval_local.csv',
        '--root-train-data-out=train',
        '--root-test-data-out=eval',
        '--working-dir=work_dir'
    ]
    # preprocess.main(ARGV1)

    ARGV2 = [
        '--train_files=work_dir/train*',
        '--tf_transform_dir=work_dir',
        '--output_dir=models',
        '--eval_files=work_dir/eval*',
        '--train_steps=3000',
        '--num_dnn_layers=4',
        '--first_dnn_layer_size=60',
        '--wide=True'
        # '--tag=flights'
        #  '--job_dir',
    ]
    task.main(ARGV2)
