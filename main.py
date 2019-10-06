from airline_demo.trainer import preprocess
from airline_demo.trainer import task
from airline_demo.trainer import model

import tensorflow_transform as tft

if __name__ == "__main__":
    # ARGV1 = [
    #     '--train-data-file=data/train.csv',
    #     '--test-data-file=data/val.csv',
    #     '--root-train-data-out=train',
    #     '--root-test-data-out=test',
    #     '--working-dir=work_dir'
    # ]
    # # preprocess.main(ARGV1)

    ARGV2 = [
        '--train_files=work_dir/train*',
        '--tf_transform_dir=work_dir',
        '--output_dir=models',
        '--eval_files=work_dir/test*',
        '--train_steps=200',
        # '--tag=flights'
        #  '--job_dir',
    ]

    task.main(ARGV2)
    
    # tf_transform_output = tft.TFTransformOutput('work_dir')
    # print(model.eval_input_receiver_fn(tf_transform_output))
    # fn = 'work_dir/train*'
    # print(model.get_raw_feature_spec())
    # print(model.input_fn(fn, tf_transform_output))