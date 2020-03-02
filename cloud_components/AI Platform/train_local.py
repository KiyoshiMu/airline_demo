"""
You need to change some arguments based on your conditions.
"""

from ML.trainer import task
from ML.trainer import model

if __name__ == "__main__":

    ARGV2 = [
        '--train_files=gs://eoseoseos/work_dir/train*',
        '--tf_transform_dir=gs://eoseoseos/work_dir',
        '--output_dir=models',
        '--eval_files=gs://eoseoseos/work_dir/eval*',
        '--train_steps=2000',
        '--tag=flights'
        '--job_dir',
    ]

    task.main(ARGV2)
