"""
You need to change some arguments based on your conditions.
"""

import time
from ML import preprocess

if __name__ == "__main__":
    ARGV1 = [
        '--train-data-file=gs://eoseoseos/for_ai/train*',
        '--test-data-file=gs://eoseoseos/for_ai/eval*',
        '--root-train-data-out=train',
        '--root-test-data-out=eval',
        '--working-dir=gs://eoseoseos/work_dir',
        '--runner=DataflowRunner',
        '--region=us-central1',
        f'--job_name=preprocess{int(time.time())}',
        '--setup_file=./setup.py',
        # '--flexrs_goal=COST_OPTIMIZED',
        '--project=eeeooosss'
    ]
    preprocess.main(ARGV1)
