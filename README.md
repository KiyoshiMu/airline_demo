# airplane_demo
A demo for a toy project based on Google Cloud

Image you are on a airplane and you're going to a far place where you will have a super important meeting. If you're going to be late, it's better to cancel it early (when the wheels get off) than letting the CEO or CTO wait your arrive, annoying that you dare to not respect them. 

Still, it's acceptable for someone to be late **less than 10 minutes**. After all, all human beings understand as a non-machine it's hard to be punctual everytime. As a result, you want somehow you can predict whether you should cancel the meeting to have as many not-too-late meetings as possibe, before your airplane wheels off.

This project's goal is to build a deep-learning model that can give the you the suggestion about whether to cancel such meetings.

We will use the flight data in 2018 from [Transtats](https://www.transtats.bts.gov). We'll use Google Cloud Platform to build a pipeline to complete the tasks below:

 1. Use 80% data from every month to build a deep-learning model, which could serve real-time request
 2. Use the rest data to simulate real-time events and evaulate the model

## Development

### Simulation Data Preparison

In this part, we're preparing the data to train the goal model. 

We will use Google Cloud Platform to perform monthly auto-downloading, processing and saving task. Even though it's not necessary to have a automactic **batch** processing pipeline, because in real life the flight data arrive in real-time, we will still build it. 

We want to use it to demostrate how to create a pipeline on GCP. Also, batch data is common. Let's see the the power of GCP which can build a complex pipeline like combining lego.

#### Local

 1. Local Downloader
 2. Local Apache Beam (Time Adjustment)
 3. Local Simultaion

#### GCP

 1. Monthly Data Download: Cloud Schedule -> Cloud Pub/Sub -> Cloud Function -> Cloud VM -> Cloud Pub/Sub -> Cloud Function -> Cloud VM
 2. Data Preparison: Cloud Dataflow
 3. Data Exploration: Bigquery


### Ai-platform

(tfma makes a lot of trouble and is useless)

CLI are as below:

    DATE=`date '+%Y%m%d_%H%M%S'`
    export JOB_NAME=flight_$DATE
    export GCS_JOB_DIR=gs://linelineline/jobs/$JOB_NAME

    gcloud ai-platform local train\
                                    --module-name task.task \
                                    --package-path task \
                                    --project airlinegcp \
                                    -- \
                                    --train_steps 1000 \
                                    --tf_transform_dir gs://linelineline/work_dir \
                                    --output_dir gs://linelineline/models \
                                    --train_files gs://linelineline/work_dir/train* \
                                    --eval_files gs://linelineline/work_dir/eval*

(the cmd sequence needs to be from required to nonrequired)

    gcloud ai-platform jobs submit training $JOB_NAME \
                                    --stream-logs \
                                    --runtime-version 1.14 \
                                    --python-version 3.5 \
                                    --config ./hptuning_config.yaml \
                                    --staging-bucket gs://linelineline \
                                    --module-name trainer.task \
                                    --package-path trainer \
                                    --region us-central1 \
                                    --project airlinegcp \
                                    -- \
                                    --train_steps 50000 \
                                    --tf_transform_dir gs://linelineline/work_dir \
                                    --output_dir gs://linelineline/models \
                                    --train_files gs://linelineline/work_dir/train* \
                                    --eval_files gs://linelineline/work_dir/eval*

tensorboard --logdir=gs://linelineline/models --port=8080