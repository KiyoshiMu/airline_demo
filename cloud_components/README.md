# Clarification

## For batch test

### batch_download

In the _Cloud Shell_ folder, use command lines to download the data you need based on time range. Like:

```bash
python3 batch_download.py --start 2018-01 --end 2019-12 --bucket eoseoseos
```

Then, it should download the data which are in the range, and the logging will show some messages like:

```bash
2020-02-10 15:03:47,076 - month - INFO - /tmp/tmpqhdctzyq/2018-01.zip download completed

2020-02-10 15:03:47,855 - month - INFO - Unzip completed to /tmp/tmpqhdctzyq/2018-01.csv

2020-02-10 15:03:51,027 - month - INFO - /tmp/tmpqhdctzyq/2018-01.csv upload completed
```

### Dataflow

First staging the Customized Template.

You should see some commended lines at the beginning of flightFlow.py in the Dataflow folder. Please change the argument based on you conditions.

```bash
python3 -m flightFlow \
--runner DataflowRunner \
--project eeeooosss \
--staging_location gs://eoseoseos/staging \
--temp_location gs://eoseoseos/temp \
--template_location gs://eoseoseos/templates/flightFlow \
--setup_file ./setup.py \
--experiments=use_beam_bq_sink \
```

Then, you can go to the Web Console and use the **customer templates**. Fill the **Additional parameters**, including _input_, _output_ and _airport_. For more information, please check the [document](https://cloud.google.com/dataflow/docs/guides/templates/running-templates).

### BigQuery

Do what you want to explore this data.

## For monthly automate update

### Cloud Scheduler

First, create a Pub/sub topic. Then, use the Cloud Scheduler to set a monthly job which sends a message to the topic. The content of the message is the **name** of a _bucket_ you created to store the data. If you are new to this, please check the [document](https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules?authuser=3&_ga=2.225359640.-367087609.1574295701#defining_the_job_schedule)

### Cloud Functions - Download

In the Could Functions folder, you can find a file named _auto_month.py_. Create a Cloud Functions, which is triggered by the topic your job in Cloud Scheduler sends to.

Select _Runtime_ as _Python 3.7_ and copy the codes in this file to the main.py field. Also, put _google-cloud-storage_ as a new line in the _requirements.txt_ field.

### Cloud Functions - Kick Dataflow

Again, in the Could Functions folder, you can find a file named _kickflow.js_. Create a Cloud Functions, which is triggered by the _Finalize/Create_ event of Cloud Storage. The bucket is the one you created to store the data. You should have used it as the content of message sent by Cloud Scheduler before.

Select _Runtime_ as _Node.js 8_ (The API for Python is documented poorly, so I prefer to use Node.js here) and copy the codes in this file to the index.js field. Also, put _"googleapis": "^47.0.0"_ as a new line in the _packages.json_'s dependencies.

The following parts are hard coded, please make sure you have changed them.

```js
const output = "eeeooosss:flight.rawflight";
const airport = "gs://eoseoseos/tmp/airports.csv";
const TEMPLATE_BUCKET = "eoseoseos";
const TEMPLATE_NAME = "flightFlow";

const request = {
  projectId: "eeeooosss"
};
```

Besides, you need to set a service account for this Cloud Functions. The role for it is Dataflow/Admin. Maybe, you need to check this [document](https://cloud.google.com/iam/docs/creating-managing-service-accounts).

### BigQuery

If you manually run the Cloud Scheduler, you can see the data would be download, upload and cleaned. Then you can check them in the BigQuery.

After exploration, I found it useful to transform the data in the following ways.

```sql
SELECT
  CASE
    WHEN ARR_DELAY >= 10 THEN 1
  ELSE
  0
END
  AS cancel,
  MKT_UNIQUE_CARRIER,
  DEP_DELAY,
  DISTANCE,
  DEP_AIRPORT_LAT AS dep_lat,
  DEP_AIRPORT_LON AS dep_lng,
  ARR_AIRPORT_LAT AS arr_lat,
  ARR_AIRPORT_LON AS arr_lng,
  EXTRACT(MONTH
  FROM
    FL_DATE) AS month,
  EXTRACT (HOUR
  FROM
    DEP_TIME) AS hour
FROM
  `eeeooosss.flight.rawflight`
WHERE
  MOD(ABS(FARM_FINGERPRINT(CAST(FL_DATE AS STRING))),10) < 8
  -- the line above is to separate the data for training and testing.
```

After saving the result into Google Storage, you can start the ML part. To move the BigQuery tables into the bucket, you may find the following lines useful.

```bash
bq extract --destination_format CSV --noprint_header flight.test gs://eoseoseos/for_ai/test/flight-*.csv
```

## ML

### Preprocess

CD to the _AI Platform_ folder.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=[Your service account .json file]
python3 main_preprocess.py
```

### Training

CD to the ML folder.

```bash
DATE=`date '+%Y%m%d_%H%M%S'`
    export JOB_NAME=flight_$DATE
    export GCS_JOB_DIR=gs://eoseoseos/jobs/$JOB_NAME

    gcloud ai-platform jobs submit training $JOB_NAME \
                                    --stream-logs \
                                    --runtime-version 1.15 \
                                    --python-version 3.7 \
                                    --config ./hptuning_config.yaml \
                                    --staging-bucket gs://eoseoseos \
                                    --module-name trainer.task \
                                    --package-path trainer \
                                    --region us-central1 \
                                    --project eeeooosss \
                                    -- \
                                    --train_steps 20000 \
                                    --tf_transform_dir gs://eoseoseos/work_dir \
                                    --output_dir gs://eoseoseos/models \
                                    --train_files gs://eoseoseos/work_dir/train* \
                                    --eval_files gs://eoseoseos/work_dir/eval*
```

### Deploy

Since the model here is created by Tensorflow estimator, you need to encode the data first and then you can send a request to ask the prediction from the model.

Go to the _Models_ in AI Platform and you can easily deploy the model which perform the best. Do you see the argument _--config ./hptuning_config.yaml_ above? It a config file for the AI Platform to search the best hyperparameter, under your defined range though.

Then, you need yet another glue, Google Functions. You may notice the kick_predict.py file in Cloud Functions folder. Copy the code into the main.py part in the interface as usual and add the dependencies, which show at the start of the file. This function is used to encode the data and call the model deployed on AI Platform. Because, the model is on AI Platform, you need to add the role _ML/Admin_ to the Google Functions.

Finally, you can use 101 ways to call the Google Functions to make predictions. Just make sure when call the REST API, you need to format the data as something like the following one:
{"instances":["UA,-17.0,654.0,40.65222222,-75.44055556,41.97444444,-87.90666667,7,11"]}.

**Cool?**

Cheers!
