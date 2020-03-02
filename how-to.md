summary: This tutorial describes a toy project based on Google Cloud Platform.
id: Avoid-Late
status: Published 
authors: Yewtsing
Feedback Link: https://moo-yewtsing.github.io/

# Avoid-Late Prediction: A Toy Project
<!-- ------------------------ -->
## Overview 
Duration: 2

This tutorial describes a toy project based on Google Cloud Platform.

Imagine you are on an airplane and you're going to a remote place where you will have a super important meeting. If you'll be late, it's better to cancel it early (when the wheels get off) than letting the CEO or CTO wait for your arrival, annoying that you dare not to respect them.

Still, it's acceptable for someone to be late for **less than 10 minutes**. After all, it's understandable that as a non-machine, it's hard to be punctual every time. As a result, you wonder if it's possible that you can predict whether you will be too late. Then, you can have as many not-too-late conferences as possible, that would be so great!

This project's goal is to make this vision become a reality. you'll build a deep-learning model that can give you a suggestion about whether to cancel a meeting.

You will use the flight data in 2018 from [Transtats](https://www.transtats.bts.gov). You will leverage the Google Cloud Platform to complete the tasks below:

1. Build micro-servers that can automatically download and clean data monthly
2. Batch download data of 2018 and processing them
3. Randomly sample 80% data to build a deep-learning model
4. Use half of the rest data to evaluate the model
5. Deploy the model

I know you may want to try it first. [Here](https://forms.gle/SYNVKpKsd7vy3qYT6) you go.

### What You’ll Learn

- how to design an architecture in micro-servers way
- how to create a schedular to kick off a pipeline automatically
- how to write & deploy codes in Python and Javascript on Google Functions
- how to create a customized dataflow template
- how to train models in different extend of control to improve the performance
- how to deploy the model
- other stuff

<!-- ------------------------ -->
## Architecture
Duration: 2

In this part, you will learn how to design an architecture in micro-servers way.

### Data Collection

![Automatically downloading](data/imgs/airweb.jpg)

You will use Cloud Schedule, Cloud Pubsub, Cloud Functions, and Cloud Dataflow as microservers together to perform monthly auto-downloading, processing, and saving tasks.

The Cloud Scheduler will initiate a message sending to Cloud Pubsub's "monthly-reminder" topic. When the message comes, it'll trigger a Cloud Function, which will download the latest monthly data from the Transtats, unzip it, change the name and save it as CSV file to the Cloud Storage. When the new CSV file successfully saved in Cloud Storage. It'll trigger another Cloud Function, which will submit a job to Cloud Dataflow. Then Cloud Dataflow will digest the new data, do some transformation and finally, sink it into Bigquery, the data warehouse.

### Model Training

![Model Training](data/imgs/model_train.jpg)

To train a machine learning model is just like cooking. If there is a recipe, you should try the existing recipe first. To reinvent pizza is not fun and the new pizza may not as tasty as the "experienced" pizza. 

In the AI Platform, there are three build-in algorithms. Understand the basic settings of machine learning job, and you can leverage the "magic" power of Google AI. After that, it's time to write your code and to see whether you can beat Google's automation.

### Model Deployment

![Model Deployment](data/imgs/deploy.jpg)

You will use the Google Sheet as the platform and AI-platform as the backend to deploy your model.

## Setup
Duration: 1

You need to do the following things and you can find plenty tutorials about how to complete these tasks.

1. Create a Project
1. Launch Google Cloud Shell
1. Create Virtual Environment via Miniconda
1. Clone the Git Repo
1. Enable the related APIs, including Cloud Functions, Cloud Dataflow, Cloud Compute Engine, Cloud AI Platform Training & Prediction. More APIs may be needed to enable when you follow the coming steps.

You may need to use following line to create virtual environment.

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O mini.sh

bash mini.sh
```

You need to type "yes" for the initialization.

```bash
installation finished.
Do you wish the installer to initialize Miniconda3
by running conda init? [yes|no]
[no] >>> yes
```

*Restart your Cloud Shell*. Your Cloud Shell should look like

```bash
(base) bunncebunny@cloudshell:~ (avoid-late)$
```

Create a new virtual environment and export parameters.

```bash
conda create -n [YOUR_ENV_NAME] python=3.7
conda activate [YOUR_ENV_NAME]
export BUCKET_ID = [YOUR_BUCKET_ID e.g. airairair]
export PROJECT_ID = [YOUR_PROJECT_ID e.g. avoid-late]
```

For example,

```bash
conda create -n nolate python=3.7
conda activate nolate
export BUCKET_ID=airairair
export PROJECT_ID=avoid-late
```

And you can see your Cloud Shell looks like:

```bash
(nolate) bunncebunny@cloudshell:~ (avoid-late)$
```

Then, clone the Repo.

```bash
git clone https://github.com/Moo-YewTsing/airline_demo.git
```

## Data Collection - Batch download
Duration: 10

In the Architecture section, you learned the design of architecture, and the data collection is designed to be monthly conducted. Here, it's different. You will perform batch downloading first to gather enough data.

In the _Cloud Shell_, use command lines to check the related scripts.

```bash
cd airline_demo/cloud_components/Cloud\ Shell/
cat batch_download.py
cat month_download.py
```

Then you need to create a bucket to store your data.

```bash
gsutil mb gs://$BUCKET_ID
```

And then, you should probably set up authentication by setting up the GOOGLE_APPLICATION_CREDENTIALS environment variable, like below. (The format of service account is [SA-NAME]@[PROJECT-ID].iam.gserviceaccount.com)

```bash
gcloud iam service-accounts create downloader \
    --description "a service-accounts for downloader" \
    --display-name "downloader"

gcloud projects add-iam-policy-binding avoid-late \
  --member serviceAccount:downloader@avoid-late.iam.gserviceaccount.com \
  --role roles/storage.admin

gcloud iam service-accounts keys create ~/downloader_key.json \
  --iam-account downloader@avoid-late.iam.gserviceaccount.com

export GOOGLE_APPLICATION_CREDENTIALS=~/downloader_key.json
```

In the _Cloud Shell_, use command lines to download the data you need based on time range **and change the bucket argument based on you conditions.** Like:

```bash
python3 batch_download.py --start 2018-01 --end 2019-12 --bucket BUCKET_ID
```

Then, it should download the data which are in the range, and the logging will show some messages like:

```bash
2020-02-10 15:03:47,076 - month - INFO - /tmp/tmpqhdctzyq/2018-01.zip download completed

2020-02-10 15:03:47,855 - month - INFO - Unzip completed to /tmp/tmpqhdctzyq/2018-01.csv

2020-02-10 15:03:51,027 - month - INFO - /tmp/tmpqhdctzyq/2018-01.csv upload completed
```

### Dataflow

Check the Dataflow template.

```bash
cd ~/airline_demo/cloud_components/Dataflow
cat flightFlow.py
```

First staging the Customized Template.

```bash
python -m flightFlow \
--runner DataflowRunner \
--project $PROJECT_ID \
--staging_location gs://$BUCKET_ID/staging \
--temp_location gs://$BUCKET_ID/temp \
--template_location gs://$BUCKET_ID/templates/flightFlow \
--setup_file ./setup.py \
--experiments=use_beam_bq_sink \
```

Create datasets in Bigquery and upload needed file.

```bash
bq mk flights
bq mk flights.rawFlights
gsutil cp airports.csv gs://$BUCKET_ID/
```

Then, start the dataflow cluster.

```bash
gcloud dataflow jobs run flightFlow \
 --gcs-location=gs://$BUCKET_ID/templates/flightFlow \
 --staging-location=gs://$BUCKET_ID/temp \
 --parameters=input="gs://$BUCKET_ID/tmp/csvs/*",output="$PROJECT_ID:flights.rawFlights",airport="gs://$BUCKET_ID/airports.csv" \
 --zone=us-central1-f
```

After about 20 mins, in console, you should see the result as following.

![dataflow clean](data/imgs/dfclean.PNG)

For more information, please check the [document](https://cloud.google.com/dataflow/docs/guides/templates/running-templates).

## Data Collection - Monthly download
Duration: 10

### Cloud Scheduler

First, create a new bucket where you will store the monthly downloaded data, so that you can later trigger Cloud Function without influence from other operation.

```bash
export DOWNLOAD_BUCKET=tmptmptmptmp

gsutil mb gs://$DOWNLOAD_BUCKET
```

Then, create a Pub/sub topic. Then, use the Cloud Scheduler to set a monthly job which sends a message to the topic. The content of the message, i.e. **Payload** is the **name** of a _bucket_ you created to store the data. If you are new to this, please check the [document](https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules?authuser=3&_ga=2.225359640.-367087609.1574295701#defining_the_job_schedule)

You may need to enabling service [appengine.googleapis.com].

For command lines, they look like as below.

```bash
gcloud pubsub topics create monthMsg

gcloud scheduler jobs create pubsub monthTrigger \
--schedule="* * 15 * *" --topic=monthMsg \
--message-body=$DOWNLOAD_BUCKET --time-zone="EST"
```

### Cloud Functions - Download

```bash
cd ~/airline_demo/cloud_componets/Cloud\ Functions/
```

Look at the file named auto_month.py.

```bash
cat auto_month.py
```

Create a Cloud Functions, which is triggered by the topic your job in Cloud Scheduler sends to.

You may want to change the year in the code, which is for the use in 2019 to avoid no data to download for the required month.

```python
date = date.replace(year=2018)
```

In Console, Select _Runtime_ as _Python 3.7_ and copy the codes in this file to the main.py field. Also, put _google-cloud-storage_ as a new line in the _requirements.txt_ field.

Or in Cloud Shell, use the lines below.

```bash
gsutil cp monthDownload.zip gs://$BUCKET_ID

gcloud functions deploy monthDownload --entry-point main --runtime python37 --trigger-topic monthMsg \
--source=gs://$BUCKET_ID/monthDownload.zip \
--service-account downloader@avoid-late.iam.gserviceaccount.com \
--memory=512MB \
--allow-unauthenticated
```

### Cloud Functions - Kick Dataflow

First, you need to set a service account for this Cloud Functions. The role for it is Dataflow/Admin. Maybe, you need to check this [document](https://cloud.google.com/iam/docs/creating-managing-service-accounts), and find the command lines below useful.

```bash
gcloud iam service-accounts create dfrunner \
    --description "a service-accounts for dataflow" \
    --display-name "dataflow_runner"

gcloud projects add-iam-policy-binding avoid-late \
  --member serviceAccount:dfrunner@avoid-late.iam.gserviceaccount.com \
  --role roles/dataflow.admin
```

Check the file named [kickflow.js]

Create a Cloud Functions, which is triggered by the _Finalize/Create_ event of Cloud Storage. The bucket is the one you created to store the data. You should have used it as the content of message sent by Cloud Scheduler before.

Select _Runtime_ as _Node.js 8_ (The API for Python is documented poorly, so I prefer to use Node.js here) and copy the codes in this file to the index.js field. Also, put _"googleapis": "^47.0.0"_ as a new line in the _packages.json_'s dependencies.

The following parts are hard coded, please make sure you have changed them.

```js
const output = "avoid-late:flights.rawFlights";
const airport = "gs://airairair/airports.csv";
const TEMPLATE_BUCKET = "airairair";
const TEMPLATE_NAME = "flightFlow";

const request = {
  projectId: "avoid-late"
};
```

```bash
cp kickflow.js index.js

zip kickflow.zip index.js package.json

gsutil cp kickflow.zip gs://$BUCKET_ID/

gcloud functions deploy kickFlow --entry-point main --runtime nodejs8 --trigger-bucket $DOWNLOAD_BUCKET \
--source=gs://$BUCKET_ID/kickflow.zip \
--service-account dfrunner@avoid-late.iam.gserviceaccount.com \
--allow-unauthenticated
```

To test the pipeline, you can manually trigger the scheduler

```bash
gcloud scheduler jobs run monthTrigger
```

In the Bucket page, you should see.

![bucket](data/imgs/bucket.PNG)

And in the Dataflow page, you should see.

![newFlow](data/imgs/newflow.PNG)

## BigQuery
Duration: 10

Bigquery is the data warehouse, and its cost is equivalent to the cost of Google Storage. As a result, the cost will not increase even if you store the data in this place where you can interact with the data.

What is the meaning of "interact with the data"? In Bigquery, you can query questions you am interested in, like whether some carriers are more likely to have a late arrival or whether seasons can influence the flights' delay. Besides, the data can be loaded into Data Studio, a visualization tool based on Bigquery.

Here, I challenge you to find the results of these questions like below, to show the type of carrier, the season, the distance, and the locations have relations with the delay.

![Data Studio answers my questions](data/imgs/flight_report.jpg)

Moreover, it's not a static visualization tool. You can "query" data by touching these icons. For example, you can select a specific date range. All the visualization will change automatically as a result.

![Data Studio change](data/imgs/studio_change.png)

From the exploration in Data Studio, you may notice that the carriers, locations, department delay, distance, and DateTime more or less have to influence on the delay of the flight. Therefore, you can select related features, including 'arr_lat', 'dep_lat', 'dep_lng', 'arr_lng', 'DEP_DELAY', 'DISTANCE', 'hour', 'month', and 'MKT_UNIQUE_CARRIER' to train a sophisticated model.

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

After saving the result into Google Storage, you can start the ML part. To move the BigQuery tables into the bucket, you may find the following lines useful. The following line only show how to save the testing part. Can you find the solution to save the training part?

```bash
bq extract --destination_format CSV --noprint_header flight.test gs://$BUCKET_ID/for_ai/test/flight-*.csv
```

## Built-in ML (Try to learn by yourself)
Duration: 10

The three algorithms are XGBoost, wide and deep model, and linear learner model. XGBoost tree is a kind of traditional machine learning model. Like random forest tree, it's based on decision tree ensembles, which combine the results of multiple classifications and regression models. The wide and deep model combines a linear model that learns and "memorizes" a wide range of rules, with a deep neural network that "generalizes" the rules. A linear learner model assigns one weight to each input feature and sums the weights to predict a numerical target value. See more [details](<[https://cloud.google.com/ml-engine/docs/algorithms/overview](https://cloud.google.com/ml-engine/docs/algorithms/overview)>).

Their final evaluation results are compared as follows. Strangely, this final evaluation in logging for build-in algorithms seems only to use part of the evaluation data. As a result, later, you need to evaluate the best built-in models by using all the evaluation data.

|         Type         | Final AUC Evaluation |
| :------------------: | :------------------: |
|       XGBoost        |        0.865         |
| Linear learner model |        0.923         |
| Wide and deep model  |        0.961         |

It's clear the wide and deep model performs the best here. Now, it's time to write you code and to see whether you could beat Google's automation.

## Preprocess
Duration: 10

CD to the _AI Platform_ folder. Have a look at the scripts inside.

Here, first, you will use Tensorflow Transform, which is an extension for data processing. It has API to handle Feature tuning (e.g., scaling and normalizing numeric values), Representation transformation (e.g., bucketization), Feature construction (e.g., feature crossing), and so on. Besides, it can use dataflow to run the processing job. As we mentioned before, dataflow can deal with TB-level jobs as well as MB-level jobs. Thus, it would be time-effective to make data processed and ready for training. Moreover, it will attach transformations to the exported model. As a result, it resolves the annoying preprocessing challenge -- the training-serving skew, a difference between the predictive performance of training and serving. It's largely caused by the discrepancy between how data are handled in the training and the serving pipelines. For more information see [Here](https://cloud.google.com/solutions/machine-learning/data-preprocessing-for-ml-with-tf-transform-pt1).

Here, for numeric data, like "DEP_DELAY" and "DISTANCE", the transformation normalizes them to "0-1". For string feature, like "MKT_UNIQUE_CARRIER", it first hashes them to value and make them one-hot-encoding. For value like "latitude", "longitude", "hour" and "month," it puts them into buckets so that these features' categorial information can be represented. Last, it makes a new feature by crossing "latitude" and "longitude" and bucketizating them.

```python
    # Scale numeric columns to have range [0, 1]
    for key in NUMERIC_FEATURE_KEYS:
        outputs[key] = tft.scale_to_0_1(inputs[key])

    for key in NUMERIC_FEATURE_KEYS_INT:
        outputs[key] = tft.scale_to_0_1(inputs[key])

    # Bucketize numeric columns
    for key in TO_BE_BUCKETIZED_FEATURE:
        outputs[f'{key}_b'] = tft.bucketize(
            inputs[key],
            TO_BE_BUCKETIZED_FEATURE[key]
        )

    for key in HASH_STRING_FEATURE_KEYS:
        outputs[key] = tft.hash_strings(inputs[key], HASH_STRING_FEATURE_KEYS[key])
```

In the _AI Platform_ folder. And change the hard code parameters of main_preprocess.py.

```python
ARGV1 = [
        '--train-data-file=gs://[BUCKET_ID]/for_ai/train*',
        '--test-data-file=gs://[BUCKET_ID]/for_ai/eval*',
        '--working-dir=gs://[BUCKET_ID]/work_dir',
        '--project=[PROJECT_ID]'
        ]
```

Then, create GOOGLE_APPLICATION_CREDENTIALS for the preprocess runner, and run it.

```bash
gcloud iam service-accounts keys create ~/dfRunner_key.json \
  --iam-account dfrunner@avoid-late.iam.gserviceaccount.com

export GOOGLE_APPLICATION_CREDENTIALS=~/dfRunner_key.json

python main_preprocess.py
```

In the console for Dataflow, you should see the process like below.

![Customed Transformation](data/imgs/prep.jpg)

## Training
Duration: 10

For the hyper-parameters, including the number of layers and the first hidden layer's size, you can write a .yaml file, like below. Then AI Platform will use its algorithm to search the best combination of these values. Wait for less than one hour, and you can see your result. Luckily, mine is better than Google's automatic data processing and model creation.

```yaml
trainingInput:
  hyperparameters:
    goal: MAXIMIZE
    hyperparameterMetricTag: auc
    maxTrials: 6
    maxParallelTrials: 2
    enableTrialEarlyStopping: True
    params:
      - parameterName: first_dnn_layer_size
        type: INTEGER
        minValue: 10
        maxValue: 100
        scaleType: UNIT_LINEAR_SCALE
      - parameterName: num_dnn_layers
        type: INTEGER
        minValue: 3
        maxValue: 6
        scaleType: UNIT_LINEAR_SCALE
```

CD to the ML folder and run the following lines.

```bash
DATE=`date '+%Y%m%d_%H%M%S'`
    export JOB_NAME=flight_$DATE
    export GCS_JOB_DIR=gs://eoseoseos/jobs/$JOB_NAME

gcloud ai-platform jobs submit training $JOB_NAME \
                                --stream-logs \
                                --runtime-version 1.15 \
                                --python-version 3.7 \
                                --config ./hptuning_config.yaml \
                                --staging-bucket gs://$BUCKET_ID \
                                --module-name trainer.task \
                                --package-path trainer \
                                --region us-central1 \
                                --project $PROJECT_ID \
                                -- \
                                --train_steps 20000 \
                                --tf_transform_dir gs://$BUCKET_ID/work_dir \
                                --output_dir gs://$BUCKET_ID/models \
                                --train_files gs://$BUCKET_ID/work_dir/train* \
                                --eval_files gs://$BUCKET_ID/work_dir/eval*
```

![HyperTune Results](data/imgs/mymodel.jpg)

![Best Model](data/imgs/final_auc.jpg)

Here, we can do model examinations so that we can know the pro and cons of the model and then can "targeted" refined the model. For example, use the What-if tool ([https://pair-code.github.io/what-if-tool/](https://pair-code.github.io/what-if-tool/)). However, from my experiences, this tool is not friendly with the AI Platform. Also, it's not directly related to GCP. So, I skip this part.

Then, it's time to deploy the model.

## Deploy
Duration: 10

Simply click the "Deploy Model". Then GCP will deploy it on ML engines and handle all the other processes.

Here, it's worth to notice that to invoke a model deployed on Google ML engines, a role for "ML Engine Developer" is [needed](https://cloud.google.com/ml-engine/docs/access-control). A helpful practice is to create a server account with this role and grant this server account to a microserver on Google Functions.

Since the model here is created by Tensorflow estimator, you need to encode the data first and then you can send a request to ask the prediction from the model.

Go to the _Models_ in AI Platform and you can easily deploy the model which perform the best. Do you see the argument *--config ./hptuning_config.yaml* above? It a config file for the AI Platform to search the best hyperparameter, under your defined range though.

Then, you need yet another glue, Google Functions. You may notice the kick_predict.py file in Cloud Functions folder. Copy the code into the main.py part in the interface as usual and add the dependencies, which show at the start of the file. This function is used to encode the data and call the model deployed on AI Platform. Because, the model is on AI Platform, you need to add the role _ML/Admin_ to the Google Functions.

Finally, you can use 101 ways to call the Google Functions to make predictions. Just make sure when call the REST API, you need to format the data as something like the following one:
{"instances":["UA,-17.0,654.0,40.65222222,-75.44055556,41.97444444,-87.90666667,7,11"]}.

You can chose Google Sheet as the platform to create a demo APP. Google Sheet has the database, UI, security, and loads of built-in APIs. To match this platform, you can use Google Forms as the APP UI.

Data are collected from Google Forms. After the user submits a form, it will trigger an "App Script" you can write in Google Sheet, it should convert the information from the user into a single payload and call the microserver on Google Functions. The microserver will transform the data in the payload to the format my model accepts. Why it's needed? Because by doing so, you can hide the secret of my model. Also, it's a separation of concerns. Later, the Google Sheet will receiver the prediction result from my model and send an e-mail based on the outcome. If the model predicts the user will not be too late for the meeting, the e-mail will encourage the user to keep the meeting. Otherwise, it will suggest the user cancel the meeting.

```js
function sendMail(emailAddress, belate) {
  if (belate) {
    var subject = "We Suggest You Cancel Your Meeting";
    var message =
      "You are very likely not able to have your meeting in time. DO IT in next time. Good Luck!";
  } else {
    var subject = "We Suggest You Keep Your Meeting";
    var message =
      "You are very likely to have your meeting in time. Enjoy this Meeting! Good Luck!";
  }
  MailApp.sendEmail(emailAddress, subject, message);
}
```

![Sample Msg](data/imgs/msg.jpg)

**Cool?**

Cheers!
