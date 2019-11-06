
# Avoid-Late Prediction: A Toy Project

This article describes a toy project based on Google Cloud Platform.
  
Imagine you are on an airplane and you're going to a remote place where you will have a super important meeting. If you'll be late, it's better to cancel it early (when the wheels get off) than letting the CEO or CTO wait for your arrival, annoying that you dare not to respect them.

Still, it's acceptable for someone to be late for **less than 10 minutes**. After all, it's understandable that as a non-machine, it's hard to be punctual every time. As a result, you wonder if it's possible that you can predict whether you will be too late. Then, you can have as many not-too-late conferences as possible, that would be so great!

This project's goal is to make this vision become a reality. I'll build a deep-learning model that can give you a suggestion about whether to cancel a meeting.

I used the flight data in 2018 from [Transtats](https://www.transtats.bts.gov). I leveraged the Google Cloud Platform to complete the tasks below:

1. Build microservers that can automatically download and clean data monthly
2. Batch download data of 2018 and processing them
3. Randomly sample 80% data to build a deep-learning model
4. Use half of the rest data to evaluate the model
5. Deploy the model
  
I know you may want to try it first. [Here](https://forms.gle/SYNVKpKsd7vy3qYT6) you go.
  
## Development

### Microservers achieve automatically downloading

In this part, I prepared the data to train the goal model. Below is the diagram of this part.
  
![Automatically downloading](https://drive.google.com/uc?id=1fajRqY0bPMYb_jSK3fFIqJ6j0gE2-Clm)
  
I used Cloud Schedule, Cloud Pubsub, Cloud Functions, and Cloud Dataflow as microservers together to perform monthly auto-downloading, processing, and saving tasks.

The Cloud Scheduler will initiate a message sending to Cloud Pubsub's "monthly-reminder" topic. When the message comes, it'll trigger a Cloud Function, which will download the latest monthly data from the Transtats, unzip it, change the name and save it as CSV file to the Cloud Storage. When the new CSV file successfully saved in Cloud Storage. It'll trigger another Cloud Function, which will submit a job to Cloud Dataflow. Then Cloud Dataflow will digest the new data, do some transformation and finally, sink it into Bigquery, the data warehouse.
  
Let me illustrate some of these components.
  
##### Cloud Dataflow -- Data Preparision

Dataflow is a "managed service for executing a wide variety of data processing patterns." It can handle extensive data, like 1 TB or more, as well as hundreds of MBs size data.

Apache Beam is its "master," or say, Dataflow is one kind of Apache Beam's runner. Apache Beam abstracts the data processing pipeline, which unifies programming models to define and execute data processing pipelines, including batch, and stream processing. As a result, I don't need to care about the hardware, the language, the platform, or even whether it's batch data or streaming data. I only need to implement *HOW* to process data, and focus on the data flow only.

Besides, Dataflow can automatic adjust the machine number to optimize the balance between cost and running time. The cost is base on the number of CUP and the time you used. Thus, running 1000 CPUs 1 hour has the same price of running 1 CUP 1000 hours. Why do I wait for 1000 hours if I can solve a problem in 1 hour? Still, the price considers the time to provision machines, so 1000 CPUs will need more preparation time, and thus create litter higher cost.
  
For the transformation here, it achieves two duties: removing the records that are without an arrival time, and converting all local time into UTC +0, a timestamp normalization. The processing situation is like below.
  
![Processing situation](https://drive.google.com/uc?id=1qF9De0UdQBTd8BKPLOa70FFvotclpVN0)
  
##### Bigquery + Datastudio -- Data Exploration

Bigquery is the data warehouse, and its cost is equivalent to the cost of Google Storage. As a result, the cost will not increase even if I store the data in this place where I can interact with the data.
  
What do I mean "interact with the data"? In Bigquery, I can query questions I am interested in, like whether some carriers are more likely to have a late arrival or whether seasons can influence the flights' delay. Besides, the data can be loaded into Data Studio, a visualization tool based on Bigquery.
  
Here, I show the result of my question below. The type of carrier, the season, the distance, and the locations have relations with the delay, which fit my intuition. (I know you will be more excited for something anti-intuition).
  
![Data Studio answers my questions](https://drive.google.com/uc?id=11tMFzjF0TTVhDA0qfnSDf-24AKS22BLJ)
  
Moreover, it's not a static visualization tool. When I say it's based on Bigquery, I mean you can "query" data by touching these icons. For example, you can select a specific date range. All the visualization will change automatically as a result.
  
![Data Studio Interaction](https://drive.google.com/uc?id=1To5-zfdNiP2rRNkKMB4o5euL2__TZ_Um)
  
From the exploration in Data Studio, I notice that the carriers, locations, department delay, distance, and DateTime more or less have to influence on the delay of the flight. Therefore, I selected related features, including 'arr_lat', 'dep_lat', 'dep_lng', 'arr_lng', 'DEP_DELAY', 'DISTANCE', 'hour', 'month', and 'MKT_UNIQUE_CARRIER' to train a sophisticated model. But before that, let's come back to Bigquery.
  
Bigquery is not just a tool providing essential SQL functions. I can build a logistic model here as a baseline for my future sophisticated deep learning model. 

I used the "rand()" function to sample the training dataset, evaluation dataset, and test dataset. Oh, don't forget as common sense in machine learning -- it's essential to have balanced negative and positive samples in the training data, i.e., having a similar amount of "cancel" data and "non-cancel" data. Then, I can use the ML training function in Bigquery to train the first basic model for this dataset and see its performance.
  
![Bigquery Training](https://drive.google.com/uc?id=1jVqAZVudxb0qPRUmQfVBW_Xszlckw1Cs)
  
![Bigquery Training Result](https://drive.google.com/uc?id=1gceKLB-DnpypXaaN1C2OHZwZu-7cSRwN)
  
Don't be fool by the training result, and the below is the real evaluation performance. Nice, the logistic model, can achieve AUC around 0.798. And it's the baseline I need to break.
  
![Bigquery Evaulation](https://drive.google.com/uc?id=1DbvYuLFTOYeNKPOXqnsfgKbbWav1PJpk)
  
### Model Training
  
In this part, I trained four types of models and deployed the best. Below is the diagram of this part.
  
![Model Training](https://drive.google.com/uc?id=1iG0p1gp6zJ2SSODHrsqXbCyCMYn1ODVz)
  
To train a machine learning model, I think, is just like cooking. If there is a recipe, I should try the existing recipe first. To reinvent pizza is not fun and the new pizza may not as tasty as the "experienced" pizza. In the AI Platform, there are three build-in algorithms, and I should try them first. Understand the basic settings of machine learning job, and I can leverage the "magic" power of Google AI.
  
The three algorithms are XGBoost, wide and deep model, and linear learner model. XGBoost tree is a kind of traditional machine learning model. Like random forest tree, it's based on decision tree ensembles, which combine the results of multiple classifications and regression models. The wide and deep model combines a linear model that learns and "memorizes" a wide range of rules, with a deep neural network that "generalizes" the rules. A linear learner model assigns one weight to each input feature and sums the weights to predict a numerical target value. See more [details]([https://cloud.google.com/ml-engine/docs/algorithms/overview](https://cloud.google.com/ml-engine/docs/algorithms/overview)).

Their final evaluation results are compared as follows. Strangely, this final evaluation in logging for build-in algorithms seems only to use part of the evaluation data. As a result, later, I have to evaluate the best built-in models by using all the evaluation data.

| Type | Final AUC Evaluation |
| :-------------: | :-------------:|
| XGBoost | 0.865 |
| Linear learner model | 0.923 |
| Wide and deep model | 0.961 |

It's clear the wide and deep model performs the best here. Now, it's time to write my code and to see whether I could beat Google's automation.
  
Here, first, I used Tensorflow Transform, which is an extension for data processing. It has API to handle Feature tuning (e.g., scaling and normalizing numeric values), Representation transformation (e.g., bucketization), Feature construction (e.g., feature crossing), and so on. Besides, it can use dataflow to run the processing job. As we mentioned before, dataflow can deal with TB-level jobs as well as MB-level jobs. Thus, it would be time-effective to make data processed and ready for training. Moreover, it will attach transformations to the exported model. As a result, it resolves the annoying preprocessing challenge -- the training-serving skew, a difference between the predictive performance of training and serving. It's largely caused by the discrepancy between how data are handled in the training and the serving pipelines. For more information see [Here](https://cloud.google.com/solutions/machine-learning/data-preprocessing-for-ml-with-tf-transform-pt1).

Here, for numeric data, like "DEP_DELAY" and "DISTANCE", I normalized them to "0-1". For string feature, like "MKT_UNIQUE_CARRIER", I first hashed them to value and make them one-hot-encoding. For value like "latitude", "longitude", "hour" and "month," I put them into buckets so that these features' categorial information can be represented. Last, I made a new feature by crossing "latitude" and "longitude" and bucketizating them. I could leverage my knowledge to beat Google's automation.
  
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

![Customed Transformation](https://drive.google.com/uc?id=1MArlRIV-DXNStxGXhqQjgeDYHdbS1Reu)
  
For the hyper-parameters, including the number of layers and the first hidden layer's size, I can write a .yaml file, like below. Then AI Platform will use its algorithm to search the best combination of these values. Wait for less than one hour, and I can see my result. Luckily, mine is better than Google's automatic data processing and model creation.
  
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

![HyperTune Results](https://drive.google.com/uc?id=1mZRMCEs2SvPo5XbD4RLGta0_EYaRyILY)

![Best Model](https://drive.google.com/uc?id=1F8jtM-aLSBQ0ZUlPPZuXDiDfr5cY4HtO)
  
Here, we can do model examinations so that we can know the pro and cons of the model and then can "targeted" refined the model. For example, use the What-if tool ([https://pair-code.github.io/what-if-tool/](https://pair-code.github.io/what-if-tool/)). However, from my experiences, this tool is not friendly with the AI Platform. Also, it's not directly related to GCP. So, I skip this part.
  
Then, it's time to deploy the model.

### Model Deployment
  
In this part, I used the Google Sheet as the platform and AI-platform as the backend to deploy my model. Below is the diagram of this part. And the idea of using Google Sheet in this way is from "[How to Grow a Spreadsheet into an Application, Google Next 2019](https://www.youtube.com/watch?v=vnm6ViI06MM)"
  
![Model Deployment](https://drive.google.com/uc?id=1muMmpnFohK8wQAV7xl9Afb6s1axrp8rc)
  
As mentioned in one of my previous posts, here come at least three solutions.

First, simply click the "Deploy Model". Then GCP will deploy it on ML engines and handle all the other processes. Second, you can deploy the model by yourself, and using docker to make the model service containerized is the option. It's super fast to write a Dockerfile based on [TensorFlow Serving](https://github.com/tensorflow/serving) and use [Google Build](https://cloud.google.com/cloud-build/) to wrap the model into a container and deploy it to [Kubernetes](https://cloud.google.com/kubernetes-engine/). Last, to achieve maximum DIY, you can choose to use a VM and do what you want. But, in this case, you have to set the networking forwarding rules, safety rules, etc..

For simplicity, I selected the first option. Here, it's worth to notice that to invoke a model deployed on Google ML engines, a role for "ML Engine Developer" is [needed](https://cloud.google.com/ml-engine/docs/access-control). A helpful practice is to create a server account with this role and grant this server account to a microserver on Google Functions.

I chose Google Sheet as the platform to create a demo APP. Google Sheet has the database, UI, security, and loads of built-in APIs. To match this platform, I used Google Forms as the APP UI.

Data are collected from Google Forms. After the user submits a form, it will trigger an "App Script" I wrote in Google Sheet, it will convert the information from the user into a single payload and call the microserver on Google Functions. The microserver will transform the data in the payload to the format my model accepts. Why it's needed? Because by doing so, I can hide the secret of my model. Also, it's a separation of concerns. Later, the Google Sheet will receiver the prediction result from my model and send an e-mail based on the outcome. If the model predicts the user will not be too late for the meeting, the e-mail will encourage the user to keep the meeting. Otherwise, it will suggest the user cancel the meeting.

```js
function sendMail(emailAddress, belate) {
  if (belate) {
    var subject = 'We Suggest You Cancel Your Meeting';
    var message = 'You are very likely not able to have your meeting in time. DO IT in next time. Good Luck!';
  }
  else{
    var subject = 'We Suggest You Keep Your Meeting';
    var message = 'You are very likely to have your meeting in time. Enjoy this Meeting! Good Luck!';
  }
  MailApp.sendEmail(emailAddress, subject, message)
}
```

![Sample Msg](https://drive.google.com/uc?id=1AE1WysazzBxzJ8oncareF10d5yi9T90K)
  
## Wrap Up
  
Let's wrap up with the whole structure.
  
![Whole Structure](https://drive.google.com/uc?id=1-7B5dnR9moZw8g4w9OKjHkYVLngnmp15)
  
Looks great. You can try it!

Good luck, and have fun!