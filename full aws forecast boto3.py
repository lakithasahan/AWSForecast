import logging

import sys
import os
import json
import time
# import util
# import pandas as pd
import boto3
from botocore.exceptions import ClientError

# import plotly.graph_objects as go
#
#
# def upload_file(session, file_name, bucket, object_name=None):
#     # If S3 object_name was not specified, use file_name
#     if object_name is None:
#         object_name = file_name
#
#     # Upload the file
#     s3_client = session.client('s3')
#     try:
#         response = s3_client.upload_file(file_name, bucket, object_name)
#         print(response)
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True
#
#
#
#
session = boto3.Session(
    aws_access_key_id='AKIAREYKQBITSK5RMTKT',
    aws_secret_access_key='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',
    region_name='ap-southeast-1')

forecast = session.client(service_name='forecast')
forecastquery = session.client(service_name='forecastquery')

# # Getting data from the csv or excel file
#
# df=pd.read_csv('filtered_file.csv',header=None)
# print(df)
#
# bucket='dmp-forecast'
# file_name='filtered_file.csv'
# upload_file(session,file_name, bucket,object_name=None)
#

# creating datagroups

DATASET_FREQUENCY = "D"
TIMESTAMP_FORMAT = "yyyy-MM-dd"

project = 'util_power_forecastdemo'
datasetName = project + '_ds'
datasetGroupName = project + '_dsg'
s3DataPath = "s3://dmp-awsforecast/electricitydata.csv"

create_dataset_group_response = forecast.create_dataset_group(DatasetGroupName=datasetGroupName,
                                                              Domain="CUSTOM",
                                                              )
datasetGroupArn = create_dataset_group_response['DatasetGroupArn']

# Specify the schema of your dataset here

schema = {
    "Attributes": [
        {
            "AttributeName": "timestamp",
            "AttributeType": "timestamp"
        },
        {
            "AttributeName": "item_id",
            "AttributeType": "string"
        },
        {
            "AttributeName": "target_value",
            "AttributeType": "float"
        },

    ]
}

# cretate Dataset

response = forecast.create_dataset(
    Domain="CUSTOM",
    DatasetType='TARGET_TIME_SERIES',
    DatasetName=datasetName,
    DataFrequency=DATASET_FREQUENCY,
    Schema=schema
)

datasetArn = response['DatasetArn']
forecast.describe_dataset(DatasetArn=datasetArn)

forecast.update_dataset_group(DatasetGroupArn=datasetGroupArn, DatasetArns=[datasetArn])

iam = session.client("iam")

role_name = "ForecastRoleDemotest"
assume_role_policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "forecast.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

try:
    create_role_response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
    )
    role_arn = create_role_response["Role"]["Arn"]
except iam.exceptions.EntityAlreadyExistsException:
    print("The role " + role_name + " exists, ignore to create it")
    role_arn = session.resource('iam').Role(role_name).arn

# Attaching AmazonForecastFullAccess to access all actions for Amazon Forecast
policy_arn = "arn:aws:iam::aws:policy/AmazonForecastFullAccess"
iam.attach_role_policy(
    RoleName=role_name,
    PolicyArn=policy_arn
)

# Now add S3 support
iam.attach_role_policy(
    PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess',
    RoleName=role_name
)
time.sleep(60)  # wait for a minute to allow IAM role policy attachment to propagate

print(role_arn)

datasetImportJobName = 'EP_DSIMPORT_JOB_TARGET'
ds_import_job_response = forecast.create_dataset_import_job(DatasetImportJobName=datasetImportJobName,
                                                            DatasetArn=datasetArn,
                                                            DataSource={
                                                                "S3Config": {
                                                                    "Path": s3DataPath,
                                                                    "RoleArn": role_arn
                                                                }
                                                            },
                                                            TimestampFormat=TIMESTAMP_FORMAT
                                                            )

ds_import_job_arn = ds_import_job_response['DatasetImportJobArn']
print(ds_import_job_arn)

# status_indicator = util.StatusIndicator()

while True:
    status = forecast.describe_dataset_import_job(DatasetImportJobArn=ds_import_job_arn)['Status']
    # status_indicator.update(status)
    print('creating dataset')
    if status in ('ACTIVE', 'CREATE_FAILED'): break
    time.sleep(10)

# status_indicator.end()

print('done creating dataset')

forecast.describe_dataset_import_job(DatasetImportJobArn=ds_import_job_arn)

print(ll)

# creating the predictor
############################################################################################################################

predictorName = project + '_deeparp_algo'

forecastHorizon = 24
algorithmArn = 'arn:aws:forecast:::algorithm/Deep_AR_Plus'

create_predictor_response = forecast.create_predictor(PredictorName=predictorName,
                                                      AlgorithmArn=algorithmArn,
                                                      ForecastHorizon=forecastHorizon,
                                                      PerformAutoML=False,
                                                      PerformHPO=False,
                                                      EvaluationParameters={"NumberOfBacktestWindows": 1,
                                                                            "BackTestWindowOffset": 24},
                                                      InputDataConfig={"DatasetGroupArn": datasetGroupArn},
                                                      FeaturizationConfig={"ForecastFrequency": "H",
                                                                           "Featurizations":
                                                                               [
                                                                                   {"AttributeName": "target_value",
                                                                                    "FeaturizationPipeline":
                                                                                        [
                                                                                            {
                                                                                                "FeaturizationMethodName": "filling",
                                                                                                "FeaturizationMethodParameters":
                                                                                                    {
                                                                                                        "frontfill": "none",
                                                                                                        "middlefill": "zero",
                                                                                                        "backfill": "zero"}
                                                                                                }
                                                                                        ]
                                                                                    }
                                                                               ]
                                                                           }
                                                      )

predictor_arn = create_predictor_response['PredictorArn']

# status_indicator = util.StatusIndicator()

while True:
    status = forecast.describe_predictor(PredictorArn=predictor_arn)['Status']
    # status_indicator.update(status)
    print('predictor training')
    if status in ('ACTIVE', 'CREATE_FAILED'): break
    time.sleep(10)
print('predictor trained done')
# status_indicator.end()

forecast.get_accuracy_metrics(PredictorArn=predictor_arn)

forecastName = project + '_deeparp_algo_forecast'

create_forecast_response = forecast.create_forecast(ForecastName=forecastName,
                                                    PredictorArn=predictor_arn)
forecast_arn = create_forecast_response['ForecastArn']

# status_indicator = util.StatusIndicator()

while True:
    status = forecast.describe_forecast(ForecastArn=forecast_arn)['Status']
    # status_indicator.update(status)
    print('forecast training')
    if status in ('ACTIVE', 'CREATE_FAILED'): break
    time.sleep(10)
print('forecast done training')
# status_indicator.end()


# plotting
########################################################################################################

response = forecastquery.query_forecast(
    ForecastArn="arn:aws:forecast:us-east-1:078942046759:forecast/my_forecast",
    StartDate='2015-01-01T01:00:00.',
    EndDate='2015-01-05T00:00:00.',
    Filters={"item_id": "client_5"},

)

# print(response)


prediction_df_p10 = pd.DataFrame.from_dict(response['Forecast']['Predictions']['p10'])
print(prediction_df_p10)

prediction_df_p10['Timestamp'] = pd.to_datetime(prediction_df_p10['Timestamp'])
# print(prediction_df_p10)


original_df = pd.read_csv('filtered_file.csv')
print(original_df)

Y1 = original_df.iloc[:, 1].tolist()
X1 = original_df.iloc[:, 0].tolist()

X2 = prediction_df_p10['Timestamp'].tolist()
Y2 = prediction_df_p10['Value'].tolist()
trace1 = go.Line(
    x=X1,
    y=Y1,

)
trace2 = go.Line(
    x=X2,
    y=Y2
)
data = [trace1, trace2]
layout = go.Layout(
    autosize=True,

    title="Total Quantity Sold Plot",
    xaxis_title="Date",
    yaxis_title="Quantity",

    # width=900,
    # height=500,

    xaxis=dict(
        autorange=True

    ),
    yaxis=dict(
        autorange=True
    )
)
fig = go.Figure(data=data, layout=layout)

fig.show()
