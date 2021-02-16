import boto3

# Step 1
# Creating client to access forecast
forecast_client = boto3.client('forecast', region_name='eu-west-1')

dataset_group_name = "covid19_week5"
forecast_domain = "CUSTOM"

# Step 2
# Creating dataset group
dataset_group_response = forecast_client.create_dataset_group(
    DatasetGroupName=dataset_group_name,
    Domain=forecast_domain
)

# Step 3
# Creating Target time series dataset
dataset_response = forecast_client.create_dataset(
    DatasetName='covid19_week5_confirmedcases_train',
    Domain='CUSTOM',
    DatasetType='TARGET_TIME_SERIES',
    DataFrequency='D',
    Schema={"Attributes": [{"AttributeName": "id", "AttributeType": "string"},
                {"AttributeName": "County", "AttributeType": "string"},
                {"AttributeName": "Province_State", "AttributeType": "string"},
                {"AttributeName": "item_id", "AttributeType": "string"},
                {"AttributeName": "Population", "AttributeType": "string"},
                {"AttributeName": "Weight", "AttributeType": "string"},
                {"AttributeName": "timestamp", "AttributeType": "timestamp"},
                {"AttributeName": "ConfirmedCases", "AttributeType": "string"},
                {"AttributeName": "target_value", "AttributeType": "float"}]
        }
)

# Step 4
# Creating dataset import job
import_job_response = forecast_client.create_dataset_import_job(
    DatasetImportJobName='week5_confirmed_cases_import',
    DatasetArn=dataset_response['DatasetArn'],
    DataSource={
        'S3Config': {
            'Path': 's3://exploring-ml-tools-eu-west-1/amazon-forecast/assets/covid_19_week_5/target_time_series/confirmedcases/',
            'RoleArn': 'arn:aws:iam::<account_id>:role/service-role/AmazonForecast-ExecutionRole-1589874597925'
        }
    },
    TimestampFormat='yyyy-MM-dd'
)

# Step 5
# Update dataset group with dataset
update_ds_group_response = forecast_client.update_dataset_group(
    DatasetGroupArn=dataset_group_response['DatasetGroupArn'],
    DatasetArns=[
        dataset_response['DatasetArn']
    ]
)

# Step 6
# Create predictor using automl
predictor_response = forecast_client.create_predictor(
    PredictorName='covid19_confirmed_case_automl',
    ForecastHorizon=14,
    PerformAutoML=True,
    PerformHPO=False,
    InputDataConfig={
        'DatasetGroupArn': dataset_group_response['DatasetGroupArn']
    },
    FeaturizationConfig={
        'ForecastFrequency': 'D'
    }
)

# Check predictor progress
forecast_client.list_predictors(
    Filters=[
        {
            'Key': 'DatasetGroupArn',
            'Value': dataset_group_response['DatasetGroupArn'],
            'Condition': 'IS'
        }
    ]
)

# Step 7
# Create forecast
forecast_response = forecast_client.create_forecast(
    ForecastName='covid19_automl_forecast',
    PredictorArn=predictor_response['PredictorArn'],
    ForecastTypes=[
        "0.1", "0.5", "0.9", "0.95", "0.99"
    ]
)

# Check the forecast progress
forecast_client.list_forecasts(
    Filters=[
        {
            'Key': 'PredictorArn',
            'Value': predictor_response['PredictorArn'],
            'Condition': 'IS'
        },
    ]
)

# Step 8
# Create forecast export job
export_forecast_response = forecast_client.create_forecast_export_job(
    ForecastExportJobName='Week2Foecast',
    ForecastArn=forecast_response['ForecastArn'],
    Destination={
        'S3Config': {
            'Path': 's3://exploring-ml-tools-eu-west-1/amazon-forecast/assets/covid_19_week_5/week6_predicted',
            'RoleArn': 'arn:aws:iam::<account_id>:role/service-role/AmazonForecast-ExecutionRole-1589874597925'
        }
    }
)
