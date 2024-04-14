import requests
import datetime
from datetime import datetime as dt
import holidays
import pandas as pd
from pyspark.sql import SparkSession
import time

# Databricks settings
DATABRICKS_TOKEN = 'your_token_here'
DATABRICKS_URL = 'https://your-instance.cloud.databricks.com'
HEADERS = {'Authorization': f'Bearer {DATABRICKS_TOKEN}'}

# Dictionary with the IDs of the Jobs to be executed during weekdays
job_dict = {
    'ETL_LoadTransformData': 101,
    'Preprocessing_FeatureEngineering': 202,
    'ModelTraining_AutoML': 303,
    'ModelValidation_CrossValidation': 404,
    'ModelDeployment_Packaging': 505,
    'ModelRefeeding_LiveDataUpdate': 606,
    'ModelMonitoring_PerformanceChecks': 707,
    'InfrastructureMonitoring_ResourceUsage': 808
}

# Pandas DataFrame to log the job executions
log_df = pd.DataFrame(columns=['Data_Hora', 'Job_Name', 'Job_ID', 'Status', 'Details'])

def is_weekday():
    today = dt.now()
    br_holidays = holidays.Brazil()
    return today.weekday() < 5 and today not in br_holidays

def run_job(job_name, job_id):
    # Starts a job in Databricks using the API
    response = requests.post(
        f'{DATABRICKS_URL}/api/2.0/jobs/run-now',
        headers=HEADERS,
        json={'job_id': job_id}
    )
    if response.status_code == 200:
        run_id = response.json()['run_id']
        return run_id, 'Job started'
    else:
        return None, f'Error starting the job {job_name}: {response.text}'

def check_job_status(run_id):
    while True:
        response = requests.get(
            f'{DATABRICKS_URL}/api/2.0/jobs/runs/get',
            headers=HEADERS,
            params={'run_id': run_id}
        )
        status = response.json()['state']['life_cycle_state']
        if status in ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED']:
            result_state = response.json()['state'].get('result_state', 'Error')
            if result_state == 'SUCCESS':
                return 'Success', 'Job completed successfully'
            else:
                error_details = response.json()['state'].get('state_message', 'Error details not available')
                return 'Error', f'Error in job, details: {error_details}'
        time.sleep(10)  # Wait 10 seconds before checking again

if is_weekday():
    for job_name, job_id in job_dict.items():
        data_hora = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        run_id, message = run_job(job_name, job_id)
        if run_id:
            status, details = check_job_status(run_id)
        else:
            status, details = 'Error', message
        log_df.loc[len(log_df)] = [data_hora, job_name, job_id, status, details]
else:
    data_hora = dt.now().strftime('%Y-%m-%d %H:%M:%S')
    log_df.loc[len(log_df)] = [data_hora, 'N/A', 'N/A', 'Not Executed', 'Weekend or holiday']

# Converting the pandas DataFrame to PySpark DataFrame
spark_df = spark.createDataFrame(log_df)
spark_df.show()

# Save the PySpark DataFrame for later use
spark_df.write.format("parquet").save("path/to/save/job_execution_log.parquet")
