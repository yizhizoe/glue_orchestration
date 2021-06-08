from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time
import json
import boto3, botocore
import os
from urllib.parse import urlparse

DEFAULT_MAX_CAPACITY = 5

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_glue_operator_args(job_name, max_capacity=DEFAULT_MAX_CAPACITY):
    glue_client = AwsBaseHook(aws_conn_id='aws_default').get_client_type(client_type='glue')
    response = glue_client.start_job_run(JobName=job_name, MaxCapacity=max_capacity)
    job_id = response['JobRunId']
    print("Job {} ID with capacity {}: {}".format(job_name, max_capacity, job_id))
    while True:
        status = glue_client.get_job_run(JobName=job_name, RunId=job_id)
        state = status['JobRun']['JobRunState']
        if state == 'SUCCEEDED':
            print('Glue job {} run ID {} succeeded'.format(job_name, job_id))
            break
        if state in ['STOPPED', 'FAILED', 'TIMEOUT', 'STOPPING']:
            print('Glue job {} run ID {} is in {} state'.format(job_name, job_id, state))
            raise Exception
        time.sleep(10)

# Parse workflow file s3 path
workflow_file_path = Variable.get("workflow_file_path")
parse_result = urlparse(workflow_file_path)
bucket_name, key = parse_result.netloc, parse_result.path.lstrip('/')
_, file_name = os.path.split(parse_result.path)

# Download workflow file to local
try:
    s3_client = boto3.resource('s3')
    s3_client.Bucket(bucket_name).download_file(key, file_name)
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise

with open(file_name) as f:
    workflow_dict = json.load(f)
    glue_tasks = dict()

    max_capacity = int(Variable.get("max_capacity"))

    for workflow_item in workflow_dict:
        dag = DAG(
            workflow_item["flow"],
            default_args=default_args,
            description='Glue job DAG generated from workflow json',
            schedule_interval=timedelta(days=1),
        )

        # operator over the existing Glue jobs
        for node in workflow_item['nodes']:
            job_name = node['id']
            glue_tasks[job_name] = PythonOperator(
                task_id=job_name,
                python_callable=get_glue_operator_args,
                op_kwargs={'job_name': job_name, 'max_capacity': max_capacity},
                dag=dag
            )

        # DAG structure is created by setting the upstream for all jobs
        for node in workflow_item['nodes']:
            job_name = node['id']
            if 'in' in node:
                firing_job_list = node['in']
                for firing_job in firing_job_list:
                    glue_tasks[job_name].set_upstream(glue_tasks[firing_job])
