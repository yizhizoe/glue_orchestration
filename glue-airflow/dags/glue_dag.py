from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time
import json
import boto3, botocore

INPUT_WORKFLOW_FILE = 'sample_workflow.json'
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

def get_glue_operator_args(job_name, max_capacity):
    glue_client = AwsBaseHook(aws_conn_id='aws_default', client_type='glue').get_client_type(client_type='glue')
    response = glue_client.start_job_run(JobName=job_name, MaxCapacity=max_capacity)
    job_id = response['JobRunId']
    print("Job {} ID with capacity {}: {}".format(job_name,max_capacity,job_id))
    while True:
        status = glue_client.get_job_run(JobName=job_name, RunId=job_id)
        state = status['JobRun']['JobRunState']
        if state == 'SUCCEEDED':
            print('Glue job {} run ID {} succeeded'.format(job_name,job_id))
            break
        if state in ['STOPPED', 'FAILED', 'TIMEOUT', 'STOPPING']:
            print('Glue job {} run ID {} is in {} state'.format(job_name,job_id, state))
            raise Exception
        time.sleep(10)

bucket_name = Variable.get("bucket_name")
s3_client = boto3.resource('s3')

try:
    s3_client.Bucket(bucket_name).download_file(INPUT_WORKFLOW_FILE, INPUT_WORKFLOW_FILE)
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise

with open(INPUT_WORKFLOW_FILE) as f:
    workflow_dict = json.load(f)
    glue_tasks = dict()

    for workflow_item in workflow_dict:
        dag = DAG(
            workflow_item["flow"],
            default_args=default_args,
            description='Glue job DAG generated from workflow json',
            schedule_interval=timedelta(days=1),
        )

        max_capacity = int(Variable.get("max_capacity"))

        # operator over the existing Glue jobs
        for node in workflow_item['nodes']:
            job_name = node['id']
            glue_tasks[job_name] = PythonOperator(
                task_id=job_name,
                python_callable=get_glue_operator_args,
                op_kwargs={'job_name':job_name, 'max_capacity':max_capacity},
                dag=dag
            )

        # DAG structure is created by setting the upstream for all jobs
        for node in workflow_item['nodes']:
            job_name = node['id']
            if 'in' in node:
                firing_job_list = node['in']
                for firing_job in firing_job_list:
                    glue_tasks[job_name].set_upstream(glue_tasks[firing_job])
