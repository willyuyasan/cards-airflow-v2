from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook

import requests
import gzip as gz
import os
import subprocess

import logging
import boto3
from botocore.exceptions import ClientError

conn = BaseHook.get_connection("appsflyer")
BASE_URI = conn.host

# https://hq.appsflyer.com/export/id924710586/installs_report/v5
api_key = Variable.get("APPSFLYER_API_TOKEN_V1")


def make_request(**kwargs):

    params = {
        'api_token': Variable.get("APPSFLYER_API_TOKEN_V1"),
        'from': (datetime.now() - (timedelta(days=int(int(Variable.get("APPSFLYER_LONG_LOOKBACK_DAYS")))))).strftime("%Y-%m-%d"),
        'to': datetime.now().strftime("%Y-%m-%d")
    }

    response = requests.get(BASE_URI, params=params)
    tsv_response = response.text.replace(',', '\t')

    tsv_response_list = tsv_response.split('\n')[1:]

    export_string = '\n'.join(tsv_response_list)

    out_file = "/home/airflow/temp/" + "appsflyer.tsv.gz"

    print(export_string)

    with gz.open(out_file, 'wt') as tsvfile:
        tsvfile.write(export_string)

    bucketName = 'cards-de-airflow-logs-qa-us-west-2'
    s3 = boto3.client('s3')

    with open(out_file, "rb") as f:
        response = s3.upload_fileobj(f, bucketName, '%s/%s' % ('/temp/', 'test'))
    print(response)


default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'start_date': datetime(2021, 1, 1, 00, 00, 00),
                'email': ["kbhargavaram@redventures.com"],
                'email_on_failure': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=2),
                'provide_context': True,
                'catchup': False}

with DAG('appsflyer-dw-tpg_appsflyer',
         default_args=default_args,
         dagrun_timeout=timedelta(hours=3),
         schedule_interval='03 01 * * *',
         ) as dag:

    extract_appsflyer_data = PythonOperator(
        task_id="extract_appsflyer_data",
        python_callable=make_request)