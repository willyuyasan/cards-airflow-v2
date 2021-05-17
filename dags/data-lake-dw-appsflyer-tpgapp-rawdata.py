from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook

import requests
import gzip as gz
import os

import logging
import boto3
from botocore.exceptions import ClientError

conn = BaseHook.get_connection("appsflyer")
BASE_URI = conn.host

# https://hq.appsflyer.com/export/id924710586/installs_report/v5
api_key = Variable.get("APPSFLYER_API_TOKEN_V1")

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3'
                             # aws_access_key_id="AKIAIPUG5LHAGS2MXRUQ",
                             # aws_secret_access_key="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"
                             )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



def make_request(**kwargs):

    params = {
        'api_token': '4b40bbd5-8641-4843-9afd-5a3c609fb51f',
        'from': '2021-04-01',
        'to': '2021-04-30'
    }

    today = '2021-05-06'

    # params['from'] = kwargs['execution_date'].strftime('%Y-%m-%d')
    # params['to'] = kwargs['execution_date'].strftime('%Y-%m-%d')

    response =  requests.get(BASE_URI, params=params)

    tsv_response = response.text.replace(',', '\t')

    tsv_response_list = tsv_response.split('\n')[1:]

    export_string = '\n'.join(tsv_response_list)

    #out_file = "/home/airflow/tmp/" + "appsflyer.tsv.gz"
    out_file = "/usr/local/airflow/" + "appsflyer.tsv.gz"

    print(export_string)

    with gz.open(out_file, 'wt') as tsvfile:
        tsvfile.write(export_string)

    bucketName=Variable.get("DBX_TPG_Bucket")

    s3 = boto3.client('s3')
                      # aws_access_key_id="AKIAIPUG5LHAGS2MXRUQ",
                      # aws_secret_access_key="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")
    with open(out_file, "rb") as f:
        s3.upload_fileobj(f, bucketName, "None")

    # s3_conn = S3Hook(aws_conn_id="my_s3_conn")
    # s3_key = "model/tpg/test/{0}/extract_appsflyer.tsv".format(today)
    #
    # s3_conn.load_file(out_file, key=s3_key, bucket_name=Variable.get("DBX_TPG_Bucket"), replace=True)

    os.remove(out_file)


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
