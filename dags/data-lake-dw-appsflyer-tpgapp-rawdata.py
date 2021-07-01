from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
import csv
import requests
import os
import boto3

conn = BaseHook.get_connection("appsflyer")
BASE_URI = conn.host

# https://hq.appsflyer.com/export/id924710586/installs_report/v5
api_key = Variable.get("APPSFLYER_API_TOKEN_V1")
S3_BUCKET = 'rv-core-cards-datamart-qa'
S3_KEY = 'data-lake/temp/test_1'
# S3_BUCKET = 'cards-de-airflow-logs-qa-us-west-2'
# S3_KEY = 'temp/test4'


def make_request(**kwargs):

    params = {
        'api_token': Variable.get("APPSFLYER_API_TOKEN_V1"),
        'from': (datetime.now() - (timedelta(days=int(int(Variable.get("APPSFLYER_LONG_LOOKBACK_DAYS")))))).strftime("%Y-%m-%d"),
        'to': datetime.now().strftime("%Y-%m-%d")
    }

    response = requests.get(BASE_URI, params=params)
    export_string = response.text
    out_file = "/home/airflow/appsflyer.csv"
    print(export_string)

    if os.path.exists(out_file):
        os.remove(out_file)

    f = open(out_file, "w")
    f.write(export_string)
    f.close()

    bucketName = 'cards-de-airflow-logs-qa-us-west-2'
    s3 = boto3.client('s3')

    with open(out_file, "rb") as f:
        response = s3.upload_fileobj(f, bucketName, '%s/%s' % ('temp', 'test4'))
    print(response)

    if os.path.exists(out_file):
        os.remove(out_file)


default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'start_date': datetime(2021, 6, 2, 00, 00, 00),
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

    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        redshift_conn_id='appsflyer_redshift_connection',
        # aws_conn_id='appsflyer_aws_s3_connection_id',
        schema="PUBLIC",
        table="appsflyer_install_test",
        copy_options=['csv', "iam_role 'arn:aws:iam::594144519693:role/dbops-redshift-spectrum'", "region 'us-east-1'"],
        task_id='transfer_s3_to_redshift',
    )

    run_this = BashOperator(
        task_id='run_after_loop',
        bash_command='nc -zv dbops-redshift-cluster-dev.redventures.rv-datascience.privatelinks.redventures.com 5439',
    )

    also_run_this = BashOperator(
        task_id='also_run_after_loop',
        bash_command='nc -zv dbops-redshift-cluster.cd92olv6lp21.us-east-1.redshift.amazonaws.com 5439',
    )

    also_run_this_2 = BashOperator(
        task_id='also_run_after_loop_2',
        bash_command='nc -zv dbops-redshift-cluster-dev.cd92olv6lp21.us-east-1.redshift.amazonaws.com 5439',
    )
