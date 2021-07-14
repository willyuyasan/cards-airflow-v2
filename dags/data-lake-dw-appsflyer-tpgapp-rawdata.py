from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
import requests
import os
import boto3
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 12, 00, 00, 00),
    'email': ["kbhargavaram@redventures.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'catchup': False,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")
}

conn = BaseHook.get_connection("appsflyer")
BASE_URI = conn.host
api_key = Variable.get("APPSFLYER_API_TOKEN_V1")
cur_date = datetime.now().strftime("%Y-%m-%d")
location = "{}TenantId={}/Date={}/".format(Variable.get("APPSFLYER_INSTALLS_LOCATION"), Variable.get("DBX_TPG_APP_Tenant_Id"), cur_date)
S3_BUCKET = Variable.get("DBX_CARDS_Bucket")
S3_KEY = "{}installs_report_{}".format(location, cur_date)


def make_request(**kwargs):

    params = {
        'api_token': Variable.get("APPSFLYER_API_TOKEN_V1"),
        'from': (datetime.now() - (timedelta(days=int(int(Variable.get("APPSFLYER_LONG_LOOKBACK_DAYS")))))).strftime("%Y-%m-%d"),
        'to': datetime.now().strftime("%Y-%m-%d")
    }

    response = requests.get(BASE_URI, params=params)
    export_string = response.text
    out_file = Variable.get("APPSFLYER_OUTFILE")
    print(export_string)

    if os.path.exists(out_file):
        os.remove(out_file)

    f = open(out_file, "w")
    f.write(export_string)
    f.close()

    s3 = boto3.client('s3')
    filename = 'installs_report_' + params['to']

    with open(out_file, "rb") as f:
        response = s3.upload_fileobj(f, S3_BUCKET, '%s%s' % (location, filename))
    print(response)

    if os.path.exists(out_file):
        os.remove(out_file)


with DAG('data-lake-dw-tpg_appsflyer_installs',
         default_args=default_args,
         dagrun_timeout=timedelta(hours=3),
         schedule_interval='0 09 * * *',
         catchup=False,
         max_active_runs=1
         ) as dag:

    extract_appsflyer_data = PythonOperator(
        task_id="extract_appsflyer_data",
        python_callable=make_request)

    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        redshift_conn_id='appsflyer_redshift_connection',
        aws_conn_id='appsflyer_aws_s3_connection_id',
        schema=Variable.get("APPSFLYER_SCHEMA"),
        table=Variable.get("APPSFLYER_TABLE"),
        copy_options=['csv', "IGNOREHEADER 1", "region 'us-east-1'", "timeformat 'auto'"],
        task_id='transfer_s3_to_redshift',
    )

# Dependencies
extract_appsflyer_data >> task_transfer_s3_to_redshift
