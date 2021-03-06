from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.latest_only_operator import LatestOnlyOperator
from operators.extract_operator import mysql_table_to_s3, pgsql_table_to_s3, s3_to_redshift
from airflow.operators.postgres_operator import PostgresOperator
from rvairflow import slack_hook as sh
from airflow.models import Variable

redshift_conn = 'cards-redshift-cluster'
aws_conn = 'appsflyer_aws_s3_connection_id'
S3_BUCKET = Variable.get('DBX_CARDS_Bucket')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 0o3, 28),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-dw-dummy-product-dag',
         default_args=default_args,
         dagrun_timeout=timedelta(hours=2),
         max_active_runs=1,
         schedule_interval="0 8 * * *",
         catchup=False) as dag:

    merge_dummy = PostgresOperator(
        task_id='merge-dummy',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/mege_dummy_products.sql',
        dag=dag
    )
    merge_dummy
