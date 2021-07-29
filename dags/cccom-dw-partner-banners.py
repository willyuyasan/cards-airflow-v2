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
    'start_date': datetime(2021, 5, 19),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-dw-partner-banners',
         schedule_interval='45 6,10,14,18,22 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    extract_partner_banners = PythonOperator(
        task_id='extract-cccom-partner_banners',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_banners.sql', 'key': 'banners.csv', 'compress': True},
        provide_context=True,
        dag=dag)

    load_partner_banners = PythonOperator(
        task_id='load-cccom-partner_banners',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_banners', 'key': 'banners.csv', 'compress': True},
        provide_context=True,
        dag=dag)

    merge_partner_banners = PostgresOperator(
        task_id='merge-cccom-partner_banners',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_banners.sql',
        dag=dag)

extract_partner_banners >> load_partner_banners >> merge_partner_banners
