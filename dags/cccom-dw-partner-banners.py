from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.latest_only_operator import LatestOnlyOperator
from operators.extract_operator import mysql_table_to_s3, make_request, pgsql_table_to_s3
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator
from rvairflow import slack_hook as sh
from airflow.models import Variable

PREFIX = Variable.get('CCCOM_MYSQL_TO_S3_PREFIX')
pg_PREFIX = Variable.get('CCCOM_PGSQL_TO_S3_PREFIX')
redshift_conn = 'cards-redshift-cluster'
aws_conn = 'appsflyer_aws_s3_connection_id'
S3_BUCKET = Variable.get('DBX_CARDS_Bucket')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(hours=4),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'op_kwargs': cfg_dict,
    'provide_context': True
}

with DAG('cccom-dw-partner-banners',

         # schedule_interval='45 6,10,14,18,22 * * *',
         schedule_interval='None',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         default_args=default_args) as dag:

    extract_partner_banners = PythonOperator(
        task_id='extract-cccom-partner_banners',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_banners.sql', 'key': PREFIX + 'banners.csv'},
        provide_context=True,
        dag=dag)

    load_partner_banners = S3ToRedshiftOperator(
        task_id='load-cccom-partner_banners',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX + 'banners.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='banners',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
        dag=dag
    )

merge_partner_banners = PostgresOperator(
    task_id='merge-cccom-partner_banners',
    postgres_conn_id=redshift_conn,
    sql='/sql/merge/cccom/merge_banners.sql',
    dag=dag
)


extract_partner_banners >> load_partner_banners >> merge_partner_banners
