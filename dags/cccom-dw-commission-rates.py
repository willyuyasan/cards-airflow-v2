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
    'start_date': datetime.now() - timedelta(hours=4),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG('cccom-dw-commission-rates',
          schedule_interval='45 0,8,12,16,20 * * *',
          dagrun_timeout=timedelta(hours=1),
          default_args=default_args)

latest_only_task = LatestOnlyOperator(
    task_id='latest_only',
    dag=dag)

extract_commission_rates_log = PythonOperator(
    task_id='extract-cccom-partner_commission_rates_log',
    python_callable=mysql_table_to_s3,
    op_kwargs={'extract_script': 'cccom/extract_partner_commission_rates_log.sql', 'key': 'partner_commission_rates_log.csv'},
    provide_context=True,
    dag=dag)

load_commission_rates_log = PythonOperator(
    task_id='load-cccom-partner_commission_rates_log',
    python_callable=s3_to_redshift,
    op_kwargs={'table': 'cccom_dw.stg_partner_commission_rates_log', 'key': 'partner_commission_rates_log.csv', 'compress': True},
    provide_context=True,
    dag=dag)

merge_commission_rates_log = PostgresOperator(
    task_id='merge-partner-commission-rates-log',
    postgres_conn_id=redshift_conn,
    sql='/sql/merge/cccom/merge_commission_rates.sql',
    dag=dag
)

extract_commission_rates_log.set_upstream(latest_only_task)
load_commission_rates_log.set_upstream(extract_commission_rates_log)
merge_commission_rates_log.set_upstream(load_commission_rates_log)
