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
    'start_date': datetime(2021, 8, 3),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG('cccom-dw-transactions_apply_map',
          schedule_interval='45 0,5-23 * * *',
          dagrun_timeout=timedelta(hours=1),
          max_active_runs=1,
          default_args=default_args)

latest_only = LatestOnlyOperator(
    task_id='latest_only',
    dag=dag)

extract = PythonOperator(
    task_id='extract-transactions_apply_map',
    python_callable=mysql_table_to_s3,
    op_kwargs={'extract_script': 'cccom/extract_transactions_apply_map.sql', 'key': 'trans_apply_map.csv', 'compress': True},
    provide_context=True,
    dag=dag)


load = PythonOperator(
    task_id='load-cccom-trans_apply_map',
    python_callable=s3_to_redshift,
    op_kwargs={'table': 'cccom_dw.stg_trans_apply_map', 'key': 'trans_apply_map.csv', 'compress': True},
    provide_context=True,
    dag=dag)

sql_task = PostgresOperator(
    task_id='merge-cccom-trans_apply_map',
    postgres_conn_id=redshift_conn,
    sql='/sql/merge/cccom/merge_trans_apply.sql',
    dag=dag
)

latest_only.set_downstream(extract)
extract.set_downstream(load)
load.set_downstream(sql_task)
