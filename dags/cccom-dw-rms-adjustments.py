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
    'email': ['rshukla@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-dw-rms-adjustments',
         default_args=default_args,
         dagrun_timeout=timedelta(hours=2),
         max_active_runs=1,
         schedule_interval="0 8 * * *",
         catchup=False) as dag:

    extract_rms_adjustments = PythonOperator(
        task_id='extract-rms-adjustments',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_rms_adjustments.sql', 'key': 'rms_adjustments.tsv', 'compress': True},
        provide_context=True,
        dag=dag)

    load_rms_adjustments = PythonOperator(
        task_id='load-rms-adjustments',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_rms_adjustments', 'key': 'rms_adjustments.tsv', 'compress': True},
        provide_context=True,
        dag=dag)

    merge_rms_adjustments = PostgresOperator(
        task_id='merge-rms-adjustments',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_rms_adjustments.sql',
        dag=dag
    )

extract_rms_adjustments >> load_rms_adjustments >> merge_rms_adjustments
