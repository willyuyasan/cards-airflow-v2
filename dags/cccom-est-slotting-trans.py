from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from operators.extract_operator import mysql_table_to_s3, s3_to_redshift
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
from rvairflow import slack_hook as sh

PREFIX = Variable.get('CCCOM_MYSQL_TO_S3_PREFIX')
redshift_conn = 'cards-redshift-cluster'
aws_conn = 'appsflyer_aws_s3_connection_id'
S3_BUCKET = Variable.get('DBX_CARDS_Bucket')
# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 19),
    'email': ['mdey@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('cccom-dw-est-slotting-trans',
         schedule_interval='45 5,9,13,17,21 * * *',
         catchup=False,
         dagrun_timeout=timedelta(hours=1),
         default_args=default_args) as dag:

    extract_commission_rates_log = PythonOperator(
        task_id='extract-cccom-rev_slotting_transactions',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_rev_slotting_transactions.sql',
                   'key': PREFIX + 'rev_slotting_transactions.csv',
                   'compress': True},
        provide_context=True
    )

    load_commission_rates_log = PythonOperator(
        task_id=f'load-cccom-rev_slotting_transactions',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_rev_slotting_trans',
                   'key': 'rev_slotting_transactions.csv',
                   'compress': True},
        provide_context=True
    )

    merge_commission_rates_log = PostgresOperator(
        task_id='merge-cccom-est_slotting_trans',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_est_slotting_trans.sql'
    )

extract_commission_rates_log >> load_commission_rates_log >> merge_commission_rates_log
