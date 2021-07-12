from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
from helper-modules.ExtractOperators import mysql_table_to_s3, make_request
import sys

# sys.path.insert(0, '/usr/local/airflow/dags/helper-modules')
# from ExtractOperators import
S3_BUCKET = Variable.get('EXTRACT-EXAMPLE-BUCKET')

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('extract_example_dag',
         start_date=datetime(2021, 1, 1),
         max_active_runs=1,
         catchup=False,
         schedule_interval=timedelta(hours=12),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    extract_appsflyer_data = PythonOperator(
        task_id="extract_appsflyer_data",
        python_callable=make_request,
        op_kwargs={'key': 'example_dags/extract_examples/appsflyer_data.csv'}
    )

    extract_mysql_data = PythonOperator(
        task_id=f'extract_mysql_data',
        python_callable=mysql_table_to_s3,  # make sure you don't include the () of the function
        op_kwargs={'query': 'partner_affiliates.sql', 'key': 'example_dags/extract_examples/partner_affiliates.csv'},
        provide_context=True
    )
