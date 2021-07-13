from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
from operators.extract_operator import mysql_table_to_s3, make_request

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


def tbd(**kwargs):
    print('Placeholder')


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('cccom-dw-clicks_sales_applications-no-dependencies',
         schedule_interval='45 * * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         default_args=default_args) as dag:

    merge_csa = PythonOperator(
        task_id='merge-clicks_sales_applications',
        python_callable=tbd,
        op_kwargs={'extract_script': 'cccom/merge_clicks_sales_applications.sql', 'key': PREFIX + 'click_transactions.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=4)
    )

    merge_clicks_sales_applications_with_cutover_date = PythonOperator(
        task_id = 'merge-clicks-sales-applications_rms-with-cutover-date',
        python_callable=tbd,
        op_kwargs={'extract_script': 'cccom/extract_click_transactions.sql', 'key': PREFIX + 'click_transactions.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=4)
    )

merge_csa >> merge_clicks_sales_applications_with_cutover_date
