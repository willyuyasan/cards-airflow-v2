from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from operators.extract_operator import mysql_table_to_s3, make_request

PREFIX = 'example_dags/extract_examples/'

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG('cccom-db-card_apr_history',
      default_args=default_args,
      max_active_runs=1,
      schedule_interval='40 8 * * *') as dag:

    latest_only_task = LatestOnlyOperator(
        task_id='latest_only_tasks',
        dag=dag)

    query_task = PythonOperator(
        task_id='cccom-db-card_query',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_card_apr_history.sql', 'key': PREFIX + 'card_apr_history.csv'},
        provide_context=True,
        dag=dag)

latest_only_task >> query_task
