from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
import csv
import boto3


conn = BaseHook.get_connection("mysql_conn_id")
BASE_URI = conn.host


# api_key = Variable.get("APPSFLYER_API_TOKEN_V1")
S3_BUCKET = 'cards-de-airflow-logs-qa-us-west-2'
S3_KEY = 'temp/test_mysql_conn'


def mysql_table_to_s3(**kwargs):
    print('Retrieving query from .sql file')
    with open(f"/usr/local/airflow/dags/sql/extract/{kwargs['query']}.sql", 'r') as f:
        query = f.read()
    s3 = boto3.client('s3')
    mysql = MySqlHook(mysql_conn_id='mysql_ro_conn')
    print("Dumping MySQL query results to local file")
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    file = 'extract_example_temp.csv'
    with open(file, 'w', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
    print("Loading file into S3")
    with open(file, 'rb') as f:
        response = s3.upload_fileobj(f, kwargs['bucket'], kwargs['key'])
    print(response)


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

    t0 = DummyOperator(
        task_id='start'
    )

    # generate tasks with a loop. task_id must be unique
    # for task in range(5):
    tm = PythonOperator(
        task_id=f'load_mysql_new',
        python_callable=mysql_table_to_s3,  # make sure you don't include the () of the function
        op_kwargs={'bucket': S3_BUCKET, 'key': S3_KEY, 'query': 'partner_affiliates'},
        provide_context=True
    )

    t0 >> tm
