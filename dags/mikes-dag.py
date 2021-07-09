from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
import csv

conn = BaseHook.get_connection("mysql_conn_id")
BASE_URI = conn.host


# api_key = Variable.get("APPSFLYER_API_TOKEN_V1")
S3_BUCKET = 'cards-de-airflow-logs-qa-us-west-2'
S3_KEY = 'temp/test_mysql_conn'


def my_custom_function(ts, **kwargs):
    """
    This can be any python code you want and is called from the python operator.
    The code is not executed until the task is run by the airflow scheduler.
    """
    hook = MySqlHook(mysql_conn_id='mysql_ro_conn')
    hook.run("select * from information_schema.tables")
    print('hooked')
    # hook.run(copy_command)
    print(f"I am task number {kwargs['task_number']}. This DAG Run execution date is {ts} and the current time is {datetime.now()}")
    print('Here is the full DAG Run context. It is available because provide_context=True')
    print(kwargs)


def execute(**kwargs):
    s3 = boto3.client('s3')
    mysql = MySqlHook(mysql_conn_id='mysql_ro_conn')
    print("Dumping MySQL query results to local file")
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute('select * from information_schema.tables')
    with open('mike_temp.csv', 'w', newline='') as f:
        csv_writer = csv.writer(f, delimiter=',', encoding="utf-8")
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        print("Loading file into S3")
        with open(out_file, "r") as f:
            response = s3.upload_fileobj(f, S3_BUCKET, 'data-lake/temp/mike_test')
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
with DAG('mikes_dag',
         start_date=datetime(2021, 1, 1),
         max_active_runs=1,
         catchup=False,
         schedule_interval=timedelta(minutes=120),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(
        task_id='start'
    )

    # generate tasks with a loop. task_id must be unique
    # for task in range(5):
    tn = PythonOperator(
        task_id=f'python_print_date_{0}',
        python_callable=my_custom_function,  # make sure you don't include the () of the function
        op_kwargs={'task_number': 0},
        provide_context=True
    )

    # generate tasks with a loop. task_id must be unique
    # for task in range(5):
    tm = PythonOperator(
        task_id=f'load_mysql',
        python_callable=execute,  # make sure you don't include the () of the function
        op_kwargs={'task_number': 0},
        provide_context=True
    )

    t0 >> [tn, tnm] # indented inside for loop so each task is added downstream of t0
