from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
# from mysql_to_s3 import MySQLToS3Operator


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
    hook.run("select 'test'")
    print('hooked')
    # hook.run(copy_command)
    print(f"I am task number {kwargs['task_number']}. This DAG Run execution date is {ts} and the current time is {datetime.now()}")
    print('Here is the full DAG Run context. It is available because provide_context=True')
    print(kwargs)


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


    # task_transfer_mysql_to_s3 = MySQLToS3Operator(
    #     query="select 'test'",
    #     s3_bucket=S3_BUCKET,
    #     s3_key=S3_KEY,
    #     mysql_conn_id='mysql_conn_id',
    #     aws_conn_id='aws_default',
    #     verify=False,
    #     task_id='transfer_mysql_to_s3'
    # )

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

    t0 >> tn  # indented inside for loop so each task is added downstream of t0
