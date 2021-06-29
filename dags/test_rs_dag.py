from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta


seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['rshukla@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('test_rs_dag',
          schedule_interval=None,
          default_args=default_args)
t1 = BashOperator(
    task_id='testairflow',
    bash_command='pwd',
    dag=dag)
t2 = BashOperator(
    task_id='t2',
    bash_command='printenv',
    dag=dag)
