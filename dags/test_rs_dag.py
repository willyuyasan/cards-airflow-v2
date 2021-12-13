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

dag = DAG('Environment_check',
          schedule_interval=None,
          default_args=default_args)
t1 = BashOperator(
    task_id='list-dag',
    bash_command='ls -lsrt ${AIRFLOW__CORE__DAGS_FOLDER} ; ls -lsrt ${AIRFLOW_HOME}',
    dag=dag)
t2 = BashOperator(
    task_id='Print-Env-Varialbe',
    bash_command='printenv',
    dag=dag)
df_i = BashOperator(
    task_id='df-i-space-usage',
    bash_command='df -i',
    dag=dag)
df_h = BashOperator(
    task_id='df-h-space-usage',
    bash_command='df -h',
    dag=dag)
find = BashOperator(
    task_id='find-file',
    bash_command='find . -xdev -type f | cut -d "/" -f 2 | sort | uniq -c | sort -nr',
    dag=dag)
