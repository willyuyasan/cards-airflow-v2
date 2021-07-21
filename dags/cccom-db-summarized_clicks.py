from airflow import DAG
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.models import Variable
from airflow.operators.mysql_operator import MySqlOperator

# conn_id = 'core-prod-57-rw-user'
conn_id = 'mysql_ro_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 28),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(hours=1)}

dt = "{{ ds }}"

dag = DAG('cccom-db-summarized_clicks',
          default_args=default_args,
          max_active_runs=1,
          schedule_interval='05 * * * *',
          catchup=False)

select_load = MySqlOperator(
    task_id='select_load_summarized_clicks',
    mysql_conn_id=conn_id,
    sql='CALL cccomus.`load_cccomus_summarized_clicks`(' + "'" + dt + "'" + ')',
    database='cccomus',
    dag=dag)
