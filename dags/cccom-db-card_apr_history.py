from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
from rvairflow import slack_hook as sh


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(hours=1),
    'depends_on_past': False,
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(hours=1)
}

with DAG('cccom-db-card_apr_history',
         schedule_interval='40 8 * * *',
         max_active_runs=1,
         default_args=default_args) as dag:

    latest_only_task = LatestOnlyOperator(
        task_id='latest_only_tasks',
        dag=dag)

    query_task = MySqlOperator(
        task_id='cccom-db-card_query',
        mysql_conn_id='mysql_ro_conn',
        sql='/sql/cron/cccom-db-card_apr_history.sql',
        dag=dag)

latest_only_task >> query_task
