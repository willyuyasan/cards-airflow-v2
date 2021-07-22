from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.models import Variable
from rvairflow import slack_hook as sh

mysql_rw_conn = 'mysql_rw_conn'
# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 19),
    'email': ['mdey@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-db-summarized_issuer_performance',
         schedule_interval='40 8 * * *',
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=2),
         default_args=default_args) as dag:

    select_load = MySqlOperator(
        task_id='select_load_issuer_summary',
        mysql_conn_id=mysql_rw_conn,
        sql='sql/cron/cccom-db-summarized_issuer_performance/insert.sql',
        dag=dag)

    delete_summary = MySqlOperator(
        task_id='delete_archived_issuer_summary_performance',
        mysql_conn_id=mysql_rw_conn,
        sql='sql/cron/cccom-db-summarized_issuer_performance/delete.sql',
        dag=dag)

delete_summary >> select_load
