from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

mysql_rw_conn = 'mysql_rw_conn'

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 19),
    'email': ['mdey@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-db-trans_click_ext_cleanup',
         schedule_interval='0 2 * * *',
         dagrun_timeout=timedelta(hours=2),
         default_args=default_args) as dag:

    latest_only_task = LatestOnlyOperator(
        task_id='latest_only',
        dag=dag)

    trans_click_cleanup = MySqlOperator(
        task_id='trans_click_ext_cleanup',
        mysql_conn_id=mysql_rw_conn,
        sql='sql/cron/trans_click_ext_cleanup.sql',
        dag=dag)

latest_only_task >> trans_click_cleanup
