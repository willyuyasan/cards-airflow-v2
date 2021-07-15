from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

PREFIX = 'example_dags/extract_examples/'
mysql_rw_conn = 'mysql_rw_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 19),
    'email': ['mdey@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-db-trans-ledger-archive',
         schedule_interval='50 2 * * *',
         dagrun_timeout=timedelta(hours=2),
         default_args=default_args) as dag:

    latest_only_task = LatestOnlyOperator(
            task_id='latest_only',
            dag=dag)

    move_to_archive = MySqlOperator(
            task_id='archive_trans_ledger',
            mysql_conn_id=mysql_rw_conn,
            sql='sql/cron/cccom-db-trans_ledger/move_trans_ledger_to_archive.sql',
            dag=dag)

    delete_archived = MySqlOperator(
            task_id='delete_archived_trans_ledger',
            mysql_conn_id=mysql_rw_conn,
            sql='sql/cron/cccom-db-trans_ledger/delete_archived_trans_ledger.sql',
            dag=dag)

latest_only_task >> move_to_archive
move_to_archive >> delete_archived
