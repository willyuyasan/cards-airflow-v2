from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from operators.mysqlreplsafeupdate_operator import MySqlReplSafeUpdateOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from rvairflow import slack_hook as sh
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 4),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# conn_id = 'core-prod-57-rw-user'
conn_id = 'mysql_rw_conn'
dag = DAG('cccom-db-summarized_applications',
          schedule_interval='00 */03 * * *',
          dagrun_timeout=timedelta(hours=1),
          default_args=default_args,
          max_active_runs=1,
          catchup=False)

insert_update_task = MySqlReplSafeUpdateOperator(
    task_id='insert_replace_summarized_transactions',
    mysql_conn_id=conn_id,
    sql='sql/cron/cccom-db-summarized_applications/select.sql',
    table='cccomus.summarized_applications',
    duplicate_handling='REPLACE',
    dag=dag)

delete_task = MySqlOperator(
    task_id='delete_old_summarized_transactions',
    mysql_conn_id=conn_id,
    sql='sql/cron/cccom-db-summarized_applications/delete.sql',
    dag=dag)

insert_update_task.set_downstream(delete_task)
