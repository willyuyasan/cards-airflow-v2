from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.models import Variable
from operators.mysqlreplsafeupdate_operator import MySqlReplSafeUpdateOperator
from airflow.operators.mysql_operator import MySqlOperator
import sys

conn_id = 'mysql_rw_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 3),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(hours=1)}

with DAG('DE_Heavy_Test_with_old_operator',
         schedule_interval=None,
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=3),
         default_args=default_args) as dag:

    delete_heavy_load = MySqlOperator(
        task_id='delete_heavy_load',
        mysql_conn_id=conn_id,
        sql='sql/extract/cccom/test_delete_heavy_data.sql',
        dag=dag)

    select_heavy_load = MySqlReplSafeUpdateOperator(
        task_id='select_heavy_load',
        mysql_conn_id=conn_id,
        sql='sql/extract/cccom/test_extract_transaction_data_heavy.sql',
        table='cccomus.transactions_test_de',
        duplicate_handling='REPLACE',
        dag=dag)

delete_heavy_load >> select_heavy_load
