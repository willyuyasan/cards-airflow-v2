from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime, timedelta, date
#import slack_helpers_v2 as sh
from rvairflow import slack_hook as sh
import data_pipeline_helpers_v2 as dh

cfg_dict = dh.read_config_file('aws-redshift-maintenance-gather-table-stats.cfg')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(hours=1),
    'email': ['vikas.malhotra@creditcards.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(),
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'op_kwargs': cfg_dict,
    'provide_context': True
}

dag = DAG('dw-maintenance-gather-stats',
          default_args=default_args,
          dagrun_timeout=timedelta(hours=1),
          schedule_interval='15 11 * * 0')

latest_only = LatestOnlyOperator(
    task_id='latest_only',
    dag=dag)

cccom_gather_table_stats_task = PythonOperator(
    task_id='cccom_gather_table_stats_task',
    python_callable=dh.execute_pipeline,
    dag=dag)

cccom_gather_table_stats_task.set_upstream(latest_only)
