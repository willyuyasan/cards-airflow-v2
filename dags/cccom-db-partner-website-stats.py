from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from operators.mysqlreplsafeupdate_operator import MySqlReplSafeUpdateOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import Variable
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 3),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

conn_id = 'mysql_rw_conn'

dag = DAG('cccom-db-partner-website-stats',
          schedule_interval='0 */3 * * *',
          dagrun_timeout=timedelta(hours=1),
          max_active_runs=1,
          default_args=default_args)

latest_only_task = LatestOnlyOperator(
    task_id='latest_only',
    dag=dag)

update_daily_stats = MySqlReplSafeUpdateOperator(
    task_id='update_partner_website_daily_stats',
    mysql_conn_id=conn_id,
    sql='sql/cron/partner_website_stats.sql',
    table='partner_website_daily_stats',
    duplicate_handling='REPLACE',
    dag=dag)

update_conv_rates = MySqlOperator(
    task_id='update_partner_website_conversion_rates',
    mysql_conn_id=conn_id,
    sql='sql/cron/partner_website_conversion_rates.sql',
    dag=dag)

latest_only_task.set_downstream(update_daily_stats)
update_daily_stats.set_downstream(update_conv_rates)
