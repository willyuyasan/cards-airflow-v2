from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.models import Variable
from operators.mysqlreplsafeupdate_operator import MySqlReplSafeUpdateOperator
from airflow.operators.mysql_operator import MySqlOperator

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
# conn_id = 'core-prod-57-rw-user'
conn_id = 'mysql_ro_conn'
dag = DAG('cccom-db-partner_affiliate_sales_transactions_report_update',
          default_args=default_args,
          max_active_runs=1,
          schedule_interval='40 8 * * *',
          catchup=False)

select_load_cccom = MySqlReplSafeUpdateOperator(
    task_id='select_load_cccom',
    mysql_conn_id=conn_id,
    sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/cccom_trans.sql',
    table='cccomus.partner_affiliate_sales_transactions_report',
    duplicate_handling='REPLACE',
    dag=dag)

select_load_noncccom = MySqlReplSafeUpdateOperator(
    task_id='select_load_noncccom',
    mysql_conn_id=conn_id,
    sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/non_cccom_trans.sql',
    table='cccomus.partner_affiliate_sales_transactions_report',
    duplicate_handling='REPLACE',
    dag=dag)

select_load_bonus = MySqlReplSafeUpdateOperator(
    task_id='select_load_bonus',
    mysql_conn_id=conn_id,
    sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/bonus_summary.sql',
    table='cccomus.partner_affiliate_bonus_summary',
    duplicate_handling='REPLACE',
    dag=dag)

delete_sales_trans = MySqlOperator(
    task_id='delete_sales_trans',
    mysql_conn_id=conn_id,
    sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/delete_partner_affiliate_sales_transactions_report.sql',
    dag=dag)

delete_bonus = MySqlOperator(
    task_id='delete_bonus',
    mysql_conn_id=conn_id,
    sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/delete_partner_affiliate_bonus_summary.sql',
    dag=dag)

truncate_load_adjustments = MySqlOperator(
    task_id='truncate_load_adjustments',
    mysql_conn_id=conn_id,
    sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/truncate_load_partner_affiliate_sales_transactions_report_adjustments.sql',
    dag=dag)

select_load_cccom.set_upstream(delete_sales_trans)
select_load_noncccom.set_upstream(select_load_cccom)
select_load_bonus.set_upstream(delete_bonus)
truncate_load_adjustments.set_upstream(select_load_bonus)
truncate_load_adjustments.set_upstream(select_load_noncccom)
