from airflow import DAG
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.models import Variable
from operators.extract_operator import mysql_table_to_s3, s3_to_mysql
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 3),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(hours=1)}

conn_id = 'mysql_rw_conn'

with DAG('cccom-db-partner_affiliate_sales_transactions_report_update',
         default_args=default_args,
         max_active_runs=1,
         schedule_interval='40 8 * * *',
         catchup=False) as dag:

    delete_sales_trans = MySqlOperator(
        task_id='delete_sales_trans',
        mysql_conn_id=conn_id,
        sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/delete_partner_affiliate_sales_transactions_report.sql',
        dag=dag)

    extract_cccom = PythonOperator(
        task_id='select_cccom',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'partner_affiliate/cccom_trans.sql', 'key': 'cccom_trans.csv'},
        provide_context=True
    )

    load_cccom = PythonOperator(
        task_id='load_cccom',
        python_callable=s3_to_mysql,
        op_kwargs={'table': 'cccomus.partner_affiliate_sales_transactions_report', 'key': 'cccom_trans.csv', 'duplicate_handling': 'REPLACE'},
        provide_context=True,
        dag=dag)

    extract_noncccom = PythonOperator(
        task_id='select_noncccom',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'partner_affiliate/non_cccom_trans.sql', 'key': 'non_cccom_trans.csv'},
        provide_context=True
    )

    load_noncccom = PythonOperator(
        task_id='load_cccom',
        python_callable=s3_to_mysql,
        op_kwargs={'table': 'cccomus.partner_affiliate_sales_transactions_report', 'key': 'non_cccom_trans.csv', 'duplicate_handling': 'REPLACE'},
        provide_context=True,
        dag=dag)

    delete_bonus = MySqlOperator(
        task_id='delete_bonus',
        mysql_conn_id=conn_id,
        sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/delete_partner_affiliate_bonus_summary.sql',
        dag=dag)

    extract_bonus = PythonOperator(
        task_id='select_bonus',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'partner_affiliate/bonus_summary.sql', 'key': 'bonus_summary.csv'},
        provide_context=True
    )

    load_bonus = PythonOperator(
        task_id='load_cccom',
        python_callable=s3_to_mysql,
        op_kwargs={'table': 'cccomus.partner_affiliate_bonus_summary', 'key': 'bonus_summary.csv', 'duplicate_handling': 'REPLACE'},
        provide_context=True,
        dag=dag)

    truncate_load_adjustments = MySqlOperator(
        task_id='truncate_load_adjustments',
        mysql_conn_id=conn_id,
        sql='sql/cron/cccom-db-partner_affiliate_sales_transactions_report_update/truncate_load_partner_affiliate_sales_transactions_report_adjustments.sql',
        dag=dag)

delete_sales_trans >> extract_cccom >> load_cccom >> extract_noncccom >> load_noncccom >> truncate_load_adjustments
delete_bonus >> extract_bonus >> load_bonus >> truncate_load_adjustments
