from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.hooks import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from operators.extract_operator import mysql_table_to_s3, s3_to_mysql
from airflow.models import Variable
from rvairflow import slack_hook as sh

mysql_rw_conn = 'mysql_rw_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 17),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('DE_Heavy_Load_Test_Dag',
         schedule_interval=None,
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=3),
         default_args=default_args) as dag:

    test_extract_heavy_data_to_s3 = PythonOperator(
        task_id='test-extract-heay-data-to-s3',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/test_extract_transaction_data_heavy.sql', 'key': 'test_extract_transaction_data_heavy.csv'},
        provide_context=True
    )
    test_load_data_s3_to_mysql = PythonOperator(
        task_id='test-load-data-s3-to-mysql',
        python_callable=s3_to_mysql,
        op_kwargs={'table': 'cccomus.transactions_test_de', 'key': 'test_extract_transaction_data_heavy.csv', 'duplicate_handling': 'REPLACE'},
        provide_context=True,
        dag=dag)
test_extract_heavy_data_to_s3 >> test_load_data_s3_to_mysql
