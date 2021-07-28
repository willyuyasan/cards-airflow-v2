from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from operators.extract_operator import mysql_table_to_s3
import gzip
import csv
import io
query = 'select * from cccomus.partner_affiliates'
query += ' UNION ALL select * from cccomus.partner_affiliates' * 19
redshift_conn = 'cards-redshift-cluster'
# Default settings applied to all tasks
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

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('mikes_test_dag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=300),
         ) as dag:

    extract_heavy_load = PythonOperator(
        task_id='extract-heavy-load',
        python_callable=mysql_table_to_s3,
        op_kwargs={'query': query, 'key': 'heavy_load_test.csv', 'compress': True},
        provide_context=True
    )
