from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from operators.extract_operator import mysql_table_to_s3, pgsql_table_to_s3, s3_to_redshift

query = 'select * from cccomus.partner_affiliates'
query += ' UNION ALL select * from cccomus.partner_affiliates' * 99


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
with DAG('mikes-bigquery-dag',
         schedule_interval=timedelta(minutes=300),
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=2),
         default_args=default_args) as dag:

    extract_affiliates = PythonOperator(
        task_id='bigquery_task',
        python_callable=mysql_table_to_s3,
        op_kwargs={'query': query, 'key': 'bigquery_test/bigquery.csv', 'compress': True},
        provide_context=True
    )