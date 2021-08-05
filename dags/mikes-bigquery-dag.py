from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# from operators.extract_operator import pgsql_s3_test
from airflow.hooks.postgres_hook import PostgresHook
import gzip

query = 'select a.* from transactions.transactions a, '
query += ', '.join([f'transactions.transactions a{i}' for i in range(11)])
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


def pgsql_s3_test(**kwargs):
    print('Retrieving query from .sql file')
    if kwargs.get('extract_script'):
        with open(f'/usr/local/airflow/dags/sql/extract/{kwargs["extract_script"]}', 'r') as f:
            query = f.read()
    elif kwargs.get('query'):
        query = kwargs.get('query')
    else:
        print('Query file not found')
        return
    key = kwargs.get('key')
    if '/' in key:
        S3_KEY = key + '.gz'
    else:
        name = key.split('.')[0]
        ts = datetime.now()
        prefix = f'cccom-dwh/stage/cccom/{name}/{ts.year}/{ts.month}/{ts.day}/'
        S3_KEY = prefix + (key + '.gz' if key else 'no_name.csv.gz')
    pgsql = PostgresHook(postgres_conn_id='postgres_ro_conn')
    print('Dumping PGSQL query results to local file')
    with gzip.open('table-data.gz', 'wb+') as gzip_file:
        pgsql.bulk_dump(f'({query})', 'table-data.gz')
    print('data dumped')


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('mikes-bigquery-dag',
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=2),
         default_args=default_args) as dag:

    extract_affiliates = PythonOperator(
        task_id='bigquery_task',
        python_callable=pgsql_s3_test,
        op_kwargs={'query': query, 'key': 'bigquery_test/bigquery.csv', 'compress': True},
        provide_context=True
    )
