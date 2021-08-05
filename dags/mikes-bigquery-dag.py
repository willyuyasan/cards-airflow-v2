from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# from operators.extract_operator import pgsql_s3_test
from airflow.hooks.postgres_hook import PostgresHook
import gzip
import io
import boto3
import csv
from tempfile import NamedTemporaryFile
from airflow.models import Variable


S3_BUCKET = Variable.get('DBX_CARDS_Bucket')
s3 = boto3.client('s3')
redshift_conn = 'cards-redshift-cluster'
aws_conn = 'appsflyer_aws_s3_connection_id'
mysql_rw_conn = 'mysql_rw_conn'
iter_size = 10000

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
    conn = pgsql.get_conn()
    cursor = conn.cursor('my_named_cursor')
    cursor.execute(query)
    print('Dumping PGSQL query results to local file')
    with NamedTemporaryFile('wb+') as temp_file:
        with gzip.GzipFile(fileobj=temp_file, mode='w') as gz:
            print('Writing data to gzipped file.')
            for row in cursor:
                buff = io.StringIO()
                writer = csv.writer(buff)
                writer.writerow(row)
                gz.write(buff.getvalue().encode())
            print('Data written')
            gz.close()
            temp_file.seek(0)
    print('Sending to S3')
    key = kwargs.get('key')
    if '/' in key:
        S3_KEY = key + '.gz'
    else:
        name = key.split('.')[0]
        ts = datetime.now()
        prefix = f'cccom-dwh/stage/cccom/{name}/{ts.year}/{ts.month}/{ts.day}/'
        S3_KEY = prefix + (key + '.gz' if key else 'no_name.csv.gz')
    s3.upload_fileobj(Fileobj=temp_file, Bucket=S3_BUCKET, Key=S3_KEY)
    print('Sent')


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('mikes-bigquery-dag',
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=30),
         schedule_interval=timedelta(hours=200),
         default_args=default_args) as dag:

    extract_affiliates = PythonOperator(
        task_id='bigquery_task',
        python_callable=pgsql_s3_test,
        op_kwargs={'query': query, 'key': 'bigquery_test/bigquery.csv', 'compress': True},
        provide_context=True
    )
