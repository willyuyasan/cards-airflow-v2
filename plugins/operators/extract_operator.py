from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
import requests
import csv
import boto3
import os

conn = BaseHook.get_connection('appsflyer')
BASE_URI = conn.host
S3_BUCKET = Variable.get('EXTRACT-EXAMPLE-BUCKET')
s3 = boto3.client('s3')


def make_request(**kwargs):
    params = {
        'api_token': Variable.get('APPSFLYER_API_TOKEN_V1'),
        'from': (datetime.now() - (timedelta(days=int(int(Variable.get('APPSFLYER_LONG_LOOKBACK_DAYS')))))).strftime('%Y-%m-%d'),
        'to': datetime.now().strftime('%Y-%m-%d')
    }
    response = requests.get(BASE_URI, params=params)
    export_string = response.text
    outfile = '/home/airflow/appsflyer.csv'
    with open(outfile, 'w') as f:
        f.write(export_string)
    outfile_to_S3(outfile, kwargs)


def mysql_table_to_s3(**kwargs):
    print('Retrieving query from .sql file')
    if kwargs.get('query_file'):
        with open(f'/usr/local/airflow/dags/sql/extract/{kwargs["query_file"]}', 'r') as f:
            query = f.read()
    else:
        print('Query file not found')
        return
    mysql = MySqlHook(mysql_conn_id='mysql_ro_conn')
    print('Dumping MySQL query results to local file')
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    outfile = '/home/airflow/mysql.csv'
    with open(outfile, 'w', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
    outfile_to_S3(outfile, kwargs)


def outfile_to_S3(outfile, kwargs):
    print('Loading file into S3')
    S3_KEY = kwargs.get('key') if kwargs.get('key') else 'example_dags/extract_examples/no_name.csv'
    with open(outfile, 'rb') as f:
        response = s3.upload_fileobj(f, S3_BUCKET, S3_KEY)
    print(response)
    if os.path.exists(outfile):
        os.remove(outfile)
