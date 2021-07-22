from datetime import datetime, timedelta
import time
import requests
import boto3
import os
import logging
import gzip
import csv
import io
import json


from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.mysql.transfers.s3_to_mysql import S3ToMySqlOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from contextlib import closing
from tempfile import NamedTemporaryFile

conn = BaseHook.get_connection('appsflyer')
BASE_URI = conn.host
S3_BUCKET = Variable.get('DBX_CARDS_Bucket')
s3 = boto3.client('s3')
redshift_conn = 'cards-redshift-cluster'
aws_conn = 'appsflyer_aws_s3_connection_id'
mysql_rw_conn = 'mysql_rw_conn'


def make_request(**kwargs):
    params = {
        'api_token': Variable.get('APPSFLYER_API_TOKEN_V1'),
        'from': (datetime.now() - (timedelta(days=int(int(Variable.get('APPSFLYER_LONG_LOOKBACK_DAYS')))))).strftime('%Y-%m-%d'),
        'to': datetime.now().strftime('%Y-%m-%d')
    }
    response = requests.get(BASE_URI, params=params)
    export_string = response.text
    ts = str(time.time()).replace('.', '_')
    outfile = f'/home/airflow/appsflyer_{ts}.csv'
    with open(outfile, 'w') as f:
        f.write(export_string)
    outfile_to_S3(outfile, kwargs)


def compressed_file(cursor, kwargs):
    mem_file = io.BytesIO()
    with gzip.GzipFile(fileobj=mem_file, mode='w') as gz:
        buff = io.StringIO()
        writer = csv.writer(buff)
        writer.writerows(cursor)
        print('Writing data to gzipped file.')
        gz.write(buff.getvalue().encode())
        print('Data written')
        gz.close()
        mem_file.seek(0)
    print('Sending to S3')
    key = kwargs.get('key')
    if '/' in key:
        S3_KEY = key + '.gz'
    else:
        name = key.split('.')[0]
        ts = datetime.now()
        prefix = f'cccom-dwh/stage/cccom/{name}/{ts.year}/{ts.month}/{ts.day}/'
        S3_KEY = prefix + (key + '.gz' if key else 'no_name.csv.gz')
    s3.upload_fileobj(Fileobj=mem_file, Bucket=S3_BUCKET, Key=S3_KEY)
    print('Sent')


def mysql_table_to_s3(**kwargs):
    print('Retrieving query from .sql file')
    if kwargs.get('extract_script'):
        with open(f'/usr/local/airflow/dags/sql/extract/{kwargs["extract_script"]}', 'r') as f:
            query = f.read()
    elif kwargs.get('query'):
        query = kwargs.get('query')
    else:
        print('Query file not found')
        return
    mysql = MySqlHook(mysql_conn_id='mysql_ro_conn')
    print('Dumping MySQL query results to local file')
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    if kwargs.get('compress'):
        compressed_file(cursor, kwargs)
        cursor.close()
        conn.close()
    else:
        ts = str(time.time()).replace('.', '_')
        outfile = f'/home/airflow/mysql_{ts}.csv'
        with open(outfile, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(cursor)
            f.flush()
            cursor.close()
            conn.close()
        outfile_to_S3(outfile, kwargs)


def pgsql_table_to_s3(**kwargs):
    print('Retrieving query from .sql file')
    if kwargs.get('extract_script'):
        with open(f'/usr/local/airflow/dags/sql/extract/{kwargs["extract_script"]}', 'r') as f:
            query = f.read()
    elif kwargs.get('query'):
        query = kwargs.get('query')
    else:
        print('Query file not found')
        return
    pgsql = PostgresHook(postgres_conn_id='postgres_ro_conn')
    print('Dumping PGSQL query results to local file')
    conn = pgsql.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    if kwargs.get('compress'):
        compressed_file(cursor, kwargs)
        cursor.close()
        conn.close()
    else:
        ts = str(time.time()).replace('.', '_')
        outfile = f'/home/airflow/pgsql_{ts}.csv'
        with open(outfile, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(cursor)
            f.flush()
            cursor.close()
            conn.close()
        outfile_to_S3(outfile, kwargs)


def s3_to_redshift(**kwargs):
    print('Loading file into Redshift')
    sch_tbl = kwargs.get('table')
    if not sch_tbl:
        print('Table not found')
        return
    schema, table = sch_tbl.split('.')
    key = kwargs.get('key')
    if '/' in key:
        S3_KEY = key
    else:
        name = key.split('.')[0]
        ts = datetime.now()
        prefix = f'cccom-dwh/stage/cccom/{name}/{ts.year}/{ts.month}/{ts.day}/'
        S3_KEY = prefix + (key if key else 'no_name.csv')
    copy_options = ['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"]
    if kwargs.get('compress'):
        S3_KEY += '.gz'
        copy_options.append('GZIP')
    rs_op = S3ToRedshiftOperator(
        task_id='redshift-copy-task',
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema=schema,
        table=table,
        copy_options=copy_options
    )
    rs_op.execute('')


def s3_to_mysql(**kwargs):
    print('Loading file into RDS MySQL')
    sch_tbl = kwargs.get('table')
    if not sch_tbl:
        print('Table not found')
        return
    schema, table = sch_tbl.split('.')
    key = kwargs.get('key')
    if '/' in key:
        S3_KEY = key
    else:
        name = key.split('.')[0]
        ts = datetime.now()
        prefix = f'cccom-dwh/stage/cccom/{name}/{ts.year}/{ts.month}/{ts.day}/'
        S3_KEY = prefix + (key if key else 'no_name.csv')
    if kwargs.get('field_format'):
        with open(f'/usr/local/airflow/dags/json/field_format/{kwargs["field_format"]}', 'r') as f:
            field_format = json.loads(f.read())
    mysql_op = S3ToMySqlOperator(
        task_id='mysql-load-task',
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        mysql_conn_id=mysql_rw_conn,
        s3_conn_id=aws_conn,
        database=schema,
        field_schema=field_format,
        table=table
    )
    mysql_op.execute('')


def outfile_to_S3(outfile, kwargs):
    print('Loading file into S3')
    key = kwargs.get('key')
    if '/' in key:
        S3_KEY = key
    else:
        name = key.split('.')[0]
        ts = datetime.now()
        prefix = f'cccom-dwh/stage/cccom/{name}/{ts.year}/{ts.month}/{ts.day}/'
        S3_KEY = prefix + (key if key else 'no_name.csv')
    with open(outfile, 'rb') as f:
        response = s3.upload_fileobj(f, S3_BUCKET, S3_KEY)
    print(response)
    if os.path.exists(outfile):
        os.remove(outfile)
