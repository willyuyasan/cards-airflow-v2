from datetime import datetime, timedelta
import time
import requests
import boto3
import os
import logging
import gzip
import csv

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from contextlib import closing
from tempfile import NamedTemporaryFile

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
    ts = str(time.time()).replace('.', '_')
    outfile = f'/home/airflow/appsflyer_{ts}.csv'
    with open(outfile, 'w') as f:
        f.write(export_string)
    outfile_to_S3(outfile, kwargs)


def mysql_table_to_s3(**kwargs):
    print('Retrieving query from .sql file')
    if kwargs.get('extract_script'):
        with open(f'/usr/local/airflow/dags/sql/extract/{kwargs["extract_script"]}', 'r') as f:
            query = f.read()
    else:
        print('Query file not found')
        return
    mysql = MySqlHook(mysql_conn_id='mysql_ro_conn')
    print('Dumping MySQL query results to local file')
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    ts = str(time.time()).replace('.', '_')
    outfile = f'/home/airflow/mysql_{ts}.csv'
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


class PostgresExtractOperator(BaseOperator):
    """
    Executes a SQL command on postgres tables and loads the data in S3
    :param sql: sql command or the file with sql
    :param s3_bucket: name of the s3 bucket into which the import is to be made
    :param s3_file_name: the output file name on S3 WITHOUT the file format extension (.csv/.tsv)
    :param s3_key: the path in S3 bucket into which the output file is to be placed
    :param s3_conn_id: s3 connection id
    :param postgres_conn_id: postgres connection id
    :param iter_size: number of rows the cursor is supposed to fetch from server on each iteration
    :param delimiter: field delimiter either a "tab" or a "comma"
    :param file_format: format of the output file either a "csv" or a "tsv"
    :param header: list of column names if intended to be in the output file

    """
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 sql,
                 s3_bucket=S3_BUCKET,
                 s3_file_name='no_name',
                 s3_key='example_dags/extract_examples/',
                 s3_conn_id='s3_default',
                 postgres_conn_id='postgres_default',
                 iter_size=10000,
                 delimiter="\t",
                 file_format="tsv",
                 header='',
                 *args, **kwargs):
        super(PostgresExtractOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_file_name = s3_file_name
        self.s3_conn_id = s3_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.iter_size = iter_size
        self.delimiter = delimiter
        self.file_format = file_format
        self.header = header

    def file_to_S3(self, outfile, s3_file_path):
        print('Loading file into S3')
        with open(outfile, 'rb') as f:
            response = s3.upload_fileobj(f, S3_BUCKET, s3_file_path)
        print(response)
        if os.path.exists(outfile):
            os.remove(outfile)

    def execute(self, context):
        execution_date = context['execution_date']
        ts = execution_date.strftime('%Y/%m/%d')
        s3_file_name = self.s3_file_name + "_{0}_{1}_{2}".format(execution_date.year,
                                                                 execution_date.month, execution_date.day)
        s3_file_path = self.s3_key + ts + "/" + s3_file_name + "." + self.file_format + ".gz"
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        logging.info("Executing the sql")

        # ToDo: explore the cur.copy_to module along with iter_size
        with closing(hook.get_conn()) as conn:
            # creating a named cursor: supposedly, a server side cursor
            # server side cursor transfers to the client only a controlled amount of data
            with closing(conn.cursor('postgres_extract_cursor')) as cur:
                cur.itersize = self.iter_size
                cur.execute(self.sql)

                with NamedTemporaryFile('w') as temp_file:
                    with gzip.GzipFile(temp_file.name, 'w') as zf:
                        tsvwriter = csv.writer(zf, delimiter=self.delimiter, dialect='excel-tab')
                        if self.header:
                            tsvwriter.writerow(self.header)
                        for row in cur:
                            try:
                                tsvwriter.writerow(row)
                            except Exception as e:
                                print("Exception while writing in the file: " + str(e))
                                print(row)
                    self.file_to_S3(temp_file.name, s3_file_path)
                    # s3.load_file(temp_file.name, s3_file_path, replace=True, bucket_name=self.s3_bucket)
