from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from operators.extract_operator import mysql_table_to_s3, make_request, pgsql_table_to_s3
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator
from rvairflow import slack_hook as sh
from airflow.models import Variable

PREFIX = Variable.get('CCCOM_MYSQL_TO_S3_PREFIX')
pg_PREFIX = Variable.get('CCCOM_PGSQL_TO_S3_PREFIX')
redshift_conn = 'cards-redshift-cluster'
aws_conn = 'appsflyer_aws_s3_connection_id'
S3_BUCKET = Variable.get('DBX_CARDS_Bucket')
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
with DAG('cccom-dw-sales-and-clicks',
         schedule_interval='0 * * * *',
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=2),
         default_args=default_args) as dag:

    extract_affiliates = PythonOperator(
        task_id=f'extract-cccom-affiliates',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_affiliates.sql', 'key': 'affiliates.csv'},
        provide_context=True
    )

    load_affiliates = S3ToRedshiftOperator(
        task_id='load-cccom-affiliates',
        s3_bucket=S3_BUCKET,
        s3_key='affiliates.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_affiliates',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_affiliates = PostgresOperator(
        task_id='merge-cccom-affiliates',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_affiliates.sql'
    )

    # This is a long-running job, so we up its priority so it
    # starts early in the DAG, and other tasks can happen in parallel.
    extract_click_transactions = PythonOperator(
        task_id='extract-cccom-click_transactions',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_click_transactions.sql', 'key': 'click_transactions.csv'},
        provide_context=True,
        priority_weight=5,
        execution_timeout=timedelta(minutes=40)
    )

    load_click_transactions = S3ToRedshiftOperator(
        task_id='load-cccom-click_trans',
        s3_bucket=S3_BUCKET,
        s3_key='click_transactions.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_click_trans',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_click_transactions = PostgresOperator(
        task_id='merge-cccom-click_trans',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_clicks.sql'
    )

    extract_device_types = PythonOperator(
        task_id='extract-cccom-device_types',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_device_types.sql', 'key': 'device_types.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    load_device_types = S3ToRedshiftOperator(
        task_id='load-cccom-device_types',
        s3_bucket=S3_BUCKET,
        s3_key='device_types.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_device_types',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_device_types = PostgresOperator(
        task_id='merge-cccom-device_types',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_device_types.sql'
    )

    extract_pages = PythonOperator(
        task_id='extract-cccom-pages',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_pages.sql', 'key': 'pages.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=2)
    )

    load_pages = S3ToRedshiftOperator(
        task_id='load-cccom-pages',
        s3_bucket=S3_BUCKET,
        s3_key='pages.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_pages',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_pages = PostgresOperator(
        task_id='merge-cccom-pages',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_pages.sql'
    )

    # This is a long-running job, so we up its priority so it
    # starts early in the DAG, and other tasks can happen in parallel.
    extract_sale_transactions = PythonOperator(
        task_id='extract-cccom-sale_transactions',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_sale_trans.sql', 'key': 'sale_trans.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=20),
        priority_weight=5
    )

    load_sale_transactions = S3ToRedshiftOperator(
        task_id='load-cccom-sale_trans',
        s3_bucket=S3_BUCKET,
        s3_key='sale_trans.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_sale_trans',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_sale_transactions = PostgresOperator(
        task_id='merge-cccom-sale_trans',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_sales.sql'
    )

    """Adding new tasks for sale trans from RMS"""
    extract_sale_rms_with_cutover_date = PythonOperator(
        task_id='extract-cccom-sales_rms-with-cutover-date',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_rms_transactions.sql', 'key': pg_'rms_transactions.csv'},
        provide_context=True
    )

    load_sale_rms_with_cutover_date = S3ToRedshiftOperator(
        task_id='load-cccom-sales_rms-with-cutover-date',
        s3_bucket=S3_BUCKET,
        s3_key=pg_'rms_transactions.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_rms_transactions',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_sales_rms_with_cutover_date = PostgresOperator(
        task_id='merge-cccom-sales_rms_with_cutover_date',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_sales_rms.sql'
    )

    """End of task defs for sale trans"""
    extract_transaction_types = PythonOperator(
        task_id='extract-cccom-transaction_types',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_transaction_types.sql', 'key': 'transaction_types.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    load_transaction_types = S3ToRedshiftOperator(
        task_id='load-cccom-trans_types',
        s3_bucket=S3_BUCKET,
        s3_key='transaction_types.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_trans_types',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_transaction_types = PostgresOperator(
        task_id='merge-cccom-trans_types',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_trans_types.sql'
    )

    extract_keywords = PythonOperator(
        task_id='extract-cccom-keywords',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_keywords.sql', 'key': 'keywords.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    load_keywords = S3ToRedshiftOperator(
        task_id='load-cccom-keywords',
        s3_bucket=S3_BUCKET,
        s3_key='keywords.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_keywords',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_keywords = PostgresOperator(
        task_id='merge-cccom-keywords',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_keywords.sql'
    )

    extract_websites = PythonOperator(
        task_id='extract-cccom-websites',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_websites.sql', 'key': 'websites.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    load_websites = S3ToRedshiftOperator(
        task_id='load-cccom-websites',
        s3_bucket=S3_BUCKET,
        s3_key='websites.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_websites',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    merge_websites = PostgresOperator(
        task_id='merge-cccom-websites',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_websites.sql'
    )

extract_sale_transactions >> load_sale_transactions >> merge_sale_transactions

extract_sale_rms_with_cutover_date >> load_sale_rms_with_cutover_date >> merge_sales_rms_with_cutover_date

merge_sale_transactions >> merge_sales_rms_with_cutover_date

extract_transaction_types >> load_transaction_types >> merge_transaction_types

extract_affiliates >> load_affiliates >> merge_affiliates

extract_keywords >> load_keywords >> merge_keywords

extract_websites >> load_websites >> merge_websites

extract_click_transactions >> load_click_transactions >> merge_click_transactions

extract_device_types >> load_device_types >> merge_device_types

extract_pages >> load_pages >> merge_pages

merge_transaction_types >> merge_sale_transactions

merge_affiliates >> merge_websites

merge_affiliates >> merge_sale_transactions

merge_keywords >> merge_click_transactions

merge_keywords >> merge_sale_transactions

merge_websites >> merge_click_transactions

merge_device_types >> merge_click_transactions

merge_pages >> merge_click_transactions
