from airflow import DAG
from datetime import datetime, timedelta
from operators.extract_operator import PostgresExtractOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from rvairflow import slack_hook as sh
from airflow.models import Variable

PREFIX = 'example_dags/extract_examples/'
redshift_conn = 'cards-redshift-cluster'
aws_conn = 'appsflyer_aws_s3_connection_id'
S3_BUCKET = Variable.get('EXTRACT-EXAMPLE-BUCKET')
# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 19),
    'email': ['mdey@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-dw-clicks_sales_and_applications_rms',
         schedule_interval='0 * * * *',
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         default_args=default_args) as dag:

    extract_applications_rms_task = PostgresExtractOperator(
                            task_id='extract-cccom-applications_rms',
                            sql='sql/extract/cccom/extract_applications_rms_test.sql',
                            s3_file_name='applications_rms_test',
                            postgres_cnn_id=redshift_conn,
                            file_format='csv',
                            header=None,
                            execution_timeout=timedelta(minutes=90),
    )

    load_applications_rms_task = S3ToRedshiftOperator(
        task_id='load-cccom-applications_rms',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX+'applications_rms_test.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_applications_rms_test',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-west-2'", "timeformat 'auto'"],
    )

    merge_applications_rms_task = PostgresOperator(
        task_id='merge-cccom-applications_rms',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_applications_rms_test.sql'
    )

    extract_sales_rms_task = PostgresExtractOperator(
                            task_id='extract-cccom-sales_rms',
                            sql='sql/extract/cccom/extract_rms_transactions_test.sql',
                            s3_file_name='rms_transactions_test',
                            postgres_conn_id=redshift_conn,
                            file_format='csv',
                            header=None,
                            execution_timeout=timedelta(minutes=90),
    )

    load_sales_rms_task = S3ToRedshiftOperator(
        task_id='load-cccom-sales_rms',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX+'rms_transactions_test.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_rms_transactions_test',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-west-2'", "timeformat 'auto'"],
    )

    merge_sales_rms_task = PostgresOperator(
        task_id='merge-cccom-sales_rms',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_sales_rms_test.sql'
    )

    merge_clicks_sales_applications_rms = PostgresOperator(
        task_id='merge-clicks-sales-applications_rms',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_clicks_sales_applications_rms_test.sql'
    )

extract_applications_rms_task >> load_applications_rms_task >> merge_applications_rms_task >> merge_clicks_sales_applications_rms
extract_sales_rms_task >> load_sales_rms_task >> merge_sales_rms_task >> merge_clicks_sales_applications_rms
