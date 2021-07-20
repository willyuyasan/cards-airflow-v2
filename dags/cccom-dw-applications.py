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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(hours=1),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}


with DAG('cccom-dw-applications',
         schedule_interval='0 * * * *',
         dagrun_timeout=timedelta(minutes=90),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    extract_task = PythonOperator(
        task_id='extract-cccom-applications',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_applications.sql', 'key': PREFIX + 'applications.csv'},
        provide_context=True,
        dag=dag)

    extract_applications_rms_cutover_logic = PythonOperator(
        task_id='extract-cccom-applications_rms-cutover-logic',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_applications_rms.sql', 'key': pg_PREFIX + 'applications_rms.tsv'},
        provide_context=True
    )

    extract_declined_applications_task = PythonOperator(
        task_id='extract-cccom-declined-applications',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_declined_applications.sql', 'key': PREFIX + 'declined_applications.csv'},
        provide_context=True,
        dag=dag)

    load_task = S3ToRedshiftOperator(
        task_id='load-cccom-applications',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX + 'applications.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='applications',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    load_declined_applications_task = S3ToRedshiftOperator(
        task_id='load-cccom-declined-applications',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX + 'declined_applications.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='declined_applications',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    load_applications_rms_cutover_logic = S3ToRedshiftOperator(
        task_id='load-cccom-applications_rms-cutover-logic',
        s3_bucket=S3_BUCKET,
        s3_key=pg_PREFIX + 'applications_rms.tsv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='merge_applications_rms',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    sql_task = PostgresOperator(
        task_id='merge-cccom-applications',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_applications.sql'
    )

    merge_applications_rms_cutover_logic = PostgresOperator(
        task_id='merge-cccom-applications_rms-cutover-logic',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_applications_rms.sql'
    )

extract_task >> load_task >> sql_task

extract_declined_applications_task >> load_declined_applications_task >> sql_task

extract_applications_rms_cutover_logic >> load_applications_rms_cutover_logic >> merge_applications_rms_cutover_logic

sql_task >> merge_applications_rms_cutover_logic
