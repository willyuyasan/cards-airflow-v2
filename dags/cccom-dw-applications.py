from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.latest_only_operator import LatestOnlyOperator
from operators.extract_operator import mysql_table_to_s3, pgsql_table_to_s3, s3_to_redshift
from airflow.operators.postgres_operator import PostgresOperator
from rvairflow import slack_hook as sh
from airflow.models import Variable

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
        op_kwargs={'extract_script': 'cccom/extract_applications.sql', 'key': 'applications.csv'},
        provide_context=True,
        dag=dag)

    load_task = PythonOperator(
        task_id='load-cccom-applications',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_applications', 'key': 'applications.csv', 'compress': True},
        provide_context=True,
        dag=dag)

    extract_declined_applications_task = PythonOperator(
        task_id='extract-cccom-declined-applications',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_declined_applications.sql', 'key': 'declined_applications.csv'},
        provide_context=True,
        dag=dag)

    load_declined_applications_task = PythonOperator(
        task_id='load-cccom-declined-applications',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_declined_applications', 'key': 'declined_applications.csv', 'compress': True},
        provide_context=True,
        dag=dag)

    extract_applications_rms_cutover_logic = PythonOperator(
        task_id='extract-cccom-applications_rms-cutover-logic',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_applications_rms.sql', 'key': 'applications_rms.tsv'},
        provide_context=True
    )

    load_applications_rms_cutover_logic = PythonOperator(
        task_id='load-cccom-applications_rms-cutover-logic',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_applications_rms', 'key': 'applications_rms.tsv', 'compress': True},
        provide_context=True,
        dag=dag)

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
