from airflow import DAG
from datetime import datetime, timedelta
from operators.extract_operator import pgsql_table_to_s3, s3_to_redshift
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from rvairflow import slack_hook as sh
from airflow.models import Variable


redshift_conn = 'cards-redshift-cluster'
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

    extract_applications_rms_task = PythonOperator(
        task_id='extract-cccom-applications_rms',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_applications_rms_test.sql',
                   'key': 'applications_rms_test.csv',
                   'compress': True},
        provide_context=True
    )

    load_applications_rms_task = PythonOperator(
        task_id='load-cccom-applications_rms',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_applications_rms_test', 'key': 'applications_rms_test.csv', 'compress': True},
        provide_context=True
    )

    merge_applications_rms_task = PostgresOperator(
        task_id='merge-cccom-applications_rms',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_applications_rms_test.sql'
    )

    extract_sales_rms_task = PythonOperator(
        task_id='extract-cccom-sales_rms',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_rms_transactions_test.sql',
                   'key': 'rms_transactions_test.csv',
                   'compress': True},
        provide_context=True
    )

    load_sales_rms_task = PythonOperator(
        task_id='load-cccom-sales_rms',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_rms_transactions_test', 'key': 'rms_transactions_test.csv', 'compress': True},
        provide_context=True
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
