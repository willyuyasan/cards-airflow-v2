from airflow import DAG
from datetime import datetime, timedelta
from operators.extract_operator import PostgresExtractOperator
from rvairflow import slack_hook as sh
from airflow.models import Variable

PREFIX = 'example_dags/extract_examples/'
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
                            postgres_conn_id='postgres_user_rms',
                            header=None,
                            execution_timeout=timedelta(minutes=90),
    )

#     load_applications_rms_task = PythonOperator(
#             task_id='load-cccom-applications_rms',
#             python_callable=dh.execute_pipeline,
#             execution_timeout=timedelta(minutes=10),
#     )
#
#     merge_applications_rms_task = PythonOperator(
#             task_id='merge-cccom-applications_rms',
#             python_callable=dh.execute_pipeline,
#             execution_timeout=timedelta(minutes=2),
#     )

    extract_sales_rms_task = PostgresExtractOperator(
                            task_id='extract-cccom-sales_rms',
                            sql='sql/extract/cccom/extract_rms_transactions_test.sql',
                            s3_file_name='rms_transactions_test',
                            postgres_conn_id='postgres_user_rms',
                            header=None,
                            execution_timeout=timedelta(minutes=90),
    )
#
#     load_sales_rms_task = PythonOperator(
#         task_id='load-cccom-sales_rms',
#         python_callable=dh.execute_pipeline,
#         execution_timeout=timedelta(minutes=10),
#     )
#
#     merge_sales_rms_task = PythonOperator(
#         task_id='merge-cccom-sales_rms',
#         python_callable=dh.execute_pipeline,
#         execution_timeout=timedelta(minutes=2)
#     )
#
#     merge_clicks_sales_applications_rms = PythonOperator(
#         task_id = 'merge-clicks-sales-applications_rms',
#         python_callable=dh.execute_pipeline,
#         execution_timeout=timedelta(minutes=4)
#     )
#
# extract_applications_rms_task >> load_applications_rms_task >> merge_applications_rms_task >> merge_clicks_sales_applications_rms
# extract_sales_rms_task >> load_sales_rms_task >> merge_sales_rms_task >> merge_clicks_sales_applications_rms
