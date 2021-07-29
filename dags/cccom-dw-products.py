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
    'start_date': datetime(2021, 5, 19),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-dw-products',
         schedule_interval='45 6,10,14,18,22 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    extract_product_types = PythonOperator(
        task_id='extract-cccom-product_types',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_product_types.sql', 'key': 'product_types.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    load_product_types = PythonOperator(
        task_id='load-cccom-product_types',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_product_types', 'key': 'product_types.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    merge_product_types = PostgresOperator(
        task_id='merge-cccom-product_types',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_product_types.sql',
        dag=dag
    )

    extract_products = PythonOperator(
        task_id='extract-cccom-products',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_products.sql', 'key': 'products.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    load_products = PythonOperator(
        task_id='load-cccom-products',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_products', 'key': 'products.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    merge_products = PostgresOperator(
        task_id='merge-cccom-products',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_products.sql',
        dag=dag
    )

    extract_programs = PythonOperator(
        task_id='extract-cccom-programs',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_programs.sql', 'key': 'programs.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    load_programs = PythonOperator(
        task_id='load-cccom-programs',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_programs', 'key': 'programs.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    merge_programs = PostgresOperator(
        task_id='merge-cccom-programs',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_programs.sql',
        dag=dag
    )

    extract_merchants = PythonOperator(
        task_id='extract-cccom-merchant',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_merchants.sql', 'key': 'merchants.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    load_merchants = PythonOperator(
        task_id='load-cccom-merchants',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_merchants', 'key': 'merchants.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    merge_merchants = PostgresOperator(
        task_id='merge-cccom-merchants',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_merchants.sql',
        dag=dag
    )

extract_product_types >> load_product_types >> merge_product_types >> merge_products

extract_products >> load_products >> merge_products

extract_programs >> load_programs >> merge_programs

extract_merchants >> load_merchants >> merge_merchants >> merge_programs
