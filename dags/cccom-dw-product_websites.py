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

with DAG('cccom-dw-product_websites',
         schedule_interval='45 0,8,12,16,20 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    extract_partner_card_website_map_task = PythonOperator(
        task_id='extract-cccom-partner-card-website-map',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_card_website_map.sql', 'key': 'partner_card_website_map.csv', 'compress': True},
        provide_context=True,
        dag=dag)

    load_partner_card_website_map_task = PythonOperator(
        task_id='load-cccom-partner-card-website-map',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_partner_card_website_map', 'key': 'partner_card_website_map.csv', 'compress': True},
        provide_context=True,
        dag=dag)

    sql_partner_card_website_map_task = PostgresOperator(
        task_id='merge-partner-card-website-map',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_product_websites.sql',
        dag=dag
    )

extract_partner_card_website_map_task >> load_partner_card_website_map_task >> sql_partner_card_website_map_task
