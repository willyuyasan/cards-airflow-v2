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

dag = DAG('cccom-dw-payin_tiers',
          schedule_interval='45 0,8,12,16,20 * * *',
          dagrun_timeout=timedelta(hours=1),
          catchup=False,
          max_active_runs=1,
          default_args=default_args)

extract_payin_tiers = PythonOperator(
    task_id='extract-cccom-payin_tiers',
    python_callable=mysql_table_to_s3,
    op_kwargs={'extract_script': 'cccom/extract_payin_tiers.sql', 'key': 'payin_tiers.csv', 'compress': True},
    provide_context=True,
    dag=dag)
extract_card_assignments = PythonOperator(
    task_id='extract-cccom-payin_tier_card_assignments',
    python_callable=mysql_table_to_s3,
    op_kwargs={'extract_script': 'cccom/extract_payin_tier_card_assignments.sql', 'key': 'payin_tier_card_assignments.csv', 'compress': True},
    provide_context=True,
    dag=dag)
extract_website_assignments = PythonOperator(
    task_id='extract-cccom-payin_tier_website_assignments',
    python_callable=mysql_table_to_s3,
    op_kwargs={'extract_script': 'cccom/extract_payin_tier_website_assignments.sql', 'key': 'payin_tier_website_assignments.csv', 'compress': True},
    provide_context=True,
    dag=dag)

load_payin_tiers = PythonOperator(
    task_id='load-cccom-payin_tiers',
    python_callable=s3_to_redshift,
    op_kwargs={'table': 'cccom_dw.stg_payin_tiers', 'key': 'payin_tiers.csv', 'compress': True},
    provide_context=True,
    dag=dag)
load_card_assignments = PythonOperator(
    task_id='load-cccom-payin_tier_card_assignments',
    python_callable=s3_to_redshift,
    op_kwargs={'table': 'cccom_dw.stg_payin_tier_card_assignments', 'key': 'payin_tier_card_assignments.csv', 'compress': True},
    provide_context=True,
    dag=dag)
load_website_assignments = PythonOperator(
    task_id='load-cccom-payin_tier_website_assignments',
    python_callable=s3_to_redshift,
    op_kwargs={'table': 'cccom_dw.stg_payin_tier_website_assignments', 'key': 'payin_tier_website_assignments.csv', 'compress': True},
    provide_context=True,
    dag=dag)


merge_product_payin_tiers = PostgresOperator(
    task_id='merge-cccom-product_payin_tiers',
    postgres_conn_id=redshift_conn,
    sql='/sql/merge/cccom/merge_product_payin_tiers.sql',
    dag=dag
)
merge_website_payin_tiers = PostgresOperator(
    task_id='merge-cccom-website_payin_tiers',
    postgres_conn_id=redshift_conn,
    sql='/sql/merge/cccom/merge_website_payin_tiers.sql',
    dag=dag
)

extract_payin_tiers >> load_payin_tiers >> merge_product_payin_tiers

load_payin_tiers >> merge_website_payin_tiers

extract_website_assignments >> load_website_assignments >> merge_website_payin_tiers

extract_card_assignments >> load_card_assignments >> merge_product_payin_tiers
