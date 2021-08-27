from airflow import DAG
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 9),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-arp-pg-refresh-mat-views',
         schedule_interval='*/30 * * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    arp_refresh_matviews = PostgresOperator(
        task_id='ar_refresh_material_views',
        postgres_conn_id='cccomprod_postgres_rw_conn',
        sql='/sql/extract/cccom/refresh_ar_matetial_views.sql',
        # params=dict_template,
        execution_timeout=timedelta(minutes=15),
        dag=dag
    )

    # only in the prod , it connects to real staging env , otherwise both
    # connections in QA and DEV env has been set to same env
    arp_refresh_matviews_stg = PostgresOperator(
        task_id='ar_refresh_material_views_stg',
        postgres_conn_id='cccomstg_postgres_rw_conn',
        sql='/sql/extract/cccom/refresh_ar_matetial_views.sql',
        # params=dict_template,
        execution_timeout=timedelta(minutes=15),
        dag=dag
    )

arp_refresh_matviews
arp_refresh_matviews_stg
