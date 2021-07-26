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
    'start_date': datetime(2018, 11, 29, 00, 00),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('rms-build_arp_reports',
         schedule_interval='40 * * * *',
         catchup=False,
         dagrun_timeout=timedelta(hours=1),
         max_active_runs=1,
         default_args=default_args) as dag:

    build_fact_arp_transaction_report = PostgresOperator(
        task_id='build-fact_arp_transactions',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_transaction.sql',
        dag=dag
    )

    build_fact_arp_performance_report_p1 = PostgresOperator(
        task_id='build-fact_arp_performance_p1',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_performance_part1.sql',
        dag=dag
    )

    build_fact_arp_performance_report_p2 = PostgresOperator(
        task_id='build-fact_arp_performance_p2',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_performance_part2.sql',
        dag=dag
    )

    build_fact_arp_performance_report_p3 = PostgresOperator(
        task_id='build-fact_arp_performance_p3',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_performance_part3.sql',
        dag=dag
    )

    build_fact_arp_overview_p1 = PostgresOperator(
        task_id='build-fact_arp_overview_p1',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_overview_p1.sql',
        dag=dag
    )

    build_fact_arp_overview_p2 = PostgresOperator(
        task_id='build-fact_arp_overview_p2',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_overview_p2.sql',
        dag=dag
    )

    build_fact_arp_payment = PostgresOperator(
        task_id='build-fact_arp_payment',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_payment.sql',
        dag=dag
    )

    build_fact_arp_sale_commissions = PostgresOperator(
        task_id='build_fact_arp_sale_commissions',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_sale_commissions.sql',
        dag=dag
    )

    build_fact_arp_adjustments = PostgresOperator(
        task_id='build_fact_arp_adjustments',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/build_fact_arp_adjustments.sql',
        dag=dag
    )

build_fact_arp_transaction_report >> build_fact_arp_performance_report_p1 >> build_fact_arp_performance_report_p2
build_fact_arp_performance_report_p2 >> build_fact_arp_performance_report_p3 >> build_fact_arp_overview_p1
build_fact_arp_overview_p1 >> build_fact_arp_overview_p2 >> build_fact_arp_payment >> build_fact_arp_sale_commissions
build_fact_arp_sale_commissions >> build_fact_arp_adjustments
