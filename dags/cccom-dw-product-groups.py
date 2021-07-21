from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.latest_only_operator import LatestOnlyOperator
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
    'start_date': datetime.now() - timedelta(hours=4),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-dw-product-groups',
         schedule_interval='45 6,10,14,18,22 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         default_args=default_args) as dag:

    extract_partner_card_groups = PythonOperator(
        task_id='extract-cccom-partner_card_groups',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_card_groups.sql', 'key': PREFIX + 'partner_card_groups.csv'},
        provide_context=True,
        dag=dag)

    load_partner_card_groups = S3ToRedshiftOperator(
        task_id='load-cccom-partner_card_groups',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX + 'partner_card_groups.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_partner_card_groups',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
        dag=dag
    )

    merge_partner_card_groups = PostgresOperator(
        task_id='merge-cccom-partner_card_groups-map',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_product_groups.sql',
        dag=dag
    )
    extract_partner_card_group_card_map = PythonOperator(
        task_id='xtract-cccom-partner_card_group_card-map',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_card_group_card_map.sql', 'key': PREFIX + 'partner_card_group_card_map.csv'},
        provide_context=True,
        dag=dag)
    load_partner_card_group_card_map = S3ToRedshiftOperator(
        task_id='load-cccom-partner_card_group_card-map',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX + 'partner_card_group_card_map.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_partner_card_group_card_map',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
        dag=dag
    )
    merge_partner_card_group_card_map = PostgresOperator(
        task_id='merge-cccom-partner_card_group_card-map',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_product_product_groups.sql',
        dag=dag
    )


extract_partner_card_groups >> load_partner_card_groups >> merge_partner_card_groups

extract_partner_card_group_card_map >> load_partner_card_group_card_map >> merge_partner_card_group_card_map
