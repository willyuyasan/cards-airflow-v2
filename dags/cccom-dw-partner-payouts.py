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
    'start_date': datetime.now() - timedelta(hours=1),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG('cccom-dw-partner-payouts',
         schedule_interval='45 0,8,12,16,20 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         default_args=default_args) as dag:

    extract_partner_payouts_task = PythonOperator(
        task_id='extract-cccom-partner-payouts',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_payouts.sql', 'key': PREFIX + 'partner_payouts.csv'},
        provide_context=True,
        dag=dag)

    extract_partner_payouts_rms = PythonOperator(
        task_id='eextract-cccom-partner_payouts_rms',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/eextract_partner_payouts_rms.sql', 'key': pg_PREFIX + 'partner_payouts_rms.tsv'},
        provide_context=True
    )

    extract_partner_payout_trans_map_task = PythonOperator(
        task_id='extract-cccom-partner-payout-trans-map',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/eextract_partner_payout_transaction_map.sql', 'key': PREFIX + 'partner_payout_transaction_map.csv'},
        provide_context=True,
        dag=dag)

    extract_partner_payout_trans_map_rms = PythonOperator(
        task_id='extract-cccom-partner_payouts_trans_map_rms',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_payout_transaction_map_rms.sql', 'key': pg_PREFIX + 'partner_payout_trans_map_rms.tsv'},
        provide_context=True
    )

    load_partner_payouts_task = S3ToRedshiftOperator(
        task_id='load-cccom-partner-payouts',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX + 'partner_payouts.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_partner_payouts',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    load_partner_payouts_rms = S3ToRedshiftOperator(
        task_id='load-cccom-partner-payouts_rms',
        s3_bucket=S3_BUCKET,
        s3_key=pg_PREFIX + 'partner_payouts_rms.tsv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_partner_payouts_rms',
        copy_options=['tsv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    load_partner_payout_trans_map_task = S3ToRedshiftOperator(
        task_id='load-cccom-partner-payout-trans-map',
        s3_bucket=S3_BUCKET,
        s3_key=PREFIX + 'partner_payout_transaction_map.csv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_partner_payout_transaction_map',
        copy_options=['csv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    load_partner_payout_trans_map_rms = S3ToRedshiftOperator(
        task_id='load-cccom-partner-payout-trans-map_rms',
        s3_bucket=S3_BUCKET,
        s3_key=pg_PREFIX + 'partner_payout_trans_map_rms.tsv',
        redshift_conn_id=redshift_conn,
        aws_conn_id=aws_conn,
        schema='cccom_dw',
        table='stg_partner_payout_transaction_map_rms',
        copy_options=['tsv', 'IGNOREHEADER 1', "region 'us-east-1'", "timeformat 'auto'"],
    )

    sql_payouts_task = PostgresOperator(
        task_id='merge-cccom-payouts',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_payouts.sql',
        dag=dag
    )

    sql_payouts_rms = PostgresOperator(
        task_id='merge-cccom-payouts_rms',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_payouts_rms.sql',
        dag=dag
    )

    sql_payout_trans_map_task = PostgresOperator(
        task_id='merge-cccom-payout-trans-map',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_trans_payout.sql',
        dag=dag
    )

    sql_payout_trans_map_task_rms = PostgresOperator(
        task_id='merge-cccom-payout-trans-map_rms',
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/merge_trans_payout_rms.sql',
        dag=dag
    )

extract_partner_payouts_task >> load_partner_payouts_task >> sql_payouts_task

extract_partner_payouts_rms >> load_partner_payouts_rms >> sql_payouts_rms

sql_payouts_task >> sql_payouts_rms

extract_partner_payout_trans_map_task >> load_partner_payout_trans_map_task >> sql_payout_trans_map_task

extract_partner_payout_trans_map_rms >> load_partner_payout_trans_map_rms >> sql_payout_trans_map_task_rms

sql_payout_trans_map_task >> sql_payout_trans_map_task_rms
