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

with DAG('cccom-dw-partner-payouts',
         schedule_interval='45 0,8,12,16,20 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         default_args=default_args) as dag:

    extract_partner_payouts_task = PythonOperator(
        task_id='extract-cccom-partner-payouts',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_payouts.sql', 'key': 'partner_payouts.csv'},
        provide_context=True,
        dag=dag)
    extract_partner_payouts_rms = PythonOperator(
        task_id='eextract-cccom-partner_payouts_rms',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_payouts_rms.sql', 'key': 'partner_payouts_rms.tsv'},
        provide_context=True
    )
    extract_partner_payout_trans_map_task = PythonOperator(
        task_id='extract-cccom-partner-payout-trans-map',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_payout_transaction_map.sql', 'key': 'partner_payout_transaction_map.csv'},
        provide_context=True,
        dag=dag)
    extract_partner_payout_trans_map_rms = PythonOperator(
        task_id='extract-cccom-partner_payouts_trans_map_rms',
        python_callable=pgsql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_partner_payout_transaction_map_rms.sql', 'key': 'partner_payout_trans_map_rms.tsv'},
        provide_context=True
    )

    load_partner_payouts_task = PythonOperator(
        task_id='load-cccom-partner-payouts',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_partner_payouts', 'key': 'partner_payouts.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    load_partner_payouts_rms = PythonOperator(
        task_id='load-cccom-partner-payouts_rms',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_partner_payouts_rms', 'key': 'partner_payouts_rms.tsv', 'compress': True},
        provide_context=True,
        dag=dag)
    load_partner_payout_trans_map_task = PythonOperator(
        task_id='load-cccom-partner-payout-trans-map',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_partner_payout_transaction_map', 'key': 'partner_payout_transaction_map.csv', 'compress': True},
        provide_context=True,
        dag=dag)
    load_partner_payout_trans_map_rms = PythonOperator(
        task_id='load-cccom-partner-payout-trans-map_rms',
        python_callable=s3_to_redshift,
        op_kwargs={'table': 'cccom_dw.stg_partner_payout_transaction_map_rms', 'key': 'partner_payout_trans_map_rms.csv', 'compress': True},
        provide_context=True,
        dag=dag)

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
