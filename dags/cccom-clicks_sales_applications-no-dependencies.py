from airflow import DAG
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults

redshift_conn = 'cards-redshift-cluster'
# Default settings applied to all tasks
default_args = {  # 'op_kwargs': cfg_dict,
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 19),
    'email': ['mdey@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('cccom-dw-clicks_sales_applications-no-dependencies',
         schedule_interval='45 * * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         default_args=default_args) as dag:

    merge_csa = PostgresOperator(
        task_id='merge-clicks_sales_applications',
        postgres_conn_id=redshift_conn,
        sql='/scripts/sql/merge/cccom/merge_clicks_sales_applications.sql'
    )

    merge_clicks_sales_applications_with_cutover_date = PostgresOperator(
        task_id='merge-clicks-sales-applications_rms-with-cutover-date',
        postgres_conn_id=redshift_conn,
        sql='/scripts/sql/merge/cccom/merge_clicks_sales_applications_rms.sql'
    )

merge_csa >> merge_clicks_sales_applications_with_cutover_date
