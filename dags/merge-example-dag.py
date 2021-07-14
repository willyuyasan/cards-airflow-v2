import datetime

from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults


redshift_conn = "cards-redshift-cluster"


default_args = {  # 'op_kwargs': cfg_dict,
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 19),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")

}
with DAG(
    dag_id="postgres_example_dag",
    start_date=datetime(2020, 2, 2),
    schedule_interval='0 * * * *',
    default_args=default_args,
    catchup=False
) as dag:
    merge_device_types_poc = PostgresOperator(
        task_id="merge_device_types_poc",
        postgres_conn_id=redshift_conn,
        sql='/sql/merge/cccom/poc_dim_device_type.sql'
    )
merge_device_types_poc
