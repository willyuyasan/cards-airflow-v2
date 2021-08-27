import os
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 15, 8),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

mysql_connection = BaseHook.get_connection('mysql_rw_conn')
pgsql_dim = BaseHook.get_connection('cccomprod_postgres_rw_conn')
pgsql_stg = BaseHook.get_connection('cccomstg_postgres_rw_conn')

venv = {**os.environ}
venv["DUMP_FILEPATH"] = str(Variable.get('cccom_dump_file_path'))

venv["MYSQL_DB_USER"] = str(mysql_connection.login)
venv["MYSQL_DBHOST"] = str(mysql_connection.host)
venv["MYSQL_DB_PASS"] = str(mysql_connection.password)

venv["PGSQL_DIM_USER"] = str(pgsql_dim.login)
venv["PGSQL_DIM_HOST"] = str(pgsql_dim.host)
venv["DIMPASSWORD"] = str(pgsql_dim.password)
venv["PGSQL_DIM_DB"] = str(Variable.get('cccom_pg_db_name'))

venv["PGSQL_STG_USER"] = str(pgsql_stg.login)
venv["PGSQL_STG_DBHOST"] = str(pgsql_stg.host)
venv["STGPASSWORD"] = str(pgsql_stg.password)
venv["PGSQL_STG_DB"] = str(Variable.get('cccom_pg_stg_db_name'))


with DAG('cccom-arp-pg-refresh-dim-tables',
         schedule_interval='@weekly',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    arp_refresh_dim_tables = BashOperator(
        task_id='ar_refresh_dim_tables',
        bash_command='/scripts/shell/cccom-arp-pg-refresh-dim-tables.sh',
        execution_timeout=timedelta(minutes=60),
        env=venv,
        dag=dag
    )
    # only in the prod , it connects to real staging env , otherwise both
    # connections in QA and DEV env has been set to same env
    arp_refresh_dim_tables_stag = BashOperator(
        task_id='ar_refresh_dim_tables_stag',
        bash_command='/scripts/shell/cccom-arp-pg-refresh-dim-tables-stag.sh',
        env=venv,
        dag=dag
    )
arp_refresh_dim_tables >> arp_refresh_dim_tables_stag
