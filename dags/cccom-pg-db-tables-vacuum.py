import os
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 26),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

pg_cccom_prod = BaseHook.get_connection('postgres_arp_svc_user')
pg_cccom_stg = BaseHook.get_connection('postgres_arp_svc_user_stg')

venv = {**os.environ}
venv["DUMP_FILEPATH"] = str(Variable.get('cccom_dump_file_path'))
venv["PGSQL_PROD_USER"] = str(pg_cccom_prod.login)
venv["PGSQL_PROD_HOST"] = str(pg_cccom_prod.host)
venv["PRODPASSWORD"] = str(pg_cccom_prod.password)


venv["PGSQL_STG_USER"] = str(pg_cccom_stg.login)
venv["PGSQL_STG_DBHOST"] = str(pg_cccom_stg.host)
venv["STGPASSWORD"] = str(pg_cccom_stg.password)


with DAG('cccom-pg-db-tables-vacuum',
         schedule_interval='45 22 * * 6',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    cccom_pg_db_tables_vacuum_prod = BashOperator(
        task_id='cccom_pg_db_tables_vacuum_p',
        bash_command='/scripts/shell/cccom-pg-db-tables-vacuum.sh',
        execution_timeout=timedelta(minutes=120),
        params={"refresh": str(Variable.get('refresh_env')),
                "file_suffix": str(Variable.get('file_suffix_prod')),
                "pg_db_host": str(pg_cccom_prod.host),
                "pg_db_user": str(pg_cccom_prod.login),
                "pg_db_name": str(pg_cccom_prod.schema)},
        env=venv,
        dag=dag
    )

    # only in the prod , it connects to real staging env , otherwise both
    # connections in QA and DEV env has been set to same env
    cccom_pg_db_tables_vacuum_stag = BashOperator(
        task_id='cccom_pg_db_tables_vacuum_s',
        bash_command='/scripts/shell/cccom-pg-stg-db-tables-vacuum.sh',
        execution_timeout=timedelta(minutes=120),
        params={"refresh": str(Variable.get('refresh_env')),
                "file_suffix": str(Variable.get('file_suffix_stag')),
                "pg_db_host": str(pg_cccom_stg.host),
                "pg_db_user": str(pg_cccom_stg.login),
                "pg_db_name": str(pg_cccom_stg.schema)},
        env=venv,
        dag=dag
    )

cccom_pg_db_tables_vacuum_prod >> cccom_pg_db_tables_vacuum_stag
