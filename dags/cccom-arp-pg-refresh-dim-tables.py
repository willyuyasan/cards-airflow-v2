from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 8),
    'email': ['pkarmakar@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

mysql_connection = BaseHook.get_connection('mysql_rw_conn')
pgsql_connection = BaseHook.get_connection('postgres_ro_conn')

with DAG('cccom-arp-pg-refresh-dim-tables',
         schedule_interval='46 0 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    arp_refresh_dim_tables = BashOperator(
        task_id='ar_refresh_dim_tables',
        bash_command='/scripts/shell/cccom-arp-pg-refresh-dim-tables.sh',
        execution_timeout=timedelta(minutes=30),
        params={
            "DUMP_FILEPATH": str(Variable.get('cccom_dump_file_path')),
            "MYSQL_DB_USER": str(mysql_connection.login),
            "MYSQL_DBHOST": str(mysql_connection.host),
            "MYSQL_DB_PASS": str(mysql_connection.password),
            "PGSQL_DB_USER": str(pgsql_connection.login),
            "PGSQL_DBHOST": str(pgsql_connection.host),
            "PGSQL_DB_PASS": str(pgsql_connection.password),
            "PGSQL_DB": str(Variable.get('cccom_pg_db_name')),

        },
        dag=dag
    )
    # only in the prod , it connects to real staging env , otherwise both
    # connections in QA and DEV env has been set to same env
    arp_refresh_dim_tables_stag = BashOperator(
        task_id='ar_refresh_dim_tables_stag',
        bash_command='/shellscripts/cccom-arp-pg-refresh-dim-tables-stag.sh',
        execution_timeout=timedelta(minutes=30),
        params={
                "DUMP_FILEPATH": str(Variable.get('cccom_dump_file_path')),
            "MYSQL_DB_USER": str(mysql_connection.login),
            "MYSQL_DBHOST": str(mysql_connection.host),
            "MYSQL_DB_PASS": str(mysql_connection.password),
            "PGSQL_DB_USER": str(pgsql_connection.login),
            "PGSQL_DBHOST": str(pgsql_connection.host),
            "PGSQL_DB_PASS": str(pgsql_connection.password),
            "PGSQL_DB": str(Variable.get('cccom_pg_stg_db_name')),

        },
        dag=dag
    )
arp_refresh_dim_tables >> arp_refresh_dim_tables_stag
