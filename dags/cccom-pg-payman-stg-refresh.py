import os
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
# from rvairflow import slack_hook as sh
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 26),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

pgsql_payman = BaseHook.get_connection('payman_cccomprod_postgres_conn')

venv = {**os.environ}
venv["DUMP_FILEPATH"] = str(Variable.get('cccom_dump_file_path'))
venv["PGSQL_USER"] = str(pgsql_payman.login)
venv["PGSQL_HOST"] = str(pgsql_payman.host)
venv["PAYMANPASS"] = str(pgsql_payman.password)

with DAG('cccom-pg-payman-stg-refresh',
         schedule_interval='0 5 * * 0',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    pg_refresh_paymen_schema = BashOperator(
        task_id='t_pg_refresh_paymen_schema',
        bash_command='/scripts/shell/cccom-pg-payman-stg-refresh.sh',
        execution_timeout=timedelta(minutes=10),
        params={"refresh": str(Variable.get('refresh_env')),
                "dbhost": str(Variable.get('refresh_host')),
                "db": str(Variable.get('refresh_db')),
                "db_to": str(Variable.get('refresh_db_to')),
                "dbhost_to": str(Variable.get('refresh_host_to'))},
        env=venv,
        dag=dag
    )

pg_refresh_paymen_schema
