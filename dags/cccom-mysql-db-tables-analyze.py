from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 3),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True
}

mysql_connection = BaseHook.get_connection('analyze_tab')
mysql_connection_stg = BaseHook.get_connection('analyze_tab_stag')

with DAG('cccom-mysql-db-tables-analyze',
         schedule_interval='45 23 * * 6',
         dagrun_timeout=timedelta(minutes=15),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    cccom_mysql_db_tables_analyze = BashOperator(
        task_id='cccom_mysql_db_tables_analyze',
        bash_command='/scripts/shell/cccom-mysql-db-tables-analyze.sh',
        execution_timeout=timedelta(minutes=15),
        params={"env": str(Variable.get('refresh_env')),
                "mysql_db_host": str(mysql_connection.host),
                "mysql_db_user": str(mysql_connection.login),
                "mysql_db_host_stag": str(mysql_connection_stg.host)
        },
        dag=dag
    )
cccom_mysql_db_tables_analyze
