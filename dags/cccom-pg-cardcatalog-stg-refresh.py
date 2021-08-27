from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 26),
    'email': ['rzagade@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}


with DAG('cccom-pg-cardcatalog-stg-refresh',
         schedule_interval='0 7 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    pg_refresh_cardcatalog_schema = BashOperator(
        task_id='t_pg_refresh_cardcatalog_schema',
        bash_command='/scripts/shell/cccom-pg-cardcatalog-stg-refresh.sh',
        execution_timeout=timedelta(minutes=60),
        params={"env": str(Variable.get('refresh_env')),
                "dbhost": str(Variable.get('refresh_host')),
                "db": str(Variable.get('refresh_db')),
                "db_to": str(Variable.get('refresh_db_to')),
                "dbhost_to": str(Variable.get('refresh_host_to'))},
        dag=dag
    )


pg_refresh_cardcatalog_schema
