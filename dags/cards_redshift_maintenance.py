from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 18),
    'email': ['rshukla@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

with DAG('cards-redshift-maintenance',
         schedule_interval='0 12 * * 0',
         dagrun_timeout=timedelta(hours=3),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    full_vacuum_table_task = BashOperator(
        task_id='full_vacuum_table',
        bash_command='python ${AIRFLOW__CORE__DAGS_FOLDER}/scripts/python/redshift_maintenance_script.py {{ params.redshift_connection_name }} {{ params.db_schema }} ',
        params={"redshift_connection_name": 'cards-redshift-cluster',
                "db_schema": Variable.get("cards_redshift_maintenance_db_schemas")},
        dag=dag)
