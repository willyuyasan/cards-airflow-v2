from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from base64 import b64encode, b64decode


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(hours=1),
    'email': ['tdicken@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=20)
}

dag = DAG('clear_glados_cache_v2',
          schedule_interval=timedelta(minutes=15),
          default_args=default_args,
          max_active_runs=1)

clear_cache = SimpleHttpOperator(
    task_id='clear_glados_cache',
    execution_timeout=timedelta(minutes=5),
    http_conn_id='http_glados_prod',
    endpoint='/api/v1/products/cache',
    extra_options={'verify': True},
    priority_weight=50,
    dag=dag)
