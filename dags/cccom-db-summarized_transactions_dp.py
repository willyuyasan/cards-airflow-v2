from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.hooks import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from operators.extract_operator import mysql_table_to_s3, s3_to_mysql
from airflow.models import Variable
from rvairflow import slack_hook as sh

s = date.today() - timedelta(days=90)
start_date = "'" + str(date(s.year, s.month, 1)) + "'"
end_date = "'" + str(datetime.now()) + "'"
mysql_rw_conn = 'mysql_rw_conn'

summarized_clicks_query = f"""
    SELECT max(date_inserted)
    FROM cccomus.transactions_click_external
    WHERE date_inserted < {end_date}"""
summarized_sales_query = f"""
    SELECT max(created_date)
    FROM cccomus.transactions_sale_external
    WHERE created_date < {end_date}"""
summarized_applications_query = f"""
    SELECT max(created_date)
    FROM cccomus.applications
    WHERE created_date < {end_date}"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 19),
    'email': ['mdey@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}


def update_summarized_dates(**kwargs):
    prod_ro = MySqlHook(mysql_conn_id='mysql_ro_conn')
    prod_57 = MySqlHook(mysql_conn_id='mysql_rw_conn')

    dt = prod_ro.get_first(kwargs["summarize_query"])
    if dt[0]:
        date_string = dt[0].isoformat()
    else:
        date_string = '1970-01-01T00:00:00'
    trans_type = kwargs["transaction_type"]

    update_query = f"""
      REPLACE INTO cccomus.summarized_transaction_dates (
      transaction_type, summarized_date)
      VALUES ('{trans_type}_stage', '{date_string}');
    """

    prod_57.run(update_query)
    # This will put date_string into XCom
    return date_string


with DAG('cccom-db-summarized_transactions_dp',
         schedule_interval='15 * * * *',
         catchup=False,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=2),
         default_args=default_args) as dag:

    latest_only_task = LatestOnlyOperator(
        task_id='latest_only_tasks',
        dag=dag)

    summarized_dates_clicks_task = PythonOperator(
        task_id='summarized_dates_clicks_task',
        python_callable=update_summarized_dates,
        op_kwargs={'summarize_query': summarized_clicks_query,
                   'transaction_type': 'clicks'},
        dag=dag,
        execution_timeout=timedelta(minutes=2)
    )

    summarized_clicks_extract_task = PythonOperator(
        task_id='extract-cccom-summarized-clicks',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_summarized_clicks.sql',
                   'key': 'summarized_clicks.csv'},
        provide_context=True
    )

    summarized_clicks_stage_load_task = PythonOperator(
        task_id='load-cccom-summarized-clicks',
        python_callable=s3_to_mysql,
        op_kwargs={'table': 'cccomus.summarized_clicks_stage',
                   'key': 'summarized_clicks.csv',
                   'field_format': 'cccom/field_format_summarized_clicks.json'},
        provide_context=True,
        execution_timeout=timedelta(minutes=2)
    )

    summarized_dates_sales_task = PythonOperator(
        task_id='summarized_dates_sales_task',
        python_callable=update_summarized_dates,
        op_kwargs={'summarize_query': summarized_sales_query,
                   'transaction_type': 'sales'},
        dag=dag,
        execution_timeout=timedelta(minutes=10)
    )

    summarized_sales_extract_task = PythonOperator(
        task_id='extract-cccom-summarized-sales',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_summarized_sales.sql',
                   'key': 'summarized_sales.csv'},
        provide_context=True
    )

    summarized_sales_stage_load_task = PythonOperator(
        task_id='load-cccom-summarized-sales',
        python_callable=s3_to_mysql,
        op_kwargs={'table': 'cccomus.summarized_sales_stage',
                   'key': 'summarized_sales.csv',
                   'field_format': 'cccom/field_format_summarized_sales.json'},
        provide_context=True,
        execution_timeout=timedelta(minutes=2)
    )

    summarized_dates_applications_task = PythonOperator(
        task_id='summarized_dates_applications_task',
        python_callable=update_summarized_dates,
        op_kwargs={'summarize_query': summarized_applications_query,
                   'transaction_type': 'applications'},
        dag=dag,
        execution_timeout=timedelta(minutes=2)
    )

    summarized_applications_extract_task = PythonOperator(
        task_id='extract-cccom-summarized-applications',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_summarized_applications.sql',
                   'key': 'summarized_applications.csv',
                   'compress': True},
        provide_context=True
    )

    summarized_applications_stage_load_task = PythonOperator(
        task_id='load-cccom-summarized-applications',
        python_callable=s3_to_mysql,
        op_kwargs={'table': 'cccomus.summarized_applications_stage',
                   'key': 'summarized_applications.csv',
                   'field_format': 'cccom/field_format_summarized_applications.json'},
        provide_context=True,
        execution_timeout=timedelta(minutes=2)
    )

    summarized_transactions_stage_task = MySqlOperator(
        task_id='summarized_transactions_stage',
        mysql_conn_id=mysql_rw_conn,
        sql='sql/summarized_transactions/summarized_transactions_stage.sql',
        dag=dag,
        params={'START_DATE': start_date,
                'END_DATE': end_date},
        retries=0,
        execution_timeout=timedelta(minutes=15)
    )

    summarized_transactions_task = MySqlOperator(
        task_id='summarized_transactions',
        mysql_conn_id=mysql_rw_conn,
        sql='sql/summarized_transactions/summarized_transactions.sql',
        dag=dag,
        params={'START_DATE': start_date,
                'END_DATE': end_date},
        retries=0,
        execution_timeout=timedelta(minutes=35)
    )

latest_only_task >> summarized_dates_clicks_task
latest_only_task >> summarized_dates_sales_task
latest_only_task >> summarized_dates_applications_task
summarized_dates_clicks_task >> summarized_clicks_extract_task
summarized_dates_sales_task >> summarized_sales_extract_task
summarized_dates_applications_task >> summarized_applications_extract_task

summarized_clicks_stage_load_task.set_upstream(summarized_clicks_extract_task)
summarized_sales_stage_load_task.set_upstream(summarized_sales_extract_task)
summarized_applications_stage_load_task.set_upstream(summarized_applications_extract_task)

summarized_transactions_stage_task.set_upstream(summarized_clicks_stage_load_task)
summarized_transactions_stage_task.set_upstream(summarized_sales_stage_load_task)
summarized_transactions_stage_task.set_upstream(summarized_applications_stage_load_task)
summarized_transactions_task.set_upstream(summarized_transactions_stage_task)
