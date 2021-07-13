from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from operators.extract_operator import mysql_table_to_s3, make_request

PREFIX = 'example_dags/extract_examples/'

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('cccom-dw-sales-and-clicks',
         schedule_interval='0 * * * *',
         catchup=False,
         max_active_runs=1,
         dagrun_timeout = timedelta(hours=2),
         default_args = default_args) as dag:

    extract_affiliates = PythonOperator(
        task_id=f'extract-cccom-affiliates',
        python_callable=mysql_table_to_s3,  # make sure you don't include the () of the function
        op_kwargs={'extract_script': 'cccom/extract_affiliates.sql', 'key': PREFIX + 'affiliates.csv'},
        provide_context=True)

    # load_affiliates = PythonOperator(
    #     task_id='load-cccom-affiliates',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=7))
    #
    # merge_affiliates = PythonOperator(
    #     task_id='merge-cccom-affiliates',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=5))

    # This is a long-running job, so we up its priority so it
    # starts early in the DAG, and other tasks can happen in parallel.
    extract_click_transactions = PythonOperator(
        task_id='extract-cccom-click_transactions',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_click_transactions.sql', 'key': PREFIX + 'click_transactions.csv'},
        provide_context=True,
        priority_weight=5,
        execution_timeout=timedelta(minutes=40))

    # load_click_transactions = PythonOperator(
    #     task_id='load-cccom-click_trans',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=9))
    #
    # merge_click_transactions = PythonOperator(
    #     task_id='merge-cccom-click_trans',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=5))

    extract_device_types = PythonOperator(
        task_id='extract-cccom-device_types',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_device_types.sql', 'key': PREFIX + 'device_types.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=3))

    # load_device_types = PythonOperator(
    #     task_id='load-cccom-device_types',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=4))
    #
    # merge_device_types = PythonOperator(
    #     task_id='merge-cccom-device_types',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=2))

    extract_pages = PythonOperator(
        task_id='extract-cccom-pages',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_pages.sql', 'key': PREFIX + 'pages.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=2))

    # load_pages = PythonOperator(
    #     task_id='load-cccom-pages',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=4))
    #
    # merge_pages = PythonOperator(
    #     task_id='merge-cccom-pages',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=3))

    # This is a long-running job, so we up its priority so it
    # starts early in the DAG, and other tasks can happen in parallel.
    extract_sale_transactions = PythonOperator(
        task_id='extract-cccom-sale_transactions',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_sale_trans.sql', 'key': PREFIX + 'sale_trans.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=20),
        priority_weight=5)

    # load_sale_transactions = PythonOperator(
    #     task_id='load-cccom-sale_trans',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=7))
    #
    # merge_sale_transactions = PythonOperator(
    #     task_id='merge-cccom-sale_trans',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=2))

    # """Adding new tasks for sale trans from RMS"""
    # extract_sale_rms_with_cutover_date = PostgresExtractOperator(
    #     task_id='extract-cccom-sales_rms-with-cutover-date',
    #     sql='sql/extract/cccom/extract_rms_transactions.sql',
    #     s3_bucket=Variable.get("CCCOM-BUCKET"),
    #     s3_key="/stage/cccom/rms_transactions/",
    #     s3_file_name='rms_transactions',
    #     s3_conn_id="my_s3_conn",
    #     postgres_conn_id='postgres_user_rms',
    #     delimiter="\t",
    #     file_format="tsv",
    #     header=None,
    #     execution_timeout=timedelta(minutes=90),
    #     priority_weight=5
    # )

    # load_sale_rms_with_cutover_date = PythonOperator(
    #     task_id='load-cccom-sales_rms-with-cutover-date',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=9),
    # )
    #
    # merge_sales_rms_with_cutover_date = PythonOperator(
    #     task_id='merge-cccom-sales_rms-with-cutover-date',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=2)
    # )

    """End of task defs for sale trans"""
    extract_transaction_types = PythonOperator(
        task_id='extract-cccom-transaction_types',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_transaction_types.sql', 'key': PREFIX + 'transaction_types.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=3))

    # load_transaction_types = PythonOperator(
    #     task_id='load-cccom-trans_types',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=4))
    #
    # merge_transaction_types = PythonOperator(
    #     task_id='merge-cccom-trans_types',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=2))

    extract_keywords = PythonOperator(
        task_id='extract-cccom-keywords',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_keywords.sql', 'key': PREFIX + 'keywords.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=3))

    # load_keywords = PythonOperator(
    #     task_id='load-cccom-keywords',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=3))
    #
    # merge_keywords = PythonOperator(
    #     task_id='merge-cccom-keywords',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=2))

    extract_websites = PythonOperator(
        task_id='extract-cccom-websites',
        python_callable=mysql_table_to_s3,
        op_kwargs={'extract_script': 'cccom/extract_websites.sql', 'key': PREFIX + 'websites.csv'},
        provide_context=True,
        execution_timeout=timedelta(minutes=3))

    # load_websites = PythonOperator(
    #     task_id='load-cccom-websites',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=4))
    #
    # merge_websites = PythonOperator(
    #     task_id='merge-cccom-websites',
    #     python_callable=dh.execute_pipeline,
    #     execution_timeout=timedelta(minutes=3))


# extract_sale_transactions >> load_sale_transactions >> merge_sale_transactions
#
# extract_sale_rms_with_cutover_date >> load_sale_rms_with_cutover_date  >> merge_sales_rms_with_cutover_date
#
# merge_sale_transactions >> merge_sales_rms_with_cutover_date
#
# extract_transaction_types >> load_transaction_types >> merge_transaction_types
#
# extract_affiliates >> load_affiliates >> merge_affiliates
#
# extract_keywords >> load_keywords >> merge_keywords
#
# extract_websites >> load_websites >> merge_websites
#
# extract_click_transactions >> load_click_transactions >> merge_click_transactions
#
# extract_device_types >> load_device_types >> merge_device_types
#
# extract_pages >> load_pages >> merge_pages
#
# merge_transaction_types >> merge_sale_transactions
#
# merge_affiliates >> merge_websites
#
# merge_affiliates >> merge_sale_transactions
#
# merge_keywords >> merge_click_transactions
#
# merge_keywords >> merge_sale_transactions
#
# merge_websites >> merge_click_transactions
#
# merge_device_types >> merge_click_transactions
#
# merge_pages >> merge_click_transactions
