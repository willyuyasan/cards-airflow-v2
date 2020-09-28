from operators.finserv_operator import FinServDatabricksSubmitRunOperator

from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email': ['vmalhotra@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'op_kwargs': cfg_dict,
    'provide_context': True
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-lake-dw-dbx-logs-test-dag'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s' % (ACCOUNT, DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
extra_small_task_custom_cluster = {
    'spark_version': '5.3.x-scala2.11',
    'node_type_id': 'i3.xlarge',
    'driver_node_type_id': 'i3.xlarge',
    'num_workers': 1,
    'auto_termination_minutes': 0,
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.driver.extraJavaOptions': '-Dconfig.resource=application-cards-qa.conf',
        'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
        'java_opts': '-Dconfig.resource=application-cards-qa.conf'
    },
    'cluster_log_conf': LOG_PATH,
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        'ebs_volume_count': 1,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com'
    },
}

# Libraries
staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
    },
]


"Users/vmalhotra@redventures.net/cards-data/DEC-714-TPG-App-Reporting-Form/test-for-dbx-logs"

# Notebook Task Parameter Setup:
test_staging_notebook_task = {
    'base_parameters': {},
    'notebook_path': "/Users/vmalhotra@redventures.net/cards-data/DEC-714-TPG-App-Reporting-Form/test-for-dbx-logs",
}

paidsearch_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.PaidSearch",
        "ACCOUNT=" + "cards",
        "PAID_SEARCH_COMPANY_ID=" + Variable.get("CARDS_PAIDSEARCH_COMPANY_IDS"),
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

# DAG Creation Step
with DAG('data-lake-dw-dbx-logs-test-dag',
         schedule_interval='30 0-23 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    test_staging = FinServDatabricksSubmitRunOperator(
        task_id='test-dbx-logs-staging',
        new_cluster=extra_small_task_custom_cluster,
        notebook_task=test_staging_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
