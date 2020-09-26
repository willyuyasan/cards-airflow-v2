from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators import ExternalTaskSensor
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email': ['rshukla@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-lake-dw-cdm-sdk-recon-daily'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s' % (ACCOUNT, DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
small_task_custom_cluster = {
    'spark_version': '5.3.x-scala2.11',
    'node_type_id': 'm5a.xlarge',
    'driver_node_type_id': 'm5a.xlarge',
    'num_workers': 2,
    'auto_termination_minutes': 0,
    'cluster_log_conf': LOG_PATH,
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.driver.extraJavaOptions': '-Dconfig.resource=application-cards-qa.conf',
        'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
        'java_opts': '-Dconfig.resource=application-cards-qa.conf'
    },
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        'ebs_volume_count': 1,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '0',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

# Libraries
staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/FileStore/jars/5168d529_c94b_4aa6_87bc_4d1cfe9b6abb-data_common_2_3_0-4f440.jar",
    },
]

base_params_recon = {
    "compareMode": "count",
    "adhocRun": "no",
    "checkSchemaMatches": "yes",
    "fromDate": (
        datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_recon_lookback_days")))))).strftime(
        "%Y-%m-%d"),
    "toDate": datetime.now().strftime("%Y-%m-%d"),
    "compareDiffThreashold": "0.01",
    "sdkEnv": "dev",
}

table_count_recon_notebook_task = {
    'base_parameters': base_params_recon,
    'notebook_path': 'Workspace/Users/rshukla@redventures.net/cards/cdm sdk/recon_sdk',
}

# DAG Creation Step
with DAG('data-lake-dw-cdm-sdk-recon-daily',
         schedule_interval='0 9 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    page_metrics_staging = FinServDatabricksSubmitRunOperator(
        task_id='table-count-recon',
        new_cluster=small_task_custom_cluster,
        notebook_task=table_count_recon_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
